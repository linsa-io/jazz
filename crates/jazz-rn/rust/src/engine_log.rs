//! Engine logging for the React Native binding.
//!
//! # Why this exists
//!
//! `jazz-tools` instruments the engine with `tracing` — including the
//! per-settle cost lines on the `jazz::settle_cost` target — but a binding that
//! installs no subscriber throws every one of them away. On the server those
//! lines land in `docker logs`; on the phone they landed nowhere, so the client
//! half of every performance claim had to be inferred from process CPU instead
//! of counted.
//!
//! # Shape
//!
//! * **Sink: Apple unified logging** (`os_log`), subsystem `io.linsa.jazz`, one
//!   category per tracing target — see [`sink`]. Non-Apple targets fall back to
//!   stderr so the crate still builds for Android and wasm.
//! * **Not the RN bridge.** These are hot-path lines; a bridge crossing per line
//!   would change what is being measured. Native logs do not reach the
//!   Metro/Expo terminal in any case — that carries JS logs only.
//! * **Off by default.** [`install`] puts the subscriber in place with an `off`
//!   filter, so `tracing`'s interest cache disables every callsite and a shipped
//!   build pays a callsite check that is already compiled out.
//! * **Reloadable.** [`set_level`] swaps the filter in a running app and
//!   rebuilds the interest cache, so a measurement can be switched on for a
//!   minute and switched back without a restart.
//! * **Installed exactly once.** RN reloads re-enter native init; [`install`]
//!   is a `OnceLock` so a second call is a no-op rather than a panic or a
//!   duplicated line.
//!
//! # Reading the logs
//!
//! ```text
//! # simulator (WARN/INFO and above)
//! xcrun simctl spawn booted log stream \
//!   --predicate 'subsystem == "io.linsa.jazz"' --style compact
//!
//! # simulator, including tracing DEBUG/TRACE lines
//! xcrun simctl spawn booted log stream --level debug \
//!   --predicate 'subsystem == "io.linsa.jazz"' --style compact
//!
//! # one category only
//! xcrun simctl spawn booted log stream \
//!   --predicate 'subsystem == "io.linsa.jazz" AND category == "settle_cost"' \
//!   --style compact
//!
//! # device
//! xcrun devicectl device console --device <udid>
//! # or Console.app, filtered on the subsystem
//! ```

use std::fmt::{self, Write as _};
use std::sync::OnceLock;

use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt as _};
use tracing_subscriber::registry::Registry;
use tracing_subscriber::{reload, EnvFilter};

mod sink;

/// Filter applied until something explicitly asks for logs.
const OFF: &str = "off";

type FilterHandle = reload::Handle<EnvFilter, Registry>;

struct Installed {
    handle: FilterHandle,
    /// `false` when something else had already claimed the process-global
    /// subscriber, which leaves our handle inert — reported rather than
    /// swallowed, because the alternative is silently logging nothing.
    is_global: bool,
}

static INSTALLED: OnceLock<Installed> = OnceLock::new();

/// Install the engine-log subscriber, filtering everything out.
///
/// Idempotent: the first caller wins and every later call is a no-op, which is
/// what an RN reload re-entering native init needs.
pub(crate) fn install() {
    let _ = installed();
}

fn installed() -> &'static Installed {
    INSTALLED.get_or_init(|| {
        let (filter, handle) = reload::Layer::new(EnvFilter::new(OFF));
        let subscriber = Registry::default().with(filter).with(OsLogLayer);
        let is_global = tracing::subscriber::set_global_default(subscriber).is_ok();
        Installed { handle, is_global }
    })
}

/// Apply a filter spec, taking effect immediately in a running app.
///
/// `spec` is an `EnvFilter` directive string, which covers both the plain level
/// words (`off`, `error`, `warn`, `info`, `debug`, `trace`) and per-target
/// filtering (`off,jazz::settle_cost=info`). An empty spec means `off`.
pub(crate) fn set_level(spec: &str) -> Result<(), String> {
    let spec = spec.trim();
    let spec = if spec.is_empty() { OFF } else { spec };

    let filter =
        EnvFilter::try_new(spec).map_err(|e| format!("invalid log filter {spec:?}: {e}"))?;

    let installed = installed();
    if !installed.is_global {
        return Err("another tracing subscriber owns this process".to_string());
    }
    installed
        .handle
        .reload(filter)
        .map_err(|e| format!("failed to apply log filter: {e}"))
}

/// Formats each event into one line and hands it to the platform sink.
struct OsLogLayer;

impl<S: Subscriber> Layer<S> for OsLogLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let metadata = event.metadata();
        let mut line = LineWriter::new(metadata.level());
        event.record(&mut line);
        sink::emit(metadata.target(), *metadata.level(), &line.buf);
    }
}

/// `[INFO] message key=value key=value`.
///
/// os_log already stamps time, process and category, so the line carries only
/// what tracing knows: the level and the fields.
struct LineWriter {
    buf: String,
}

impl LineWriter {
    fn new(level: &Level) -> Self {
        Self {
            buf: format!("[{level}]"),
        }
    }

    fn push(&mut self, name: &str, value: fmt::Arguments<'_>) {
        self.buf.push(' ');
        // The `message` field is the event's free text and has no useful name.
        let _ = if name == "message" {
            write!(self.buf, "{value}")
        } else {
            write!(self.buf, "{name}={value}")
        };
    }
}

impl Visit for LineWriter {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.push(field.name(), format_args!("{value}"));
    }

    // Integers, floats and bools reach this through `Visit`'s default impls.
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.push(field.name(), format_args!("{value:?}"));
    }
}

#[cfg(test)]
mod tests {
    use super::{install, set_level, sink};

    /// One test, not several: the subscriber and its filter are process-global,
    /// so parallel tests would race on the level.
    #[test]
    fn filter_gates_the_sink_and_reloads_without_restart() {
        install();
        let _ = sink::capture::take();

        // Default is off: the callsite is disabled, nothing reaches the sink.
        tracing::info!(target: "jazz::settle_cost", micros = 1);
        assert!(
            sink::capture::take().is_empty(),
            "logs before any level was set"
        );

        // Raising one target leaves the others off.
        set_level("jazz::settle_cost=info").expect("valid directive");
        tracing::info!(target: "jazz::settle_cost", micros = 2, hot_client = "local");
        tracing::info!(target: "jazz::sync", micros = 3);
        let lines = sink::capture::take();
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert_eq!(lines[0].0, "jazz::settle_cost");
        assert_eq!(lines[0].1, "[INFO] micros=2 hot_client=local");

        // Free-text messages keep their position, named fields follow.
        set_level("info").expect("valid directive");
        tracing::info!(target: "jazz::sync", answer = 42, "hello");
        let lines = sink::capture::take();
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert_eq!(lines[0].1, "[INFO] hello answer=42");

        // And back off again, in the same process.
        set_level("off").expect("valid directive");
        tracing::info!(target: "jazz::settle_cost", micros = 4);
        assert!(
            sink::capture::take().is_empty(),
            "logs after being switched off"
        );

        // Re-entering init (an RN reload) must not panic or double-install.
        install();
        set_level("off").expect("still reloadable after a second install");

        // A bad directive is reported, not silently ignored.
        set_level("jazz::settle_cost=louder").expect_err("invalid level");
    }
}
