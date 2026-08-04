//! Platform sink for engine logs.
//!
//! Apple targets write to unified logging (`os_log`) through the C shim in
//! `src/os_log_shim.c`. Every other target writes to stderr, which keeps
//! Android and wasm builds compiling; only iOS is a supported reading surface
//! today.

use tracing::Level;

/// Subsystem every engine log line is filed under. The documented read
/// commands filter on exactly this string.
const SUBSYSTEM: &str = "io.linsa.jazz";

/// os_log category for a tracing target.
///
/// `jazz::settle_cost` becomes `settle_cost`, `jazz_tools::sync_manager::foo`
/// becomes `sync_manager.foo`: the crate prefix is noise once the subsystem
/// already says which engine emitted the line. Targets are `&'static str`
/// callsite metadata, so the set is bounded by the binary.
fn category_for(target: &str) -> String {
    let trimmed = target
        .strip_prefix("jazz::")
        .or_else(|| target.strip_prefix("jazz_tools::"))
        .unwrap_or(target);
    trimmed.replace("::", ".")
}

/// Write one already-formatted line to the platform sink.
///
/// `message` carries its own `[LEVEL]` prefix; `level` is passed separately
/// only so the Apple sink can pick an `os_log_type_t`.
pub(super) fn emit(target: &'static str, level: Level, message: &str) {
    #[cfg(test)]
    capture::record(target, message);

    #[cfg(target_vendor = "apple")]
    apple::emit(target, level, message);

    #[cfg(not(target_vendor = "apple"))]
    {
        // Subsystem and category are printed rather than structured, so a
        // non-Apple line still says where it came from.
        let _ = level;
        eprintln!("{SUBSYSTEM}[{}] {message}", category_for(target));
    }
}

#[cfg(target_vendor = "apple")]
mod apple {
    use std::collections::HashMap;
    use std::ffi::{c_char, c_void, CString};
    use std::sync::{LazyLock, RwLock};

    use tracing::Level;

    // `os_log_type_t` from <os/log.h>.
    //
    // Unified logging has no "warn", and INFO/DEBUG messages are hidden from
    // `log stream` unless it is started with `--level info` / `--level debug`.
    // Since engine logs are off unless someone deliberately turned them on,
    // tracing WARN and INFO map to DEFAULT so the documented read command shows
    // them with no extra flags. The tracing level is written into the message
    // text, so the distinction is not lost.
    const OS_LOG_TYPE_DEFAULT: u8 = 0x00;
    const OS_LOG_TYPE_DEBUG: u8 = 0x02;
    const OS_LOG_TYPE_ERROR: u8 = 0x10;

    extern "C" {
        fn jazz_rn_os_log_create(subsystem: *const c_char, category: *const c_char) -> *mut c_void;
        fn jazz_rn_os_log_emit(log: *mut c_void, ty: u8, message: *const c_char);
    }

    /// An `os_log_t`. Apple documents log objects as safe to use from any
    /// thread, and they are never released: one is created per tracing target,
    /// a set bounded by the callsites compiled into the binary.
    #[derive(Clone, Copy)]
    struct Logger(*mut c_void);

    // SAFETY: `os_log_t` is a thread-safe, immutable handle per Apple's
    // os_log(3); the pointer is only ever passed back to os_log.
    unsafe impl Send for Logger {}
    unsafe impl Sync for Logger {}

    static LOGGERS: LazyLock<RwLock<HashMap<&'static str, Logger>>> =
        LazyLock::new(Default::default);

    fn os_log_type(level: Level) -> u8 {
        match level {
            Level::ERROR => OS_LOG_TYPE_ERROR,
            Level::WARN | Level::INFO => OS_LOG_TYPE_DEFAULT,
            Level::DEBUG | Level::TRACE => OS_LOG_TYPE_DEBUG,
        }
    }

    fn logger_for(target: &'static str) -> Logger {
        if let Some(logger) = LOGGERS
            .read()
            .ok()
            .and_then(|loggers| loggers.get(target).copied())
        {
            return logger;
        }

        // A racing thread may create a second handle for the same target;
        // os_log_create returns the shared object for a given
        // subsystem/category pair, so that is harmless.
        let subsystem = CString::new(super::SUBSYSTEM).expect("subsystem has no NUL");
        let category =
            CString::new(super::category_for(target)).unwrap_or_else(|_| c"jazz".to_owned());
        // SAFETY: both pointers are valid, NUL-terminated, and only read for
        // the duration of the call.
        let logger =
            Logger(unsafe { jazz_rn_os_log_create(subsystem.as_ptr(), category.as_ptr()) });

        if let Ok(mut loggers) = LOGGERS.write() {
            loggers.insert(target, logger);
        }
        logger
    }

    pub(super) fn emit(target: &'static str, level: Level, message: &str) {
        let text = match CString::new(message) {
            Ok(text) => text,
            // An embedded NUL would truncate the C string. Keep the line,
            // escape the byte.
            Err(_) => CString::new(message.replace('\0', "\\0")).expect("no NUL left"),
        };
        let logger = logger_for(target);
        // SAFETY: `logger` came from os_log_create and `text` is a valid
        // NUL-terminated string that outlives the call.
        unsafe { jazz_rn_os_log_emit(logger.0, os_log_type(level), text.as_ptr()) };
    }
}

/// Test-only tee. Lines still go to the real platform sink, so a `cargo test`
/// run on macOS can be watched with the same `log stream` command the manual
/// procedure documents.
#[cfg(test)]
pub(super) mod capture {
    use std::sync::Mutex;

    static LINES: Mutex<Vec<(&'static str, String)>> = Mutex::new(Vec::new());

    pub(crate) fn record(target: &'static str, message: &str) {
        if let Ok(mut lines) = LINES.lock() {
            lines.push((target, message.to_owned()));
        }
    }

    /// Drain everything recorded so far.
    pub(crate) fn take() -> Vec<(&'static str, String)> {
        LINES
            .lock()
            .map(|mut lines| lines.drain(..).collect())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::category_for;

    #[test]
    fn categories_drop_the_crate_prefix() {
        assert_eq!(category_for("jazz::settle_cost"), "settle_cost");
        assert_eq!(
            category_for("jazz_tools::sync_manager::gc"),
            "sync_manager.gc"
        );
        assert_eq!(category_for("jazz_rn"), "jazz_rn");
    }
}
