//! Per-settle-pass cost accounting for the query settle path.
//!
//! # Why this exists
//!
//! On the live server, one chat message send costs a ~1 s burst of one core.
//! CPU-triggered `eu-stack` sampling during real sends caught the chain
//! `batched_tick -> QueryManager::process -> settle_server_subscriptions ->
//! settle_with_context -> ArraySubqueryNode::reevaluate_all ->
//! evaluate_subgraph_for_single -> SubgraphTemplate::instantiate ->
//! try_compile_with_schema_context_shared`, but only ~10 samples were caught,
//! so the RELATIVE split of that second is unproven. An isolated backend-role
//! repro cost ~70 ms/message; the gap to 1 s was attributed to policies plus
//! subscription fan-out and never measured.
//!
//! This module makes the split readable straight out of `docker logs`: every
//! settle pass that exceeds a threshold emits ONE `tracing::info!` line with
//! the exact counts of the work it did.
//!
//! # What a "settle pass" is
//!
//! One [`QueryManager::process`](crate::query_manager::manager::QueryManager::process)
//! call — the tick body that `batched_tick` drives. It covers both subscription
//! settle loops (local subscriptions and `settle_server_subscriptions`) plus
//! the write application that feeds them, which is exactly the unit the
//! operator sees as the CPU burst.
//!
//! # Cost model
//!
//! Counters are process-global relaxed atomics, always on and exact (never
//! sampled). A pass is measured as a snapshot difference: [`SettlePass::begin`]
//! records the counters and the clock, the drop computes the delta and only
//! then decides whether to log. Below the threshold a pass costs two
//! `Instant::now()` calls, ~11 relaxed loads at each end, and one relaxed
//! `fetch_add` per counted event. No allocation, no formatting, no per-row
//! logging.
//!
//! Global (rather than per-`QueryManager`) counters are correct because a
//! settle pass is single-threaded by construction: `process` takes `&mut self`
//! and, in the server runtime, the whole `RuntimeCore` is behind one mutex, so
//! ticks of one node never overlap. Several nodes ticking in ONE process (test
//! fleets) would blend their counts into whichever pass is open; tests that
//! assert on the numbers must serialise, exactly as the allocator-based gates
//! in `tests/include_instance_flatness.rs` already do.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use web_time::Instant;

use crate::sync_manager::ClientId;

/// Settle passes above this wall duration emit a cost line. Overridable with
/// `JAZZ_SETTLE_LOG_MS`; `0` logs every pass.
const DEFAULT_THRESHOLD_MS: u64 = 5;

/// Subscription graphs settled — one per subscription that actually did settle
/// work in the pass, at both the local (`QueryManager::process`) and the server
/// (`settle_server_subscriptions`) sites. Subscriptions short-circuited as
/// clean are NOT counted: they did no work.
pub static SUBSCRIPTIONS_SETTLED: AtomicU64 = AtomicU64::new(0);

/// Dirty graph nodes evaluated by `QueryGraph::settle_with_context`, summed
/// over every graph settled in the pass — including the per-instance subgraphs
/// of array subqueries, which settle through the same function.
pub static GRAPH_NODES_EVALUATED: AtomicU64 = AtomicU64::new(0);

/// Cached array-subquery instances (re-)evaluated —
/// `ArraySubqueryNode::evaluate_subgraph_for_single` calls. This is the (a) of
/// the split: work proportional to the number of live include instances rather
/// than to the size of the change.
pub static SUBQUERY_INSTANCE_EVALS: AtomicU64 = AtomicU64::new(0);

/// Array-subquery instantiations — `SubgraphTemplate::instantiate` calls, i.e.
/// instance evaluations that could NOT reuse a cached subgraph and had to
/// compile a fresh one. This is the (b) of the split.
pub static SUBQUERY_INSTANTIATIONS: AtomicU64 = AtomicU64::new(0);

/// Query plans compiled —
/// `QueryGraph::compile_execution_plan_with_schema_context_shared` calls. A
/// superset of [`SUBQUERY_INSTANTIATIONS`]: every instantiation compiles one
/// plan, and nested includes inside it compile more.
pub static PLAN_COMPILES: AtomicU64 = AtomicU64::new(0);

/// Per-row policy evaluations — `PolicyEvaluator::evaluate_row_access` calls,
/// counting recursive descent through referencing/inherited policies, since
/// that descent is the cost. This is the (c) of the split.
pub static POLICY_ROW_EVALS: AtomicU64 = AtomicU64::new(0);

/// Per-row select-policy checks run to authorize a session-scoped
/// subscription's sync scope —
/// `QueryManager::provenance_row_matches_current_select_policy` calls,
/// including the ones served from the cross-tick verdict cache.
pub static SCOPE_AUTHZ_CHECKS: AtomicU64 = AtomicU64::new(0);

/// The subset of [`SCOPE_AUTHZ_CHECKS`] that missed the verdict cache and
/// actually evaluated a policy against storage. The ratio of the two is what
/// says whether authorization is being paid per tick or amortised.
pub static SCOPE_AUTHZ_EVALS: AtomicU64 = AtomicU64::new(0);

/// Rows loaded through the query row loader
/// (`QueryManager::load_visible_row_for_query`) — the dominant storage read
/// driver of a settle. Part of the (d) of the split.
pub static ROW_LOADS: AtomicU64 = AtomicU64::new(0);

/// Index reads issued by `IndexScanNode`: one per full index scan and one per
/// incremental point membership probe. The rest of the (d) of the split.
pub static INDEX_READS: AtomicU64 = AtomicU64::new(0);

/// Rows a settled subscription emitted downstream — added + removed + updated
/// over the `RowDelta` of every subscription settled in the pass. The
/// denominator for everything above: the useful output the pass produced.
pub static ROWS_EMITTED: AtomicU64 = AtomicU64::new(0);

/// Count one event.
#[inline]
pub(crate) fn bump(counter: &AtomicU64) {
    counter.fetch_add(1, Ordering::Relaxed);
}

/// Count `amount` events at once, for sites that already know the batch size.
#[inline]
pub(crate) fn add(counter: &AtomicU64, amount: u64) {
    if amount != 0 {
        counter.fetch_add(amount, Ordering::Relaxed);
    }
}

/// Identity of the most expensive single subscription settle in the open pass.
/// Reset by [`SettlePass::begin`]; only written when a subscription beats the
/// current maximum, so the common case is one relaxed load.
static HOT_MICROS: AtomicU64 = AtomicU64::new(0);
static HOT_CLIENT_HIGH: AtomicU64 = AtomicU64::new(0);
static HOT_CLIENT_LOW: AtomicU64 = AtomicU64::new(0);
static HOT_QUERY: AtomicU64 = AtomicU64::new(0);

/// Record how long one subscription's settle took, keeping the pass's maximum.
///
/// Called only from sites that have already decided to do real settle work, so
/// the two `Instant::now()` calls it costs sit next to a graph settle, never
/// next to a short-circuit.
pub fn note_subscription_settle(client_id: Option<ClientId>, query_id: u64, elapsed: Duration) {
    let micros = micros_of(elapsed);
    if micros <= HOT_MICROS.load(Ordering::Relaxed) {
        return;
    }
    let (high, low) = match client_id {
        Some(ClientId(uuid)) => {
            let bits = uuid.as_u128();
            ((bits >> 64) as u64, bits as u64)
        }
        None => (0, 0),
    };
    HOT_MICROS.store(micros, Ordering::Relaxed);
    HOT_CLIENT_HIGH.store(high, Ordering::Relaxed);
    HOT_CLIENT_LOW.store(low, Ordering::Relaxed);
    HOT_QUERY.store(query_id, Ordering::Relaxed);
}

fn micros_of(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX)
}

/// One reading of every settle counter.
///
/// Exposed so tests can assert the accounting itself: take a snapshot, run a
/// scenario of known shape, take another, and compare the difference against
/// what the scenario implies.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SettleCounts {
    pub subscriptions: u64,
    pub graph_nodes: u64,
    pub instance_evals: u64,
    pub subquery_instantiations: u64,
    pub plan_compiles: u64,
    pub policy_row_evals: u64,
    pub scope_authz_checks: u64,
    pub scope_authz_evals: u64,
    pub row_loads: u64,
    pub index_reads: u64,
    pub rows_emitted: u64,
}

impl SettleCounts {
    /// Read every counter. Counters are independent atomics, so a snapshot
    /// taken while a pass is running on another thread is not a consistent
    /// cut — see the module note on single-threaded passes.
    pub fn snapshot() -> Self {
        Self {
            subscriptions: SUBSCRIPTIONS_SETTLED.load(Ordering::Relaxed),
            graph_nodes: GRAPH_NODES_EVALUATED.load(Ordering::Relaxed),
            instance_evals: SUBQUERY_INSTANCE_EVALS.load(Ordering::Relaxed),
            subquery_instantiations: SUBQUERY_INSTANTIATIONS.load(Ordering::Relaxed),
            plan_compiles: PLAN_COMPILES.load(Ordering::Relaxed),
            policy_row_evals: POLICY_ROW_EVALS.load(Ordering::Relaxed),
            scope_authz_checks: SCOPE_AUTHZ_CHECKS.load(Ordering::Relaxed),
            scope_authz_evals: SCOPE_AUTHZ_EVALS.load(Ordering::Relaxed),
            row_loads: ROW_LOADS.load(Ordering::Relaxed),
            index_reads: INDEX_READS.load(Ordering::Relaxed),
            rows_emitted: ROWS_EMITTED.load(Ordering::Relaxed),
        }
    }

    /// Work done since `base`, field by field.
    pub fn since(self, base: Self) -> Self {
        Self {
            subscriptions: self.subscriptions.saturating_sub(base.subscriptions),
            graph_nodes: self.graph_nodes.saturating_sub(base.graph_nodes),
            instance_evals: self.instance_evals.saturating_sub(base.instance_evals),
            subquery_instantiations: self
                .subquery_instantiations
                .saturating_sub(base.subquery_instantiations),
            plan_compiles: self.plan_compiles.saturating_sub(base.plan_compiles),
            policy_row_evals: self.policy_row_evals.saturating_sub(base.policy_row_evals),
            scope_authz_checks: self
                .scope_authz_checks
                .saturating_sub(base.scope_authz_checks),
            scope_authz_evals: self
                .scope_authz_evals
                .saturating_sub(base.scope_authz_evals),
            row_loads: self.row_loads.saturating_sub(base.row_loads),
            index_reads: self.index_reads.saturating_sub(base.index_reads),
            rows_emitted: self.rows_emitted.saturating_sub(base.rows_emitted),
        }
    }
}

/// Wall-duration threshold above which a pass logs its cost record.
fn threshold_micros() -> u64 {
    #[cfg(any(test, feature = "test"))]
    if let Some(forced) = test_override::forced_ms() {
        return forced.saturating_mul(1_000);
    }

    static THRESHOLD: OnceLock<u64> = OnceLock::new();
    *THRESHOLD.get_or_init(|| {
        std::env::var("JAZZ_SETTLE_LOG_MS")
            .ok()
            .and_then(|raw| raw.trim().parse::<u64>().ok())
            .unwrap_or(DEFAULT_THRESHOLD_MS)
            .saturating_mul(1_000)
    })
}

/// Scoped accumulator for one settle pass. Emits at most one line on drop.
pub struct SettlePass {
    started: Instant,
    base: SettleCounts,
}

impl SettlePass {
    /// Open a pass: reset the hot-subscription slot and snapshot the counters.
    pub fn begin() -> Self {
        HOT_MICROS.store(0, Ordering::Relaxed);
        Self {
            started: Instant::now(),
            base: SettleCounts::snapshot(),
        }
    }

    /// Work recorded so far in this pass.
    pub fn cost(&self) -> SettleCounts {
        SettleCounts::snapshot().since(self.base)
    }
}

impl Drop for SettlePass {
    fn drop(&mut self) {
        let micros = micros_of(self.started.elapsed());
        if micros < threshold_micros() {
            return;
        }

        let cost = self.cost();
        let hot_micros = HOT_MICROS.load(Ordering::Relaxed);
        let hot_client = u128::from(HOT_CLIENT_HIGH.load(Ordering::Relaxed)) << 64
            | u128::from(HOT_CLIENT_LOW.load(Ordering::Relaxed));
        let hot_client = if hot_client == 0 {
            "local".to_string()
        } else {
            uuid::Uuid::from_u128(hot_client).to_string()
        };

        tracing::info!(
            target: "jazz::settle_cost",
            micros,
            subscriptions = cost.subscriptions,
            graph_nodes = cost.graph_nodes,
            instance_evals = cost.instance_evals,
            subquery_instantiations = cost.subquery_instantiations,
            plan_compiles = cost.plan_compiles,
            policy_row_evals = cost.policy_row_evals,
            scope_authz_checks = cost.scope_authz_checks,
            scope_authz_evals = cost.scope_authz_evals,
            row_loads = cost.row_loads,
            index_reads = cost.index_reads,
            rows_emitted = cost.rows_emitted,
            hot_micros,
            hot_client,
            hot_query = HOT_QUERY.load(Ordering::Relaxed),
            "jazz settle pass cost"
        );
    }
}

#[cfg(any(test, feature = "test"))]
mod test_override {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Mutex, MutexGuard, OnceLock, PoisonError};

    const UNSET: u64 = u64::MAX;

    static FORCED_MS: AtomicU64 = AtomicU64::new(UNSET);

    fn lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    /// Holds the settle-cost log threshold for the guard's lifetime.
    ///
    /// The embedded mutex guard serialises every test that forces a threshold,
    /// the same plug shape as `precise_dirty::force_precise_dirty`, so
    /// force-users cannot observe each other's override and no test has to
    /// mutate the process environment.
    pub struct SettleLogThreshold {
        _serialised: MutexGuard<'static, ()>,
    }

    impl Drop for SettleLogThreshold {
        fn drop(&mut self) {
            FORCED_MS.store(UNSET, Ordering::SeqCst);
        }
    }

    /// Force the settle-cost log threshold to `milliseconds` (0 logs every
    /// pass) for the returned guard's lifetime.
    pub fn force_settle_log_ms(milliseconds: u64) -> SettleLogThreshold {
        let guard = lock().lock().unwrap_or_else(PoisonError::into_inner);
        FORCED_MS.store(milliseconds, Ordering::SeqCst);
        SettleLogThreshold { _serialised: guard }
    }

    pub(super) fn forced_ms() -> Option<u64> {
        match FORCED_MS.load(Ordering::SeqCst) {
            UNSET => None,
            milliseconds => Some(milliseconds),
        }
    }
}

#[cfg(any(test, feature = "test"))]
pub use test_override::{SettleLogThreshold, force_settle_log_ms};
