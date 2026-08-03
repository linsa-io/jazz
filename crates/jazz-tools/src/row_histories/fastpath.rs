//! Serial-write fast path for [`apply_row_batch`](super::apply_row_batch).
//!
//! A new visible batch whose parent set equals the entire previous branch
//! frontier dominates every old tip: after insertion the frontier is exactly
//! `{row}` and the unfiltered visible preview is the row itself. For such
//! writes on never-forked rows the next [`VisibleRowEntry`] is derivable from
//! the previous entry plus the incoming row alone — no
//! `load_branch_history` / full `visible_entry_from_history_rows` rebuild,
//! turning serial appends from O(history) to O(1).
//!
//! The bar is byte-exactness: the constructed entry must equal the full
//! rebuild bit for bit (enforced after every op by the randomized differential
//! oracle in `storage::conformance_differential`). Every eligibility guard
//! below exists to keep that provable from O(1) state; any miss falls back to
//! the full path, which is always correct.
//!
//! Why the per-tier carry-forward is subtle: the rebuild computes each tier
//! preview over the *tier-filtered* row set, and filtering can disconnect a
//! linear chain — a tier-satisfying ancestor whose descendants are not
//! tier-confirmed ("tier hole") re-surfaces as a concurrent tier tip whose
//! merge output depends on deep history. [`carried_tier_pointer`] therefore
//! only handles the transitions whose outcome is provable from the previous
//! entry, and declines the rest (see its doc comment for the case analysis).

use std::sync::OnceLock;
use std::sync::atomic::AtomicU64;

use smallvec::SmallVec;

use crate::sync_manager::DurabilityTier;

use super::codecs::tier_satisfies;
use super::types::{BatchId, StoredRowBatch, VisibleRowEntry};

/// Applies that constructed the visible entry without a history rebuild.
pub static HISTORY_FASTPATH_HITS: AtomicU64 = AtomicU64::new(0);
/// Applies in the targeted population (previous entry present, incoming row
/// visible) that still took the full-history path.
pub static HISTORY_FASTPATH_FALLBACKS: AtomicU64 = AtomicU64::new(0);

/// Kill switch: `JAZZ_HISTORY_FASTPATH=0` (or `false`) forces the full path.
/// Default is on. The env var is read once; tests use
/// [`force_history_fastpath`] instead to avoid env races.
pub fn history_fastpath_enabled() -> bool {
    #[cfg(any(test, feature = "test"))]
    if let Some(forced) = test_override::forced() {
        return forced;
    }

    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        !matches!(
            std::env::var("JAZZ_HISTORY_FASTPATH").as_deref(),
            Ok("0") | Ok("false")
        )
    })
}

#[cfg(any(test, feature = "test"))]
mod test_override {
    use std::sync::atomic::{AtomicU8, Ordering};
    use std::sync::{Mutex, MutexGuard, OnceLock, PoisonError};

    const UNSET: u8 = 0;
    const FORCED_ON: u8 = 1;
    const FORCED_OFF: u8 = 2;

    static STATE: AtomicU8 = AtomicU8::new(UNSET);

    fn lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    /// Holds the fast path in a forced mode for the guard's lifetime.
    ///
    /// The embedded mutex guard serialises every test that forces a mode (or
    /// asserts on the global hit counter), so parallel tests cannot observe
    /// each other's override. Dropping restores the unforced default.
    pub struct HistoryFastpathMode {
        _serialised: MutexGuard<'static, ()>,
    }

    impl Drop for HistoryFastpathMode {
        fn drop(&mut self) {
            STATE.store(UNSET, Ordering::SeqCst);
        }
    }

    /// Force the fast path on or off for the returned guard's lifetime.
    pub fn force_history_fastpath(enabled: bool) -> HistoryFastpathMode {
        let guard = lock().lock().unwrap_or_else(PoisonError::into_inner);
        STATE.store(
            if enabled { FORCED_ON } else { FORCED_OFF },
            Ordering::SeqCst,
        );
        HistoryFastpathMode { _serialised: guard }
    }

    pub(super) fn forced() -> Option<bool> {
        match STATE.load(Ordering::SeqCst) {
            FORCED_ON => Some(true),
            FORCED_OFF => Some(false),
            _ => None,
        }
    }
}

#[cfg(any(test, feature = "test"))]
pub use test_override::{HistoryFastpathMode, force_history_fastpath};

/// Attempt the O(1) serial-write construction of the next [`VisibleRowEntry`].
///
/// Returns `None` when any eligibility guard misses; the caller then takes the
/// full-history rebuild. Caller obligations (checked upstream in
/// `apply_row_batch_with_context`): the row is not a known-new object, its
/// batch id is not already present in history, and parent existence was
/// validated.
pub(super) fn try_serial_fastpath_entry(
    previous_entry: Option<&VisibleRowEntry>,
    row: &StoredRowBatch,
) -> Option<VisibleRowEntry> {
    if !history_fastpath_enabled() {
        return None;
    }
    // Only VisibleDirect | VisibleTransactional. A StagingPending batch
    // contributes nothing to the frontier and must never become `current_row`
    // through a shortcut — that would publish a staged batch. (Staging rows
    // also keep `supersede_older_staging_rows_for_batch` on the full path by
    // construction.) Deletes interact with the delete-winner overlay of the
    // preview and stay on the full path.
    if !row.state.is_visible() || row.delete_kind.is_some() {
        return None;
    }
    let previous = previous_entry?;
    if previous.current_row.branch != row.branch {
        return None;
    }
    // Domination: the parent SET must equal the previous frontier SET
    // (order-insensitive, duplicates rejected). Not `len() == 1` — set
    // equality also admits an explicit merge-commit naming every current tip,
    // after which the frontier is trivially `{row}`.
    if !parents_cover_frontier_exactly(&row.parents, &previous.branch_frontier) {
        return None;
    }
    // Never-forked precondition: a historically forked-then-linearised row can
    // still carry a live merged tier preview (populated pool/ordinals) whose
    // O(1) maintenance across arbitrary interleavings is not provable.
    if !previous.winner_batch_pool.is_empty()
        || previous.current_winner_ordinals.is_some()
        || previous.worker_winner_ordinals.is_some()
        || previous.edge_winner_ordinals.is_some()
        || previous.global_winner_ordinals.is_some()
        || previous.merge_artifacts.is_some()
    {
        return None;
    }

    let old_tip = &previous.current_row;
    let worker_batch_id = carried_tier_pointer(
        row,
        old_tip,
        DurabilityTier::Local,
        previous.worker_batch_id,
    )?;
    let edge_batch_id = carried_tier_pointer(
        row,
        old_tip,
        DurabilityTier::EdgeServer,
        previous.edge_batch_id,
    )?;
    let global_batch_id = carried_tier_pointer(
        row,
        old_tip,
        DurabilityTier::GlobalServer,
        previous.global_batch_id,
    )?;

    debug_assert!(
        previous.merge_artifacts.is_none(),
        "never-forked guard admits only entries without merge artifacts"
    );
    Some(VisibleRowEntry {
        current_row: row.clone(),
        branch_frontier: vec![row.batch_id()],
        worker_batch_id,
        edge_batch_id,
        global_batch_id,
        winner_batch_pool: Vec::new(),
        current_winner_ordinals: None,
        worker_winner_ordinals: None,
        edge_winner_ordinals: None,
        global_winner_ordinals: None,
        merge_artifacts: None,
    })
}

fn parents_cover_frontier_exactly(parents: &[BatchId], frontier: &[BatchId]) -> bool {
    // Empty parents over an existing entry would be a concurrent root insert
    // (and `frontier` of a live entry is never empty) — never a domination.
    if parents.is_empty() || parents.len() != frontier.len() {
        return false;
    }
    let mut sorted_parents: SmallVec<[BatchId; 2]> = SmallVec::from_slice(parents);
    sorted_parents.sort_unstable();
    if sorted_parents.windows(2).any(|pair| pair[0] == pair[1]) {
        return false;
    }
    let mut sorted_frontier: SmallVec<[BatchId; 2]> = SmallVec::from_slice(frontier);
    sorted_frontier.sort_unstable();
    sorted_parents == sorted_frontier
}

/// Carry one tier sidecar pointer across a domination event, or decline.
///
/// `Some(pointer)` is proven byte-equal to what the full rebuild
/// (`VisibleRowEntry::rebuild_with_descriptor` →
/// `preview_override_sidecar`) stores; `None` means "not provable from the
/// entry alone — take the full path". Let S be the tier-filtered visible row
/// set before this write, B the old current row, C the incoming row. The
/// never-forked guard already established that all sidecar ordinals are
/// `None`, which makes every stored tier preview equal to an actual history
/// row. Case analysis:
///
/// - `C` does not satisfy the tier: S is unchanged by this insert, so the
///   rebuilt candidate preview is the same one the previous rebuild compared,
///   and it can never match the new current row `C` (its metadata row predates
///   `C`). Therefore:
///   - old pointer `None` with `B` satisfying the tier ⇒ the old preview
///     matched `B` exactly ⇒ the new pointer is `Some(B)`;
///   - old pointer `None` with `B` not satisfying ⇒ S is empty ⇒ `None`;
///   - old pointer `Some(x)` ⇒ carried verbatim (the preview still is row
///     `x`). Note this deliberately refines the design sketch's "old tip
///     satisfies ⇒ point at old tip": when a pointer is already present it is
///     the rebuild's answer, the old tip is not.
/// - `C` satisfies the tier: S gains `C`. If S was provably empty before
///   (old pointer `None` and `B` not satisfying), the new tier preview is
///   `{C}` which matches the new current row ⇒ `None`. Otherwise older
///   tier-satisfying rows may hide behind tier holes and re-surface as
///   concurrent tier tips (the tier-filtered frontier is
///   `{C} ∪ (tips(S) \ parents)`), whose merge needs full history — decline.
///   See `history_fastpath_declines_tier_satisfying_row_over_tier_hole` for a
///   concrete divergence this rules out.
fn carried_tier_pointer(
    row: &StoredRowBatch,
    old_tip: &StoredRowBatch,
    tier: DurabilityTier,
    previous_pointer: Option<BatchId>,
) -> Option<Option<BatchId>> {
    let old_tip_satisfies = tier_satisfies(old_tip.confirmed_tier, tier);
    if tier_satisfies(row.confirmed_tier, tier) {
        if !old_tip_satisfies && previous_pointer.is_none() {
            Some(None)
        } else {
            None
        }
    } else if old_tip_satisfies && previous_pointer.is_none() {
        Some(Some(old_tip.batch_id()))
    } else {
        Some(previous_pointer)
    }
}
