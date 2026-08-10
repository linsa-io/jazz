//! Resolving a batch's visible ancestors must cost the ancestors, not the history.
//!
//! `history_rows_visible_before_batch` walks the parent chain of an incoming
//! batch through the rows already visible for that row. It only ever reads each
//! candidate's `parents`, but it used to index them by CLONING every visible
//! row — payload and parent vector included — so an incoming write cost one
//! full copy of the row's history.
//!
//! That is invisible while histories are short. Measured in production
//! (2026-08-10) on a presence row grown to 2541 history entries, with a client
//! re-sending unappliable batches at ~52/s: about 132k row clones a second,
//! and a pinned core doing nothing else. The stack dump named this function
//! under `smallvec::grow`.
//!
//! The gate counts cloned rows: it must track the ANCESTOR set, not the size of
//! the history it was found in.

use super::*;

/// A history long enough that "clones the history" and "clones the ancestors"
/// cannot be confused, and small enough to stay a unit test.
const HISTORY_ROWS: usize = 200;

#[test]
fn resolving_ancestors_does_not_clone_the_whole_history() {
    let row_id = ObjectId::new();

    // A chain: each row's parent is the batch before it. Only the last two are
    // ancestors of the incoming batch.
    // Deliberately over-allocated: a fresh collect would size itself to the
    // selection, so capacity is a second, independent witness that the
    // caller's own buffer came back rather than a copy of it.
    let mut visible_rows = Vec::with_capacity(HISTORY_ROWS * 4);
    let mut previous: Vec<BatchId> = Vec::new();
    for index in 0..HISTORY_ROWS {
        let row = visible_row(
            row_id,
            "main",
            previous.clone(),
            index as u64,
            format!("history-{index}").as_bytes(),
        );
        previous = vec![row.batch_id];
        visible_rows.push(row);
    }

    // The incoming batch descends from the newest visible row.
    let incoming = visible_row(
        row_id,
        "main",
        previous.clone(),
        HISTORY_ROWS as u64,
        b"incoming",
    );

    let input_ptr = visible_rows.as_ptr();
    let input_capacity = visible_rows.capacity();
    let ancestors = SyncManager::history_rows_visible_before_batch(&incoming, visible_rows, false)
        .expect("the parent chain resolves");

    assert!(
        !ancestors.is_empty(),
        "the walk must actually find the batch's ancestors, else it gates nothing"
    );
    assert_eq!(
        ancestors.len(),
        HISTORY_ROWS,
        "a linear history makes every entry an ancestor — that is the production shape, \
         and the reason indexing alone was not enough"
    );
    assert_eq!(
        ancestors.as_ptr(),
        input_ptr,
        "the ancestors came back in a different allocation — the rows were copied instead of \
         the caller's own vector being narrowed in place"
    );
    assert_eq!(
        ancestors.capacity(),
        input_capacity,
        "the returned vector was re-sized, so it is a copy: a {HISTORY_ROWS}-row history \
         costs a {HISTORY_ROWS}-row copy on every incoming batch"
    );
}
