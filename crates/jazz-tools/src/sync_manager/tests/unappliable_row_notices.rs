//! A row that keeps failing is counted, but a row that fails DIFFERENTLY is loud.
//!
//! Repeat failures carry no new information, so they are counted rather than printed —
//! one wedged row produced 130,634 identical WARN lines in three hours in production,
//! 49,606 a minute at peak. But suppression keyed on the row alone would hide the one
//! thing that IS new: the row's failure mode changing.
//!
//! That is not hypothetical, and the deployment makes it worse. `RUST_LOG` is `info` on
//! the sync server, so the DEBUG line the repeats fall back to does not exist there at
//! all — the detail is not moved, it is gone. A row wedged on `ParentNotFound` whose
//! storage then starts failing would be silent for the whole interval, which is precisely
//! how the 2026-08-15 ENOSPC incident began: one failed commit, 548 cascading failures.
//! A change written to improve incident detection would have regressed it for another.

use super::*;

#[test]
fn a_row_that_fails_differently_is_never_suppressed() {
    let mut sync = SyncManager::new();
    let row_key = (ObjectId::new(), BranchName::new("main"));

    let (first, attempts, _) = sync.note_unappliable_row(row_key, "parent-not-found");
    assert!(first, "the first failure for a row must always be spoken");
    assert_eq!(attempts, 1);

    let (repeat, attempts, _) = sync.note_unappliable_row(row_key, "parent-not-found");
    assert!(
        !repeat,
        "the same failure again inside the interval carries nothing new and must be \
         counted rather than printed"
    );
    assert_eq!(attempts, 2, "the repeat must still be counted");

    // THE ONE DIFFERENCE: same row, same interval, different failure.
    let (changed, attempts, _) = sync.note_unappliable_row(row_key, "storage-error");
    assert!(
        changed,
        "a row whose failure MODE changes must be spoken immediately. Production runs at \
         `info`, so the DEBUG repeat line does not exist there — suppressing this hides a \
         storage failure behind whatever the row was already failing on, for the whole \
         interval. That is the shape of an ENOSPC incident arriving unannounced."
    );
    assert_eq!(attempts, 3);

    let (repeat_after_change, _, _) = sync.note_unappliable_row(row_key, "storage-error");
    assert!(
        !repeat_after_change,
        "once spoken, the NEW failure is itself a repeat and goes back to being counted"
    );
}
