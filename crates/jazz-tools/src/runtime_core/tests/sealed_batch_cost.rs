//! A transactional write must not cost a walk of the whole store.
//!
//! `sealed_batch_submission` captures a "family visible frontier" for every
//! `Transactional` batch, and `capture_family_visible_frontier` builds it from EVERY
//! visible row in the branch family. The payload is compatibility-only: upstream PR #920
//! removed the validation that read it (conflicts are decided from the staged rows' own
//! parents, `sync_manager::inbox::validate_transactional_parent_frontiers`) and left the
//! capture behind, commented for removal at the next storage-format break. Nothing has
//! read it since.
//!
//! While rows are small this is invisible. Measured on a device store holding 390
//! one-megabyte blob rows, the same code turned every sent message into a 384 MB read and
//! froze the app on every write.
//!
//! The gate asserts the SHAPE, not a duration and not bytes: the captured frontier must be
//! bounded by the batch, never by the store. Shape is what makes it backend-independent —
//! `MemoryStorage` overrides the capture with an in-memory walk that costs nothing to
//! traverse, so a bytes-read assertion would pass here while the real backends bled.

use std::collections::HashMap;

use super::*;
use crate::storage::Storage as _;

/// Unrelated rows seeded before the measured write. More than a handful, so a frontier
/// that tracks the store is unmistakable next to one that tracks the batch.
const UNRELATED_ROWS: usize = 25;

fn transactional() -> WriteContext {
    WriteContext {
        session: None,
        attribution: None,
        updated_at: None,
        batch_mode: Some(crate::batch_fate::BatchMode::Transactional),
        batch_id: None,
        target_branch_name: None,
    }
}

fn user_values(id: ObjectId, name: &str) -> HashMap<String, Value> {
    HashMap::from([
        ("id".to_string(), Value::Uuid(id)),
        ("name".to_string(), Value::Text(name.to_string())),
    ])
}

#[test]
fn a_sealed_transactional_batch_does_not_carry_the_whole_store() {
    let mut s = create_3tier_rc();

    // Seeded as plain direct writes: they become visible immediately, which is what puts
    // them in the visible region the capture walks. Rows left staging-pending are not
    // visible yet and would not reach the frontier, so the gate would pass while the
    // defect stood.
    for index in 0..UNRELATED_ROWS {
        s.a.insert(
            "users",
            user_values(ObjectId::new(), &format!("seed-{index}")),
            None,
        )
        .expect("seed an unrelated row");
    }

    let ((row_id, _values), _receiver) = insert_and_wait_for_batch(
        &mut s.a,
        "users",
        user_values(ObjectId::new(), "Alice"),
        Some(&transactional()),
        DurabilityTier::Local,
    )
    .expect("write the measured row");

    let history =
        s.a.storage()
            .scan_history_row_batches("users", row_id)
            .expect("read the written row's history");
    let batch_id = history
        .first()
        .expect("the write produced a history entry")
        .batch_id;

    // The submission is only persisted once the batch commits.
    s.a.commit_batch(batch_id).expect("commit the batch");

    let submission =
        s.a.storage()
            .load_sealed_batch_submission(batch_id)
            .expect("read the sealed submission")
            .expect("a transactional write seals a submission");

    assert!(
        submission.captured_frontier.len() <= 1,
        "sealing one row carried a frontier of {} members with {UNRELATED_ROWS} unrelated \
         rows in the store — the capture follows the size of the STORE, not the size of \
         the change. Nothing reads this payload (upstream PR #920); on a store holding \
         blob rows it costs a full read of every one of them per write.",
        submission.captured_frontier.len(),
    );
}
