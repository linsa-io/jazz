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

/// A tick must cost what there is to do, not what the store has kept.
///
/// `recover_completed_sealed_batches_with_storage` is the first statement of every
/// `immediate_tick` (`runtime_core/ticks.rs`), and it re-reads the WHOLE retained
/// sealed-submission table each time: a prefix scan, a decode per row (which resolves a
/// branch name per member ord), then a point-get of that batch's authoritative fate — and
/// then `continue`s on everything already fated. The work it can actually do is bounded by
/// the submissions that are still drivable; the price it pays is bounded by the table.
///
/// Retention makes that gap permanent rather than transient. A submission whose stored fate
/// is `Missing` can never be deleted (`fate_settled_at` does not call it terminal) and can
/// never be resolved (`fate_needs_settlement_at` excludes it), so it is re-read forever. A
/// production store measured 1284 of them — every one already fated, none drivable — at
/// 11.06 ms of storage reads per tick. Under a user simply typing into one chat draft that
/// was 70% of the server's CPU, and because ticks are serialized under the runtime mutex
/// and debounced at 1 ms, an 11 ms floor per tick does not merely burn CPU: it sets the
/// tick rate for every client on the process.
///
/// The gate asserts the SHAPE — how many fates the tick looked up and whether it walked the
/// table at all — not a duration and not bytes. A timing assertion would encode this
/// machine, and a bytes assertion would pass on a backend that answers from memory.
const RETAINED_BUT_UNDRIVABLE: usize = 200;

#[test]
#[ignore = "open: the fate point-read per retained submission is still O(retained). The row \
            reads and decodes are gone (see the gate below); removing the fate reads needs an \
            index of drivable submissions maintained where the fate is written."]
fn a_tick_does_not_re_read_every_settled_submission() {
    let sweep = Arc::new(Mutex::new(SweepCallCounts::default()));
    let app_id = AppId::from_name("sealed-batch-sweep-cost");
    // The sweep returns immediately when `my_tiers` is empty, so a runtime without a
    // durability tier would gate nothing at all.
    let schema_manager = SchemaManager::new(
        SyncManager::new().with_durability_tier(DurabilityTier::Local),
        test_schema(),
        app_id,
        "dev",
        "main",
    )
    .unwrap();
    let mut core = new_test_core(
        schema_manager,
        Box::new(RowMutationObservingStorage::observing_sweep(Arc::clone(
            &sweep,
        ))) as Box<dyn Storage>,
        NoopScheduler,
    );
    core.immediate_tick();

    // Every seeded submission carries a fate the sweep can do nothing with: confirmed above
    // this node's own tier, so `can_promote_direct_fate` is false and the loop `continue`s.
    // This is the population that accumulates in a real store.
    for index in 0..RETAINED_BUT_UNDRIVABLE {
        let batch_id = BatchId::new();
        let row_id = ObjectId::new();
        core.storage_mut()
            .upsert_sealed_batch_submission(&SealedBatchSubmission::new(
                batch_id,
                crate::batch_fate::BatchMode::Direct,
                crate::object::BranchName::new("main"),
                vec![SealedBatchMember {
                    object_id: row_id,
                    row_digest: crate::digest::Digest32([index as u8; 32]),
                }],
                Vec::new(),
            ))
            .unwrap();
        core.storage_mut()
            .upsert_authoritative_batch_fate(&crate::batch_fate::BatchFate::DurableDirect {
                batch_id,
                confirmed_tier: DurabilityTier::GlobalServer,
            })
            .unwrap();
    }

    *sweep.lock().unwrap() = SweepCallCounts::default();
    core.immediate_tick();
    let measured = *sweep.lock().unwrap();

    assert!(
        measured.authoritative_fate_gets <= 4,
        "a tick looked up {} batch fates with nothing to settle. Every retained submission \
         costs one point read per tick, forever: the ones seeded here are confirmed above \
         this node's tier, so the sweep reads each of them only to `continue`. The cost \
         must follow the drivable work ({} submissions are not drivable), not the size of \
         the table. Scans of the submission table this tick: {}",
        measured.authoritative_fate_gets,
        RETAINED_BUT_UNDRIVABLE,
        measured.sealed_submission_scans
    );
}

/// A tick must not read the rows of submissions it cannot drive.
///
/// The sweep's discriminator is cheap — one point read of the batch's authoritative fate —
/// and its payload is expensive: reading a submission row means decoding it, and every
/// decode resolves a branch name by ord, a second random read. Doing the expensive half
/// first meant a store full of already-fated submissions paid for a decode and a
/// branch-name lookup per row, every tick, to learn each time that there was nothing to do.
///
/// Measured on a production store: 1284 retained submissions, none drivable, 11.06 ms per
/// tick — of which 8.75 ms was the value scan and its decodes and only 2.31 ms the fate
/// reads. This gate pins the order, not the timing: fate first, row only for survivors.
#[test]
fn a_tick_does_not_read_the_rows_of_submissions_it_cannot_drive() {
    let sweep = Arc::new(Mutex::new(SweepCallCounts::default()));
    let app_id = AppId::from_name("sealed-batch-sweep-order");
    let schema_manager = SchemaManager::new(
        SyncManager::new().with_durability_tier(DurabilityTier::Local),
        test_schema(),
        app_id,
        "dev",
        "main",
    )
    .unwrap();
    let mut core = new_test_core(
        schema_manager,
        Box::new(RowMutationObservingStorage::observing_sweep(Arc::clone(
            &sweep,
        ))) as Box<dyn Storage>,
        NoopScheduler,
    );
    core.immediate_tick();

    for index in 0..RETAINED_BUT_UNDRIVABLE {
        let batch_id = BatchId::new();
        core.storage_mut()
            .upsert_sealed_batch_submission(&SealedBatchSubmission::new(
                batch_id,
                crate::batch_fate::BatchMode::Direct,
                crate::object::BranchName::new("main"),
                vec![SealedBatchMember {
                    object_id: ObjectId::new(),
                    row_digest: crate::digest::Digest32([index as u8; 32]),
                }],
                Vec::new(),
            ))
            .unwrap();
        core.storage_mut()
            .upsert_authoritative_batch_fate(&crate::batch_fate::BatchFate::DurableDirect {
                batch_id,
                confirmed_tier: DurabilityTier::GlobalServer,
            })
            .unwrap();
    }

    *sweep.lock().unwrap() = SweepCallCounts::default();
    core.immediate_tick();
    let measured = *sweep.lock().unwrap();

    assert_eq!(
        (
            measured.submission_row_reads,
            measured.branch_name_gets,
            measured.sealed_submission_scans
        ),
        (0, 0, 0),
        "a tick read {} submission rows, {} branch names and did {} value scans of the \
         submission table, with {} retained submissions and none of them drivable. The fate \
         of each batch already said so before any row was touched: the row read, its decode, \
         and the branch-name lookup the decode performs are all work spent to reach a \
         `continue`.",
        measured.submission_row_reads,
        measured.branch_name_gets,
        measured.sealed_submission_scans,
        RETAINED_BUT_UNDRIVABLE
    );
}
