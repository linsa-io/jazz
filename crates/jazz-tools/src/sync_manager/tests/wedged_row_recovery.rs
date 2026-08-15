//! Defect-26 gates: one failed storage commit must not wedge a row's sync forever.
//!
//! Measured live (production 2026-08-15, sync server): a single transient ENOSPC on a
//! rocksdb commit made one heartbeat batch fail to apply, and every subsequent batch of
//! that row — each parenting the previous — died with `ParentNotFound(<the failed batch>)`
//! at ~10 s cadence, 548 failures over two wedged `users` rows, long after the disk was
//! freed. Nothing retried, nothing requested the missing ancestor, and the sender's dedup
//! bookkeeping had already recorded the failed batch as delivered, so it was never offered
//! again.
//!
//! Gate A replays the incident shape end to end through the permission-approval path (the
//! `source="permission_approval"` of the live logs): a chain of three parented batches
//! whose FIRST batch hits a transient commit failure; after the storage heals, the row
//! must converge — every batch applied, the newest visible.
//!
//! Gate B pins the protocol face the healing rests on: a batch arriving with a genuinely
//! missing parent must ask the sending client for the ancestor (`BatchFate::Missing`, the
//! existing retransmission instruction) and must apply once the ancestor arrives. Its
//! negative controls: a malformed batch (no metadata, no locator) is still dropped
//! terminally, and the ancestor ask is bounded — resending the orphan does not multiply
//! the requests.

use super::*;
use crate::storage::StorageError;

fn enospc() -> StorageError {
    StorageError::IoError(
        "rocksdb txn commit: IO error: No space left on device: While appending to file: \
         /data/jazz.rocksdb/000936.log"
            .to_string(),
    )
}

/// Send one row batch from a client and drain the permission queue by approving
/// everything, exactly as the server runtime does for an authorized write.
fn send_and_approve<H: Storage>(
    sm: &mut SyncManager,
    io: &mut H,
    client_id: ClientId,
    row_id: ObjectId,
    row: &StoredRowBatch,
) {
    sm.push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: row_id,
                metadata: row_metadata("users"),
            }),
            row: row.clone(),
        },
    });
    sm.process_inbox(io);
    for check in sm.take_pending_permission_checks() {
        sm.approve_permission_check(io, check);
    }
}

fn history_batch_ids<H: Storage>(io: &H, row_id: ObjectId) -> HashSet<BatchId> {
    io.scan_history_row_batches("users", row_id)
        .expect("history scan should succeed on healthy storage")
        .into_iter()
        .map(|row| row.batch_id())
        .collect()
}

fn missing_answers_for(outbox: &[OutboxEntry], client_id: ClientId, parent: BatchId) -> usize {
    outbox
        .iter()
        .filter(|entry| {
            matches!(
                entry,
                OutboxEntry {
                    destination: Destination::Client(id),
                    payload: SyncPayload::BatchFate {
                        fate: BatchFate::Missing { batch_id },
                    },
                } if *id == client_id && *batch_id == parent
            )
        })
        .count()
}

/// Gate A — the incident: a transient commit failure on the first batch of a chain, then
/// the storage heals and the chain keeps arriving. The row must converge; before the fix
/// it stays wedged forever (the failed batch is dropped, every child dies with
/// `ParentNotFound`, and nothing requests or retries anything).
#[test]
fn a_row_wedged_by_a_transient_commit_failure_converges_after_the_storage_heals() {
    let mut io = MemoryStorage::new();
    seed_users_schema(&mut io);

    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::GlobalServer);
    let client_id = ClientId::new();
    add_client(&mut sm, &io, client_id);
    sm.set_client_role(client_id, ClientRole::User);
    sm.set_client_session(
        client_id,
        crate::query_manager::session::Session::new("alice"),
    );
    sm.take_outbox();

    let row_id = ObjectId::new();
    let b1 = visible_row(row_id, "main", Vec::new(), 1_000, b"hb-1");
    let b2 = visible_row(row_id, "main", vec![b1.batch_id], 2_000, b"hb-2");
    let b3 = visible_row(row_id, "main", vec![b2.batch_id], 3_000, b"hb-3");

    // The first commit fails transiently — the incident's ENOSPC.
    io.set_row_mutation_failure(Some(enospc()));
    send_and_approve(&mut sm, &mut io, client_id, row_id, &b1);

    // The disk is freed; the client, whose local applies all succeeded, keeps sending
    // only the newest batch, each parenting the previous.
    io.set_row_mutation_failure(None);
    send_and_approve(&mut sm, &mut io, client_id, row_id, &b2);
    send_and_approve(&mut sm, &mut io, client_id, row_id, &b3);

    let applied = history_batch_ids(&io, row_id);
    assert_eq!(
        applied,
        HashSet::from([b1.batch_id, b2.batch_id, b3.batch_id]),
        "the row must converge once the storage heals and the row sees traffic — a \
         transient commit failure wedged it permanently instead (applied: {applied:?})"
    );
    let visible = io
        .load_visible_region_row("users", "main", row_id)
        .expect("visible read should succeed")
        .expect("the row must have a visible version after convergence");
    assert_eq!(
        visible.batch_id(),
        b3.batch_id,
        "the newest batch of the healed chain must win visibility"
    );
}

/// Gate B — the protocol face: a batch with a genuinely missing parent asks the sending
/// client for the ancestor, and applies once the ancestor arrives.
#[test]
fn a_missing_parent_is_requested_from_the_sender_and_the_child_applies_when_it_arrives() {
    let mut io = MemoryStorage::new();
    seed_users_schema(&mut io);

    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::GlobalServer);
    let client_id = ClientId::new();
    add_client(&mut sm, &io, client_id);
    sm.set_client_role(client_id, ClientRole::Backend);
    sm.take_outbox();

    let row_id = ObjectId::new();
    let parent = visible_row(row_id, "main", Vec::new(), 1_000, b"parent");
    let child = visible_row(row_id, "main", vec![parent.batch_id], 2_000, b"child");

    // The child arrives first; its parent was never sent.
    sm.push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: row_id,
                metadata: row_metadata("users"),
            }),
            row: child.clone(),
        },
    });
    sm.process_inbox(&mut io);

    let outbox = sm.take_outbox();
    assert_eq!(
        missing_answers_for(&outbox, client_id, parent.batch_id),
        1,
        "the server must ask the sending client to retransmit the missing ancestor \
         (BatchFate::Missing for the parent), got outbox: {outbox:?}"
    );

    // The client's Missing handler retransmits the ancestor's rows.
    sm.push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: row_id,
                metadata: row_metadata("users"),
            }),
            row: parent.clone(),
        },
    });
    sm.process_inbox(&mut io);

    let applied = history_batch_ids(&io, row_id);
    assert_eq!(
        applied,
        HashSet::from([parent.batch_id, child.batch_id]),
        "once the requested ancestor arrives, the parked child must apply with it \
         (applied: {applied:?})"
    );
    let visible = io
        .load_visible_region_row("users", "main", row_id)
        .expect("visible read should succeed")
        .expect("the row must be visible after the ancestor arrives");
    assert_eq!(
        visible.batch_id(),
        child.batch_id,
        "the child must win visibility over the backfilled ancestor"
    );
}

/// Gate B negative control: a malformed batch — no metadata, no locator — is terminally
/// dropped, exactly as before. It must not be parked and must not generate an ancestor
/// request.
#[test]
fn a_malformed_batch_is_still_dropped_without_parking_or_requests() {
    let mut io = MemoryStorage::new();
    seed_users_schema(&mut io);

    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::GlobalServer);
    let client_id = ClientId::new();
    add_client(&mut sm, &io, client_id);
    sm.set_client_role(client_id, ClientRole::Backend);
    sm.take_outbox();

    let row_id = ObjectId::new();
    let orphan = visible_row(row_id, "main", vec![BatchId::new()], 1_000, b"foreign");

    sm.push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::RowBatchCreated {
            metadata: None,
            row: orphan,
        },
    });
    sm.process_inbox(&mut io);

    assert!(
        sm.parked_row_batches.is_empty(),
        "an unusable batch must not be parked"
    );
    assert!(
        !sm.take_outbox().iter().any(|entry| matches!(
            entry.payload,
            SyncPayload::BatchFate {
                fate: BatchFate::Missing { .. }
            }
        )),
        "an unusable batch must not generate an ancestor request"
    );
    assert!(
        history_batch_ids(&io, row_id).is_empty(),
        "an unusable batch must not land in history"
    );
}

/// Gate B negative control: the ancestor ask is bounded. A peer that keeps resending the
/// orphan without ever supplying its parent gets the request once per budget window, not
/// once per resend — the request path must not loop.
#[test]
fn resending_an_orphan_does_not_multiply_ancestor_requests_or_grow_the_park() {
    let mut io = MemoryStorage::new();
    seed_users_schema(&mut io);

    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::GlobalServer);
    let client_id = ClientId::new();
    add_client(&mut sm, &io, client_id);
    sm.set_client_role(client_id, ClientRole::Backend);
    sm.take_outbox();

    let row_id = ObjectId::new();
    let parent = visible_row(row_id, "main", Vec::new(), 1_000, b"never-sent");
    let child = visible_row(row_id, "main", vec![parent.batch_id], 2_000, b"orphan");

    let mut total_requests = 0;
    for _ in 0..5 {
        sm.push_inbox(InboxEntry {
            source: Source::Client(client_id),
            payload: SyncPayload::RowBatchCreated {
                metadata: Some(RowMetadata {
                    id: row_id,
                    metadata: row_metadata("users"),
                }),
                row: child.clone(),
            },
        });
        sm.process_inbox(&mut io);
        total_requests += missing_answers_for(&sm.take_outbox(), client_id, parent.batch_id);
    }

    assert_eq!(
        total_requests, 1,
        "five resends inside the answer interval must produce exactly one ancestor \
         request — the budget that bounds the seal path's Missing answers must bound \
         this one too"
    );
    let parked = sm
        .parked_row_batches
        .get(&(row_id, BranchName::new("main")))
        .map(|queue| queue.len())
        .unwrap_or(0);
    assert_eq!(
        parked, 1,
        "resending the same orphan must refresh the parked entry in place, not grow the \
         queue"
    );
}

/// The park is capped per row: the oldest parked batch is evicted once the row holds
/// `MAX_PARKED_ROW_BATCHES_PER_ROW` unappliable batches. Nothing is lost terminally —
/// the sender still holds every evicted batch — but memory must not track the wedge's
/// duration.
#[test]
fn the_park_evicts_its_oldest_batch_at_the_per_row_cap() {
    let mut io = MemoryStorage::new();
    seed_users_schema(&mut io);

    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::GlobalServer);
    let client_id = ClientId::new();
    add_client(&mut sm, &io, client_id);
    sm.set_client_role(client_id, ClientRole::Backend);
    sm.take_outbox();

    let row_id = ObjectId::new();
    let mut orphans = Vec::new();
    for index in 0..(MAX_PARKED_ROW_BATCHES_PER_ROW + 1) {
        let never_sent_parent = BatchId::new();
        let orphan = visible_row(
            row_id,
            "main",
            vec![never_sent_parent],
            1_000 + index as u64,
            b"orphan",
        );
        sm.push_inbox(InboxEntry {
            source: Source::Client(client_id),
            payload: SyncPayload::RowBatchCreated {
                metadata: Some(RowMetadata {
                    id: row_id,
                    metadata: row_metadata("users"),
                }),
                row: orphan.clone(),
            },
        });
        sm.process_inbox(&mut io);
        orphans.push(orphan);
    }

    let queue = sm
        .parked_row_batches
        .get(&(row_id, BranchName::new("main")))
        .expect("the row must have parked batches");
    assert_eq!(
        queue.len(),
        MAX_PARKED_ROW_BATCHES_PER_ROW,
        "the park must hold exactly the cap after overflowing it"
    );
    assert!(
        !queue
            .iter()
            .any(|parked| parked.row.batch_id == orphans[0].batch_id),
        "the OLDEST parked batch must be the one evicted"
    );
    assert!(
        queue
            .iter()
            .any(|parked| parked.row.batch_id == orphans.last().unwrap().batch_id),
        "the newest parked batch must be retained"
    );
}
