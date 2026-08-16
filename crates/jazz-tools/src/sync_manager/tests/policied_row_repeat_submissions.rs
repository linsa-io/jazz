//! PROBE (defect 27 research): repeated policied-row direct writes in ONE server process.
//!
//! Live law to reproduce: a client's 10s presence heartbeat on a POLICIED `users` row
//! lands on the server exactly once per server process lifetime and then never again,
//! with no log at any level. This file drives the server side of the beat protocol the
//! way a real direct write drives it — `RowBatchCreated` (StagingPending) followed by
//! `SealBatch` — twice in a row, and asserts the second beat wins visibility.

use super::*;

/// One heartbeat as a real direct write puts it on the wire: the staged row, then the
/// seal that publishes it. Permission checks are drained by approving, as the server
/// runtime does for an authorized write.
fn beat<H: Storage>(sm: &mut SyncManager, io: &mut H, client_id: ClientId, row: &StoredRowBatch) {
    sm.push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: row.row_id,
                metadata: row_metadata("users"),
            }),
            row: row.clone(),
        },
    });
    sm.process_inbox(io);
    for check in sm.take_pending_permission_checks() {
        sm.approve_permission_check(io, check);
    }

    sm.push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::SealBatch {
            submission: sealed_submission(
                row.batch_id,
                "main",
                vec![SealedBatchMember {
                    object_id: row.row_id,
                    row_digest: row.content_digest(),
                }],
                Vec::new(),
            ),
        },
    });
    sm.process_inbox(io);
    for check in sm.take_pending_permission_checks() {
        sm.approve_permission_check(io, check);
    }
}

#[test]
fn a_second_policied_row_update_applies_in_the_same_server_process() {
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
    let b1 = row_with_state(
        visible_row(row_id, "main", Vec::new(), 1_000, b"hb-1"),
        crate::row_histories::RowState::StagingPending,
        None,
    );
    beat(&mut sm, &mut io, client_id, &b1);

    let after_first = io
        .load_visible_region_row("users", "main", row_id)
        .expect("visible read should succeed");
    assert_eq!(
        after_first.as_ref().map(|row| row.batch_id()),
        Some(b1.batch_id),
        "control: the first beat must be visible, else the harness is wrong"
    );

    let b2 = row_with_state(
        visible_row(row_id, "main", vec![b1.batch_id], 2_000, b"hb-2"),
        crate::row_histories::RowState::StagingPending,
        None,
    );
    beat(&mut sm, &mut io, client_id, &b2);

    let after_second = io
        .load_visible_region_row("users", "main", row_id)
        .expect("visible read should succeed");
    assert_eq!(
        after_second.as_ref().map(|row| row.batch_id()),
        Some(b2.batch_id),
        "the SECOND policied-row update in the same server process must apply; live it \
         never does (server frozen between restarts)"
    );
}

// ---------------------------------------------------------------------------
// Client-side enqueue probes: does a fresh local write to a row whose ancestry
// carries a terminal rejection ever reach the outbox?
// ---------------------------------------------------------------------------

fn seed_local_history(io: &mut MemoryStorage, row_id: ObjectId, rows: &[StoredRowBatch]) {
    seed_users_schema(io);
    io.put_row_locator(
        row_id,
        Some(
            &crate::storage::row_locator_from_metadata(&row_metadata("users"))
                .expect("row metadata should produce a row locator"),
        ),
    )
    .unwrap();
    io.append_history_region_rows("users", rows).unwrap();
    let newest = rows.last().expect("at least one row");
    io.upsert_visible_region_rows(
        "users",
        std::slice::from_ref(&VisibleRowEntry::rebuild(newest.clone(), rows)),
    )
    .unwrap();
}

fn queued_row_batch_ids(sm: &mut SyncManager, server_id: ServerId) -> Vec<BatchId> {
    sm.take_outbox()
        .into_iter()
        .filter_map(|entry| match entry {
            OutboxEntry {
                destination: Destination::Server(id),
                payload: SyncPayload::RowBatchCreated { row, .. },
            } if id == server_id => Some(row.batch_id()),
            _ => None,
        })
        .collect()
}

/// The live row's ancestry carries a terminal rejection from the defect-26 era. Every
/// later beat is a fresh batch parented on the previous one. With the per-server delivered
/// frontier EMPTY — its state right after a (re)connect, `ServerState::default()` — the
/// ancestor walk descends past the frontier and reaches the rejection.
#[test]
fn a_fresh_beat_is_queued_even_when_an_ancestor_was_rejected_and_the_frontier_is_empty() {
    let mut io = MemoryStorage::new();
    let row_id = ObjectId::new();
    let rejected = visible_row(row_id, "main", Vec::new(), 1_000, b"rejected-era");
    let middle = visible_row(row_id, "main", vec![rejected.batch_id], 2_000, b"healed");
    let beat = visible_row(row_id, "main", vec![middle.batch_id], 3_000, b"beat");
    seed_local_history(
        &mut io,
        row_id,
        &[rejected.clone(), middle.clone(), beat.clone()],
    );
    io.upsert_authoritative_batch_fate(&BatchFate::Rejected {
        batch_id: rejected.batch_id,
        code: "permission_denied".to_string(),
        reason: "the defect-26 era".to_string(),
    })
    .unwrap();

    let mut sm = SyncManager::new();
    let server_id = ServerId::new();
    sm.add_server_with_storage(server_id, true, &io);
    sm.take_outbox();

    sm.forward_row_batch_to_servers_with_storage(
        &io,
        "users",
        row_id,
        row_metadata("users"),
        beat.clone(),
    );

    let queued = queued_row_batch_ids(&mut sm, server_id);
    assert!(
        queued.contains(&beat.batch_id),
        "a fresh local beat must be queued to the server; the ancestor walk abandoned the \
         whole row because an ANCESTOR carries a terminal rejection (queued: {queued:?})"
    );
}

/// Same history, but the frontier already covers the beat's direct parent — the state
/// after one successful send on this connection. The walk stops at the frontier and never
/// sees the rejection.
#[test]
fn the_same_beat_is_queued_when_the_frontier_already_covers_its_parent() {
    let mut io = MemoryStorage::new();
    let row_id = ObjectId::new();
    let rejected = visible_row(row_id, "main", Vec::new(), 1_000, b"rejected-era");
    let middle = visible_row(row_id, "main", vec![rejected.batch_id], 2_000, b"healed");
    let beat = visible_row(row_id, "main", vec![middle.batch_id], 3_000, b"beat");
    seed_local_history(
        &mut io,
        row_id,
        &[rejected.clone(), middle.clone(), beat.clone()],
    );
    io.upsert_authoritative_batch_fate(&BatchFate::Rejected {
        batch_id: rejected.batch_id,
        code: "permission_denied".to_string(),
        reason: "the defect-26 era".to_string(),
    })
    .unwrap();

    let mut sm = SyncManager::new();
    let server_id = ServerId::new();
    sm.add_server_with_storage(server_id, true, &io);
    // One earlier send on this connection put the parent in the delivered frontier.
    sm.forward_row_batch_to_servers_with_storage(
        &io,
        "users",
        row_id,
        row_metadata("users"),
        middle.clone(),
    );
    sm.take_outbox();

    sm.forward_row_batch_to_servers_with_storage(
        &io,
        "users",
        row_id,
        row_metadata("users"),
        beat.clone(),
    );

    let queued = queued_row_batch_ids(&mut sm, server_id);
    assert!(
        queued.contains(&beat.batch_id),
        "with the parent already in the frontier the walk must stop there and queue the \
         beat (queued: {queued:?})"
    );
}

/// Capture the `warn`-and-above events a body emits on this thread.
///
/// The withhold must never be silent again: the gap between "written locally" and
/// "missing upstream" said nothing at any level, which is why the defect hid for days.
fn captured_logs<R>(body: impl FnOnce() -> R) -> (R, String) {
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    struct SharedWriter(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for SharedWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .expect("the log buffer lock is not poisoned")
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for SharedWriter {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    let buffer = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::fmt()
        .with_writer(SharedWriter(Arc::clone(&buffer)))
        .with_ansi(false)
        .with_max_level(tracing::Level::WARN)
        .finish();
    let result = tracing::subscriber::with_default(subscriber, body);
    let logs = String::from_utf8(
        buffer
            .lock()
            .expect("the log buffer lock is not poisoned")
            .clone(),
    )
    .expect("captured logs are utf-8");
    (result, logs)
}

/// A terminal rejection is confined to the ONE ancestor that carries it. It is still
/// withheld — the authority denied it, resending earns the same denial — but it does not
/// silence that row's fresh writes, it does not silence a DIFFERENT row sharing the
/// connection, and it is announced in the log naming the row and the reason.
#[test]
fn a_rejected_ancestor_is_withheld_loudly_and_leaves_other_rows_alone() {
    let mut io = MemoryStorage::new();

    let poisoned_id = ObjectId::new();
    let rejected = visible_row(poisoned_id, "main", Vec::new(), 1_000, b"rejected-era");
    let poisoned_beat = visible_row(
        poisoned_id,
        "main",
        vec![rejected.batch_id],
        2_000,
        b"poisoned-beat",
    );
    seed_local_history(
        &mut io,
        poisoned_id,
        &[rejected.clone(), poisoned_beat.clone()],
    );
    io.upsert_authoritative_batch_fate(&BatchFate::Rejected {
        batch_id: rejected.batch_id,
        code: "schema_unavailable".to_string(),
        reason: "schema unavailable for branch main".to_string(),
    })
    .unwrap();

    let neighbour_id = ObjectId::new();
    let neighbour_root = visible_row(neighbour_id, "main", Vec::new(), 1_000, b"neighbour-root");
    let neighbour_beat = visible_row(
        neighbour_id,
        "main",
        vec![neighbour_root.batch_id],
        2_000,
        b"neighbour-beat",
    );
    seed_local_history(
        &mut io,
        neighbour_id,
        &[neighbour_root.clone(), neighbour_beat.clone()],
    );

    let mut sm = SyncManager::new();
    let server_id = ServerId::new();
    sm.add_server_with_storage(server_id, true, &io);
    sm.take_outbox();

    let (queued, logs) = captured_logs(|| {
        sm.forward_row_batch_to_servers_with_storage(
            &io,
            "users",
            poisoned_id,
            row_metadata("users"),
            poisoned_beat.clone(),
        );
        sm.forward_row_batch_to_servers_with_storage(
            &io,
            "users",
            neighbour_id,
            row_metadata("users"),
            neighbour_beat.clone(),
        );
        queued_row_batch_ids(&mut sm, server_id)
    });

    assert!(
        queued.contains(&poisoned_beat.batch_id),
        "the poisoned row's own fresh write must still be queued (queued: {queued:?})"
    );
    assert!(
        !queued.contains(&rejected.batch_id),
        "the rejected ancestor itself must stay withheld — the authority already denied \
         it (queued: {queued:?})"
    );
    assert!(
        queued.contains(&neighbour_beat.batch_id),
        "a DIFFERENT row must be untouched by another row's rejection (queued: {queued:?})"
    );
    assert!(
        logs.contains("withholding a rejected ancestor"),
        "the withhold must be logged at warn — silence is what hid this defect \
         (captured: {logs:?})"
    );
    assert!(
        logs.contains(&poisoned_id.to_string())
            && logs.contains("schema unavailable for branch main"),
        "the withhold log must name the row it withheld from and why \
         (captured: {logs:?})"
    );
    assert!(
        !logs.contains(&neighbour_id.to_string()),
        "the untouched row must not appear in any withhold log (captured: {logs:?})"
    );
}

/// Control: the identical history and frontier state, with NO rejected ancestor. If this
/// passes while the two above fail, the terminal rejection in the ancestry — not the
/// harness — is what silences the row.
#[test]
fn control_the_same_beat_is_queued_when_no_ancestor_was_rejected() {
    let mut io = MemoryStorage::new();
    let row_id = ObjectId::new();
    let root = visible_row(row_id, "main", Vec::new(), 1_000, b"root");
    let middle = visible_row(row_id, "main", vec![root.batch_id], 2_000, b"healed");
    let beat = visible_row(row_id, "main", vec![middle.batch_id], 3_000, b"beat");
    seed_local_history(
        &mut io,
        row_id,
        &[root.clone(), middle.clone(), beat.clone()],
    );

    let mut sm = SyncManager::new();
    let server_id = ServerId::new();
    sm.add_server_with_storage(server_id, true, &io);
    sm.take_outbox();

    sm.forward_row_batch_to_servers_with_storage(
        &io,
        "users",
        row_id,
        row_metadata("users"),
        beat.clone(),
    );

    let queued = queued_row_batch_ids(&mut sm, server_id);
    assert!(
        queued.contains(&beat.batch_id),
        "control must be green: without a rejected ancestor the beat is queued \
         (queued: {queued:?})"
    );
}
