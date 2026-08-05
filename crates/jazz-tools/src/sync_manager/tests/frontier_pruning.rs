//! Fix D1: `sent_batch_ids` is a delivered-frontier cursor, not the full
//! delivered history. These tests pin the pruning rule
//! (`SentBatchIds::record_delivery`), its size bound, the O(1) ancestor-DFS
//! termination it must preserve, and the safety of the one direction pruning
//! is allowed to err in: re-offering a batch the receiver already has.

use super::*;
use std::cell::Cell;

fn serial_chain(row_id: ObjectId, len: usize) -> Vec<StoredRowBatch> {
    let mut chain = Vec::with_capacity(len);
    let mut current = visible_row(row_id, "main", Vec::new(), 1_000, b"root");
    chain.push(current.clone());
    for level in 1..len {
        let next = visible_row(
            row_id,
            "main",
            vec![current.batch_id()],
            1_000 + level as u64,
            format!("c{level}").as_bytes(),
        );
        chain.push(next.clone());
        current = next;
    }
    chain
}

fn server_sent_set(sm: &SyncManager, server_id: ServerId, row_id: ObjectId) -> HashSet<BatchId> {
    sm.servers
        .get(&server_id)
        .and_then(|server| {
            server
                .sent_batch_ids
                .get(&(row_id, BranchName::new("main")))
        })
        .map(|sent| sent.iter().copied().collect())
        .unwrap_or_default()
}

fn client_sent_set(sm: &SyncManager, client_id: ClientId, row_id: ObjectId) -> HashSet<BatchId> {
    sm.clients
        .get(&client_id)
        .and_then(|client| {
            client
                .sent_batch_ids
                .get(&(row_id, BranchName::new("main")))
        })
        .map(|sent| sent.iter().copied().collect())
        .unwrap_or_default()
}

#[test]
fn serial_deliveries_keep_the_server_sent_set_at_frontier_size() {
    const DELIVERIES: usize = 64;

    let io = MemoryStorage::new();
    let mut sm = SyncManager::new();
    let server_id = ServerId::new();
    sm.add_server_with_storage(server_id, false, &io);
    let row_id = ObjectId::new();

    let chain = serial_chain(row_id, DELIVERIES);
    let mut connections_after_second_delivery = None;
    for (index, tip) in chain.iter().enumerate() {
        sm.forward_row_batch_to_servers_with_storage(
            &io,
            "users",
            row_id,
            row_metadata("users"),
            tip.clone(),
        );
        let sent = server_sent_set(&sm, server_id, row_id);
        assert!(
            sent.len() <= 2,
            "serial delivery #{index} grew the sent set to {} ids; the frontier cursor must stay O(1)",
            sent.len()
        );
        assert_eq!(
            sent,
            HashSet::from([tip.batch_id()]),
            "after a serial delivery the frontier is exactly the delivered tip"
        );
        if index == 1 {
            connections_after_second_delivery = Some(sm.memory_size().1);
        }
    }

    assert_eq!(
        Some(sm.memory_size().1),
        connections_after_second_delivery,
        "per-connection retained memory must stay flat across a long serial delivery run"
    );

    let queued = sm
        .take_outbox()
        .into_iter()
        .filter(|entry| {
            matches!(
                entry,
                OutboxEntry {
                    destination: Destination::Server(id),
                    payload: SyncPayload::RowBatchCreated { .. },
                } if *id == server_id
            )
        })
        .count();
    assert_eq!(
        queued, DELIVERIES,
        "pruning must not change what a serial history sends: each batch exactly once"
    );
}

#[test]
fn serial_deliveries_keep_the_client_sent_set_at_frontier_size() {
    const DELIVERIES: usize = 64;

    let io = MemoryStorage::new();
    let mut sm = SyncManager::new();
    let client_id = ClientId::new();
    add_client(&mut sm, &io, client_id);
    let row_id = ObjectId::new();
    set_client_query_scope(
        &mut sm,
        &io,
        client_id,
        QueryId(1),
        HashSet::from([(row_id, BranchName::new("main"))]),
        None,
    );

    for (index, tip) in serial_chain(row_id, DELIVERIES).iter().enumerate() {
        sm.queue_row_to_client(client_id, row_id, row_metadata("users"), tip.clone(), false);
        confirm_queued(&mut sm);
        let sent = client_sent_set(&sm, client_id, row_id);
        assert!(
            sent.len() <= 2,
            "serial delivery #{index} grew the client sent set to {} ids",
            sent.len()
        );
        assert_eq!(sent, HashSet::from([tip.batch_id()]));
    }

    // Pruning reads the parents *before* scope stripping; the delivered
    // payload must still arrive with parents cleared for visible rows.
    for entry in sm.take_outbox() {
        if let OutboxEntry {
            destination: Destination::Client(id),
            payload: SyncPayload::RowBatchNeeded { row, .. },
        } = entry
        {
            assert_eq!(id, client_id);
            assert!(
                row.parents.is_empty(),
                "visible rows delivered to clients must keep shipping without parents"
            );
        }
    }
}

#[test]
fn fork_delivery_retains_both_tips_and_reoffers_the_pruned_ancestor() {
    let mut io = MemoryStorage::new();
    seed_users_schema(&mut io);
    let row_id = ObjectId::new();
    let root = visible_row(row_id, "main", Vec::new(), 1_000, b"root");
    let tip_a = visible_row(row_id, "main", vec![root.batch_id()], 2_000, b"tip-a");
    let tip_b = visible_row(row_id, "main", vec![root.batch_id()], 2_000, b"tip-b");
    io.put_row_locator(
        row_id,
        Some(
            &crate::storage::row_locator_from_metadata(&row_metadata("users"))
                .expect("row metadata should produce a row locator"),
        ),
    )
    .unwrap();
    io.append_history_region_rows("users", &[root.clone(), tip_a.clone(), tip_b.clone()])
        .unwrap();

    let mut sm = SyncManager::new();
    let server_id = ServerId::new();
    sm.add_server_with_storage(server_id, false, &io);

    for row in [&root, &tip_a] {
        sm.forward_row_batch_to_servers_with_storage(
            &io,
            "users",
            row_id,
            row_metadata("users"),
            row.clone(),
        );
    }
    assert_eq!(
        server_sent_set(&sm, server_id, row_id),
        HashSet::from([tip_a.batch_id()]),
        "recording tip A prunes the dominated root"
    );

    // Tip B forks off the pruned root: the DFS misses the frontier, descends
    // through the root and re-offers it — the safe (idempotent) direction.
    sm.forward_row_batch_to_servers_with_storage(
        &io,
        "users",
        row_id,
        row_metadata("users"),
        tip_b.clone(),
    );

    assert_eq!(
        server_sent_set(&sm, server_id, row_id),
        HashSet::from([tip_a.batch_id(), tip_b.batch_id()]),
        "a forked history keeps every delivered tip in the frontier"
    );

    let root_sends = sm
        .take_outbox()
        .into_iter()
        .filter(|entry| {
            matches!(
                entry,
                OutboxEntry {
                    destination: Destination::Server(id),
                    payload: SyncPayload::RowBatchCreated { row, .. },
                } if *id == server_id && row.batch_id() == root.batch_id()
            )
        })
        .count();
    assert_eq!(
        root_sends, 2,
        "the pruned root is re-offered exactly once by the fork delivery"
    );
}

#[test]
fn duplicate_delivery_of_a_pruned_ancestor_is_idempotent_at_the_receiver() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    seed_users_schema(&mut io);
    let server_id = ServerId::new();
    let row_id = ObjectId::new();
    let root = visible_row(row_id, "main", Vec::new(), 1_000, b"root");
    let child = visible_row(row_id, "main", vec![root.batch_id()], 2_000, b"child");

    let deliver = |sm: &mut SyncManager, io: &mut MemoryStorage, row: &StoredRowBatch| {
        sm.process_from_server(
            io,
            server_id,
            SyncPayload::RowBatchCreated {
                metadata: Some(RowMetadata {
                    id: row_id,
                    metadata: row_metadata("users"),
                }),
                row: row.clone(),
            },
        );
    };

    deliver(&mut sm, &mut io, &root);
    deliver(&mut sm, &mut io, &child);
    assert_eq!(sm.take_pending_row_visibility_changes().len(), 2);

    // The re-offer a stale frontier can produce: the exact root batch again.
    deliver(&mut sm, &mut io, &root);

    assert!(
        sm.take_pending_row_visibility_changes().is_empty(),
        "re-applying an identical ancestor batch must be a no-op"
    );
    assert_eq!(
        io.scan_history_row_batches("users", row_id).unwrap().len(),
        2,
        "the duplicate must not append a history row"
    );
    assert_eq!(
        load_visible_row(&io, "users", row_id, "main").batch_id(),
        child.batch_id(),
        "the visible tip must not roll back to the re-offered ancestor"
    );
}

#[test]
fn force_resend_reinserts_the_forced_batch_without_disturbing_the_frontier() {
    let io = MemoryStorage::new();
    let mut sm = SyncManager::new();
    let server_id = ServerId::new();
    sm.add_server_with_storage(server_id, false, &io);
    let row_id = ObjectId::new();

    let chain = serial_chain(row_id, 4);
    for tip in &chain {
        sm.forward_row_batch_to_servers_with_storage(
            &io,
            "users",
            row_id,
            row_metadata("users"),
            tip.clone(),
        );
    }
    let tip = chain.last().expect("chain is non-empty");
    assert_eq!(
        server_sent_set(&sm, server_id, row_id),
        HashSet::from([tip.batch_id()])
    );
    sm.take_outbox();

    // Targeted forget-and-resend of an already-pruned batch (the Missing-fate
    // retransmission path) must still queue it and re-record it as sent.
    let forced = &chain[1];
    sm.force_row_batch_to_servers(row_id, row_metadata("users"), forced.clone());

    let forced_sends = sm
        .take_outbox()
        .into_iter()
        .filter(|entry| {
            matches!(
                entry,
                OutboxEntry {
                    destination: Destination::Server(id),
                    payload: SyncPayload::RowBatchCreated { row, .. },
                } if *id == server_id && row.batch_id() == forced.batch_id()
            )
        })
        .count();
    assert_eq!(forced_sends, 1, "force path must resend the forced batch");
    assert_eq!(
        server_sent_set(&sm, server_id, row_id),
        HashSet::from([tip.batch_id(), forced.batch_id()]),
        "the forced batch rejoins the sent set next to the frontier tip"
    );

    // The next serial write is unaffected: its parent (the tip) is retained,
    // so it prunes the tip and the lingering forced id simply stays behind.
    let next = visible_row(row_id, "main", vec![tip.batch_id()], 9_000, b"next");
    sm.forward_row_batch_to_servers_with_storage(
        &io,
        "users",
        row_id,
        row_metadata("users"),
        next.clone(),
    );
    assert_eq!(
        server_sent_set(&sm, server_id, row_id),
        HashSet::from([next.batch_id(), forced.batch_id()])
    );
}

/// Counts the storage reads the ancestor DFS performs on a miss, so the
/// serial fast path can assert it never descends at all.
struct DfsProbeStorage {
    inner: MemoryStorage,
    history_loads: Cell<usize>,
    fate_loads: Cell<usize>,
}

impl DfsProbeStorage {
    fn new(inner: MemoryStorage) -> Self {
        Self {
            inner,
            history_loads: Cell::new(0),
            fate_loads: Cell::new(0),
        }
    }

    fn reset(&self) {
        self.history_loads.set(0);
        self.fate_loads.set(0);
    }
}

impl Storage for DfsProbeStorage {
    fn raw_table_put(
        &mut self,
        table: &str,
        key: &str,
        value: &[u8],
    ) -> Result<(), crate::storage::StorageError> {
        self.inner.raw_table_put(table, key, value)
    }

    fn raw_table_delete(
        &mut self,
        table: &str,
        key: &str,
    ) -> Result<(), crate::storage::StorageError> {
        self.inner.raw_table_delete(table, key)
    }

    fn raw_table_get(
        &self,
        table: &str,
        key: &str,
    ) -> Result<Option<Vec<u8>>, crate::storage::StorageError> {
        self.inner.raw_table_get(table, key)
    }

    fn raw_table_scan_prefix(
        &self,
        table: &str,
        prefix: &str,
    ) -> Result<crate::storage::RawTableRows, crate::storage::StorageError> {
        self.inner.raw_table_scan_prefix(table, prefix)
    }

    fn raw_table_scan_range(
        &self,
        table: &str,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<crate::storage::RawTableRows, crate::storage::StorageError> {
        self.inner.raw_table_scan_range(table, start, end)
    }

    fn load_row_locator(
        &self,
        id: ObjectId,
    ) -> Result<Option<crate::storage::RowLocator>, crate::storage::StorageError> {
        self.inner.load_row_locator(id)
    }

    fn load_visible_region_entry(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<VisibleRowEntry>, crate::storage::StorageError> {
        self.inner.load_visible_region_entry(table, branch, row_id)
    }

    fn load_history_row_batch(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: BatchId,
    ) -> Result<Option<StoredRowBatch>, crate::storage::StorageError> {
        self.history_loads.set(self.history_loads.get() + 1);
        self.inner
            .load_history_row_batch(table, branch, row_id, batch_id)
    }

    fn load_authoritative_batch_fate(
        &self,
        batch_id: BatchId,
    ) -> Result<Option<BatchFate>, crate::storage::StorageError> {
        self.fate_loads.set(self.fate_loads.get() + 1);
        self.inner.load_authoritative_batch_fate(batch_id)
    }
}

#[test]
fn serial_forward_after_pruning_terminates_without_any_ancestor_walk() {
    const DEPTH: usize = 512;

    let mut inner = MemoryStorage::new();
    seed_users_schema(&mut inner);
    let row_id = ObjectId::new();
    let chain = serial_chain(row_id, DEPTH + 1);
    inner
        .put_row_locator(
            row_id,
            Some(
                &crate::storage::row_locator_from_metadata(&row_metadata("users"))
                    .expect("row metadata should produce a row locator"),
            ),
        )
        .unwrap();
    inner.append_history_region_rows("users", &chain).unwrap();
    let io = DfsProbeStorage::new(inner);

    let mut sm = SyncManager::new();
    let server_id = ServerId::new();
    sm.add_server_with_storage(server_id, false, &io);

    let (next, delivered) = chain.split_last().expect("chain is non-empty");
    for tip in delivered {
        sm.forward_row_batch_to_servers_with_storage(
            &io,
            "users",
            row_id,
            row_metadata("users"),
            tip.clone(),
        );
    }
    assert_eq!(
        server_sent_set(&sm, server_id, row_id).len(),
        1,
        "after {DEPTH} serial deliveries the sent set is the single-tip frontier"
    );

    // The next serial write must terminate on the frontier probe alone: no
    // parent-row loads, no fate loads, despite every inner ancestor id having
    // been pruned from the sent set.
    io.reset();
    sm.forward_row_batch_to_servers_with_storage(
        &io,
        "users",
        row_id,
        row_metadata("users"),
        next.clone(),
    );
    assert_eq!(
        io.history_loads.get(),
        0,
        "a serial forward after pruning must not descend into stored ancestors"
    );
    assert_eq!(
        io.fate_loads.get(),
        0,
        "a serial forward after pruning must not consult ancestor fates"
    );
    assert_eq!(
        server_sent_set(&sm, server_id, row_id),
        HashSet::from([next.batch_id()])
    );
}
