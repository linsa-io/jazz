//! A rejected write must leave every map that tracked it.
//!
//! Three maps track one local write, and they exist separately because they answer
//! separate questions: `pending_local_row_batches` (IndexScan source overlay and row-loader
//! durability downgrade), `scope_exempt_local_rows` and
//! `confirmed_local_rows_awaiting_scope` (read-your-own-writes exemption in
//! `filter_synced_query_scope_tuples`). They retire on different terms — but a row that is
//! going away entirely must leave all of them together.
//!
//! The rejection paths did not. `retract_local_rejected_row` (twice) and
//! `restore_local_rejected_delete_row` each removed from `pending_local_row_batches` alone,
//! so a rejected row kept its scope exemption with no way left to lose it: the only other
//! retirement is an inbound update for the row, and that arm is gated on the row still
//! being in `pending_local_row_batches`. The exemption became permanently unreachable.
//!
//! That is not a leak of memory only. On the rejected-DELETE path the row is restored to
//! visible locally, so the author goes on reading its own replica of a row the server may
//! since have dropped from the query scope — the same defect
//! `query_manager::manager_tests::local_write_exemption` was written for, reachable by any
//! row that ever had a batch rejected. Production is not short of those: one incident
//! recorded 264 rejected batches in 26 minutes.
//!
//! The gate asserts the family retires together, which is the invariant that keeps the next
//! map added to it from being forgotten the same way.

use super::*;

#[test]
fn a_rejected_write_leaves_every_map_that_tracked_it() {
    let schema = protected_documents_schema();
    let mut client = create_runtime_with_schema(schema.clone(), "rejected-write-tracking");
    let mut server = create_runtime_with_schema(schema, "rejected-write-tracking");

    let client_id = ClientId::new();
    let server_id = ServerId::new();
    // The server knows this connection as mallory while the client writes alice's rows:
    // the write satisfies the client's own policy and is denied on arrival. That is what
    // produces a real `Rejected` fate rather than an injected one.
    server.add_client(client_id, Some(Session::new("mallory")));
    client.add_server(server_id);
    let alice_session = Session::new("alice");

    let ((row_id, _values), _batch_id) = client
        .insert(
            "documents",
            document_insert_values("alice", "denied-doc"),
            Some(&WriteContext::from_session(alice_session)),
        )
        .expect("the write satisfies the client's own policy");
    client.batched_tick();

    let tracked_while_pending = client
        .schema_manager()
        .query_manager()
        .local_row_tracking(row_id);
    assert!(
        tracked_while_pending.scope_exempt,
        "fixture precondition: a fresh local write must be scope-exempt, else this gates \
         nothing — got {tracked_while_pending:?}"
    );

    pump_client_messages_to_server(&mut client, &mut server, server_id, client_id);

    // Carry the rejection back. This is the path that used to strip the row from
    // `pending_local_row_batches` and leave the exemption behind.
    let mut server_outputs = Vec::new();
    let mut rejected = false;
    for _ in 0..10 {
        pump_server_messages_to_clients(
            &mut server,
            &mut [ClientForServer {
                core: &mut client,
                server_id,
                client_id,
            }],
            &mut server_outputs,
        );
        client.batched_tick();
        client.immediate_tick();
        rejected |= server_outputs.iter().any(|entry| {
            matches!(
                &entry.payload,
                SyncPayload::BatchFate { fate } if fate.is_rejected()
            )
        });
    }
    assert!(
        rejected,
        "fixture precondition: the server must reject the write, else this gates nothing"
    );

    let tracking = client
        .schema_manager()
        .query_manager()
        .local_row_tracking(row_id);
    assert!(
        tracking.is_retired(),
        "a rejected write must leave every map that tracked it. The row is gone — there is \
         no local write left to read back — but a residual scope exemption has no retirement \
         path of its own: the inbound-update arm is gated on the row still being in \
         `pending_local_row_batches`, which the rejection just cleared. Whatever the server \
         says about this row from now on, this node would keep answering from its own \
         replica. Got {tracking:?}"
    );
}
