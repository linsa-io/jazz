//! A node that applies a row must report it, and only after the write barrier.
//!
//! The manager-level gates prove what the SERVER does with a confirmation. None of them can
//! prove the confirmation is ever sent, because the emission lives in the runtime tick — and
//! a mechanism whose trigger is never exercised is exactly what shipped inert once already
//! in this work.

use super::*;

use crate::sync_manager::{Destination, InboxEntry, Source, SyncPayload};

/// Applying a row from a server produces a confirmation addressed back to it.
#[test]
fn applying_a_row_confirms_it_upstream() {
    let mut s = create_3tier_rc();
    s.a.set_upstream_supports_delivery_acks(true);

    let ((row_id, _values), _receiver) = insert_and_wait_for_batch(
        &mut s.b,
        "users",
        user_insert_values(ObjectId::new(), "Alice"),
        None,
        DurabilityTier::Local,
    )
    .expect("write a row on the server");

    // Hand the row to the client the way the transport does. Built from the server's own
    // stored batch rather than fished out of its outbox: what is under test is the client's
    // apply-and-report path, not whether this particular fixture happens to forward.
    let row =
        s.b.storage()
            .scan_history_row_batches("users", row_id)
            .expect("read the row's history")
            .into_iter()
            .next()
            .expect("the write produced a batch");
    let metadata = crate::sync_manager::RowMetadata {
        id: row_id,
        metadata: std::collections::HashMap::from([(
            crate::metadata::MetadataKey::Table.to_string(),
            "users".to_string(),
        )]),
    };
    s.a.park_sync_message(InboxEntry {
        source: Source::Server(s.b_server_for_a),
        payload: SyncPayload::RowBatchNeeded {
            metadata: Some(metadata),
            row,
        },
    });

    s.a.batched_tick();

    let confirmed: Vec<_> =
        s.a.sync_sender()
            .take()
            .into_iter()
            .filter_map(|entry| match (&entry.destination, entry.payload) {
                (Destination::Server(server_id), SyncPayload::DeliveryConfirmed { rows })
                    if *server_id == s.b_server_for_a =>
                {
                    Some(rows)
                }
                _ => None,
            })
            .flatten()
            .collect();

    assert!(
        confirmed.iter().any(|row| row.row_id == row_id),
        "the node applied the row and never reported it, so the sender has no way to learn \
         the row arrived — every claim it makes is a guess about its own send path, which \
         is what loses messages when a socket dies without saying so"
    );
}

/// A node whose upstream does not understand confirmations stays silent.
///
/// Sending anyway would be worse than useless: an older server cannot decode the payload,
/// and a frame it cannot decode is rejected whole, taking the writes batched alongside it.
#[test]
fn a_node_talking_to_an_older_server_sends_no_confirmations() {
    let mut s = create_3tier_rc();
    s.a.set_upstream_supports_delivery_acks(false);

    let ((row_id, _values), _receiver) = insert_and_wait_for_batch(
        &mut s.b,
        "users",
        user_insert_values(ObjectId::new(), "Alice"),
        None,
        DurabilityTier::Local,
    )
    .expect("write a row on the server");

    let row =
        s.b.storage()
            .scan_history_row_batches("users", row_id)
            .expect("read the row's history")
            .into_iter()
            .next()
            .expect("the write produced a batch");
    s.a.park_sync_message(InboxEntry {
        source: Source::Server(s.b_server_for_a),
        payload: SyncPayload::RowBatchNeeded {
            metadata: Some(crate::sync_manager::RowMetadata {
                id: row_id,
                metadata: std::collections::HashMap::from([(
                    crate::metadata::MetadataKey::Table.to_string(),
                    "users".to_string(),
                )]),
            }),
            row,
        },
    });

    s.a.batched_tick();

    let sent_any =
        s.a.sync_sender()
            .take()
            .into_iter()
            .any(|entry| matches!(entry.payload, SyncPayload::DeliveryConfirmed { .. }));
    assert!(
        !sent_any,
        "a confirmation went to a server that never said it understands them — an older \
         peer cannot decode it, and the frame it arrives in is rejected whole"
    );
}
