//! A payload that was never delivered must not be recorded as delivered.
//!
//! `queue_row_to_client` records the delivery — `sent_metadata`, and `record_delivery` on
//! `sent_batch_ids` — and only then pushes the payload onto the outbox. Nothing rolls that
//! back, and the payload is dropped silently on two paths in
//! `ConnectionEventHub::dispatch_prepared`: the client has no registered stream at that
//! instant, or its channel is already dead. Afterwards the row is never offered again,
//! because the claim short-circuits every later attempt.
//!
//! This is not the "peer is offline" case the eight tests in `offline_delivery_edges.rs`
//! and `offline_reap_delivery.rs` cover. Those tear the client down and subscribe again,
//! which mints a fresh query id; the server then treats the subscription as new, re-derives
//! its scope, and sets `force_resend` — which busts the claim as a side effect. A phone
//! whose WiFi drops for three seconds does not do that: its transport reconnects underneath
//! a subscription that stayed alive, replaying the SAME query id, and the server recognises
//! it as equivalent and already settled. No re-derivation, no `force_resend`, and the row
//! stays undelivered for as long as the client's server-side state lives.
//!
//! So this test deliberately does not reconnect at all. It pins the invariant underneath
//! all of that: queue a row, never deliver it, and the next ordinary attempt must still
//! offer it.

use super::*;

/// Confirm the given queued payloads as the receiver would.
fn confirm_queued_entries(sm: &mut SyncManager, entries: &[OutboxEntry]) {
    let confirmed: Vec<_> = entries
        .iter()
        .filter_map(|entry| match (&entry.destination, &entry.payload) {
            (Destination::Client(client_id), SyncPayload::RowBatchNeeded { row, .. }) => Some((
                *client_id,
                row.row_id,
                BranchName::new(row.branch.as_str()),
                row.batch_id,
            )),
            _ => None,
        })
        .collect();
    sm.confirm_client_deliveries(&confirmed);
}

/// Queue a row, throw the payload away, and expect the row to be offered again.
///
/// Discarding the outbox is exactly what the two drop paths do — the difference between
/// this and a delivered row is that nobody ever handed it to a connection.
#[test]
fn a_dropped_payload_is_offered_again() {
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

    let row = visible_row(row_id, "main", Vec::new(), 1_000, b"only-copy");

    sm.queue_row_to_client(client_id, row_id, row_metadata("users"), row.clone(), false);
    let queued = sm.take_outbox();
    assert_eq!(
        queued.len(),
        1,
        "the row in scope should have been queued once"
    );

    // The payload is dropped here — the client had no live connection, or its channel died
    // between the queue and the write. `queued` goes out of scope undelivered.
    drop(queued);

    // Any later forward of the same row takes this path: `force_resend` is false, because
    // nothing re-derived the query's scope.
    sm.queue_row_to_client(client_id, row_id, row_metadata("users"), row.clone(), false);

    let retried = sm.take_outbox();
    assert!(
        !retried.is_empty(),
        "the row was recorded as delivered although its payload was never handed to a \
         connection, so it will never be offered again — the receiver has lost it \
         permanently, and only a client reap clears the claim"
    );
}

/// The property a fix must not break: a row that WAS delivered is not sent twice.
///
/// Stated separately because the cheapest wrong fix — never record the claim — passes the
/// test above and turns every settle into a resend storm.
#[test]
fn a_delivered_payload_is_not_offered_twice() {
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

    let row = visible_row(row_id, "main", Vec::new(), 1_000, b"delivered");

    sm.queue_row_to_client(client_id, row_id, row_metadata("users"), row.clone(), false);
    let delivered = sm.take_outbox();
    assert_eq!(delivered.len(), 1, "first queue should produce one payload");
    confirm_queued_entries(&mut sm, &delivered);

    sm.queue_row_to_client(client_id, row_id, row_metadata("users"), row, false);
    assert!(
        sm.take_outbox().is_empty(),
        "a row already delivered to this client was queued again"
    );
}

/// A row that is never confirmed must stop being re-offered.
///
/// Some rows can never be applied by the peer — a rejected fate, a decode failure, a bug on
/// its side. Nothing confirms them, so nothing clears them, and the peer stays marked as
/// owed rows forever: every subscription registration re-derives its scope and re-sends.
/// That is a retransmission livelock, and it is worse than losing the row, because it never
/// stops. After a bounded number of attempts the sender gives up loudly and records the
/// claim, which is the same outcome as the old behaviour for that one row.
#[test]
fn a_row_that_is_never_confirmed_stops_being_re_offered() {
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

    let row = visible_row(row_id, "main", Vec::new(), 1_000, b"never-applies");

    // Offer it up to the cap, never confirming. Each attempt is what a re-offer does when
    // the peer is owed the row.
    for attempt in 1..=MAX_REDELIVERY_ATTEMPTS {
        sm.queue_row_to_client(client_id, row_id, row_metadata("users"), row.clone(), true);
        let _discarded = sm.take_outbox();
        assert!(
            sm.client_has_undelivered_payloads(client_id),
            "the sender stopped waiting after only {attempt} attempts"
        );
    }

    // One more, and it must give up rather than keep the peer marked forever.
    sm.queue_row_to_client(client_id, row_id, row_metadata("users"), row, true);
    let _discarded = sm.take_outbox();

    assert!(
        !sm.client_has_undelivered_payloads(client_id),
        "a row nothing can confirm is still recorded as owed past {MAX_REDELIVERY_ATTEMPTS} \
         attempts, so every later subscription re-derives and re-sends it — a livelock that \
         never ends and is worse than the loss it replaced"
    );
}

/// Reaping a client takes its outstanding rows with it.
///
/// A peer that stays away long enough is reaped, and its whole server-side state goes: the
/// next connection rebuilds it from scratch and re-derives everything in scope, which
/// delivers what it missed by a different route. What must not survive is the bookkeeping —
/// an entry left behind would keep a client that no longer exists counted as owed rows, and
/// nothing would ever clear it.
#[test]
fn reaping_a_client_clears_what_it_was_owed() {
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

    let row = visible_row(row_id, "main", Vec::new(), 1_000, b"owed-at-reap");
    sm.queue_row_to_client(client_id, row_id, row_metadata("users"), row, false);
    let _dropped = sm.take_outbox();
    assert!(
        sm.client_has_undelivered_payloads(client_id),
        "precondition: the client should be owed the row whose payload was dropped"
    );

    assert!(sm.remove_client(client_id), "the client should reap");

    assert!(
        !sm.client_has_undelivered_payloads(client_id),
        "a reaped client is still recorded as owed rows — the entry outlives the client it \
         belongs to, and nothing will ever confirm or clear it"
    );
}
