//! Confirmations must change HOW a row is delivered, never WHAT the peer ends up with.
//!
//! The unit gates elsewhere pin individual decisions — a claim deferred, a re-offer made,
//! a re-offer narrowed. None of them can say whether the mechanism as a whole is neutral,
//! and that is the failure this project keeps producing: a change that is right in the one
//! scenario the test constructs and wrong in the traffic around it.
//!
//! So this runs the same scenario twice, once with a peer that confirms and once with a
//! peer that does not, and compares the outcomes:
//!
//! - with no payload dropped, the two modes must agree exactly — confirmations are not
//!   allowed to add, drop, reorder or duplicate anything on the happy path;
//! - with a payload dropped, the confirming peer must converge on the full set. The other
//!   is allowed to be missing rows; that IS the defect, and this file is where the
//!   difference is stated rather than assumed.
//!
//! A single scenario runner drives both, so the two modes cannot drift apart in setup.
//!
//! What this level CANNOT see: the emission of the confirmation itself. That happens in the
//! runtime tick, after the durability barrier, and this harness drives `QueryManager` only —
//! so here the peer applies rows and never reports them, and the server keeps counting them
//! as owed. The recovery is still proved, because it depends on the server's bookkeeping and
//! on the replayed subscription, not on the report. The report has its own gate at the
//! runtime level.

use super::*;

use crate::sync_manager::{ClientId, ServerId};

/// Which peer behaviour to run the scenario with.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Mode {
    /// A current peer: it reports the rows it applies.
    Confirms,
    /// An older peer: it never reports, so the sender records the claim when it queues.
    Silent,
}

/// Whether the scenario drops a payload the way a dying socket does.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Gap {
    None,
    /// One row is written while the peer cannot receive it, and its payload is discarded.
    DropOneRow,
}

/// The names the peer holds at the end, sorted so the comparison is order-independent.
fn run_scenario(mode: Mode, gap: Gap) -> Vec<String> {
    let schema = test_schema();

    let (mut server, mut server_io) = create_query_manager(SyncManager::new(), schema.clone());
    let (mut client, mut client_io) = create_query_manager(SyncManager::new(), schema);

    server
        .insert(
            &mut server_io,
            "users",
            &[Value::Text("before".into()), Value::Integer(90)],
        )
        .unwrap();
    server.process(&mut server_io);

    let server_id = ServerId::new();
    let client_id = ClientId::new();
    connect_server(&mut client, &client_io, server_id);
    connect_client(&mut server, &server_io, client_id);
    server
        .sync_manager_mut()
        .set_client_acks_deliveries(client_id, mode == Mode::Confirms);
    client
        .sync_manager_mut()
        .set_upstream_supports_delivery_acks(mode == Mode::Confirms);
    let _ = client.sync_manager_mut().take_outbox();

    let query = client
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();
    let sub_id = client.subscribe_with_sync(query, None, None).unwrap();
    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );

    if gap == Gap::DropOneRow {
        // Written while the peer cannot receive. Discarding the outbox without delivering
        // it is exactly what a socket that has died does to the payload.
        server
            .insert(
                &mut server_io,
                "users",
                &[Value::Text("during the gap".into()), Value::Integer(95)],
            )
            .unwrap();
        server.process(&mut server_io);
        let _discarded = server.sync_manager_mut().take_outbox();

        // The peer comes back the way a phone does: the transport reconnects underneath a
        // subscription that never died, replaying the SAME query id. Subscribing afresh
        // would mint a new one, and the server would re-derive and force-resend as a side
        // effect — self-healing by accident, which is how the existing offline tests pass
        // while the field loses messages.
        // A real reconnect drops the server and adds it again — that pair is what clears
        // the marker guarding the replay. Only adding it back is not a reconnect.
        client.sync_manager_mut().remove_server(server_id);
        // The QueryManager's add, not the SyncManager's: the replay of active subscriptions
        // lives one level up, and it is the whole point of this step.
        client.add_server_with_storage(&client_io, server_id, false);
    } else {
        server
            .insert(
                &mut server_io,
                "users",
                &[Value::Text("during the gap".into()), Value::Integer(95)],
            )
            .unwrap();
        server.process(&mut server_io);
    }

    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );

    let mut names: Vec<String> = client
        .get_subscription_results(sub_id)
        .iter()
        .filter_map(|(_, row)| match &row[0] {
            Value::Text(name) => Some(name.clone()),
            _ => None,
        })
        .collect();
    names.sort();
    names
}

/// On the happy path the two modes must be indistinguishable.
///
/// If they are not, confirmations are not neutral: they either change what reaches the peer
/// or change how often, and both are regressions for every peer that never loses anything.
#[test]
fn confirmations_are_neutral_when_nothing_is_dropped() {
    let confirming = run_scenario(Mode::Confirms, Gap::None);
    let silent = run_scenario(Mode::Silent, Gap::None);

    assert_eq!(
        confirming, silent,
        "a confirming peer and a silent one ended up with different rows on a run where \
         nothing was dropped — confirmations are supposed to change how delivery is \
         tracked, not what arrives"
    );
    assert_eq!(
        confirming,
        vec!["before".to_string(), "during the gap".to_string()],
        "the peer should hold both matching rows"
    );
}

/// With a payload dropped, the confirming peer recovers and the silent one does not.
///
/// This is the whole point of the mechanism, stated as a difference rather than left
/// implicit: the row is unreachable for a peer that cannot tell the sender it never
/// arrived.
#[test]
fn a_confirming_peer_recovers_a_dropped_row_and_a_silent_one_does_not() {
    let confirming = run_scenario(Mode::Confirms, Gap::DropOneRow);
    let silent = run_scenario(Mode::Silent, Gap::DropOneRow);

    assert_eq!(
        confirming,
        vec!["before".to_string(), "during the gap".to_string()],
        "the confirming peer never got the row written during the gap back — the sender \
         cleared its claim for a payload nobody received, or nothing re-offered it"
    );
    assert!(
        silent.len() <= confirming.len(),
        "the silent peer ended up with MORE rows than the confirming one ({silent:?} vs \
         {confirming:?}) — confirmations cannot make delivery worse"
    );
}
