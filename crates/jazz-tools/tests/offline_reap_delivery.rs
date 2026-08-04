//! Updates written while a peer was offline AND reaped must still reach it on reconnect.
//!
//! The offline case on its own is covered (`history_conflict.rs`,
//! `offline_reconnect_replays_local_edit_after_rejoin`) and passes. What no test covered is
//! the same window with a REAP in the middle — the server dropping the peer's client state
//! after its TTL expires, which is what happens to a phone that is closed for longer than
//! `--client-ttl-secs`.
//!
//! Reported from the field: with the TTL at 24 h everything arrives; with it at the
//! upstream default of 300 s, messages sent while the phone was closed never appear, and
//! reopening the app does not heal it.
//!
//! THIS TEST PASSES. It is a gate, not a reproduction — the field failure is NOT explained
//! by reap-then-reconnect on its own, and saying otherwise would be inventing a cause. Two
//! earlier shapes were tried and both passed as well: the same sequence asserted through a
//! one-shot `query()` (which fetches on the spot and would mask a delivery bug — that
//! version was vacuous), and against the real server with the production schema, where the
//! row reached the peer's local store both with and without a reap. What the model here
//! does NOT carry: row-level policies, and a client that was suspended by the OS rather
//! than shut down. Start there.
//!
//! Why a reap could lose data at all: `QueryManager::remove_client` drops the peer's
//! `ClientState`, which carries the bookkeeping of what has already been sent to it. If the
//! state that replaces it on reconnect starts from the CURRENT frontier rather than from
//! what the peer actually holds, everything written during the gap is considered delivered
//! and is never sent. The peer's own store is not wrong — it simply never hears about it.

#![cfg(feature = "test")]

mod support;

use std::collections::{BTreeSet, HashMap};
use std::time::Duration;

use futures::StreamExt as _;
use jazz_tools::object::ObjectId;

use jazz_tools::server::JazzServer;
use jazz_tools::{
    ColumnType, DurabilityTier, JazzClient, QueryBuilder, SchemaBuilder, TableSchema, Value,
};
use support::{TestingClient, wait_for_query};

const READY_TIMEOUT: Duration = Duration::from_secs(30);
const QUERY_TIMEOUT: Duration = Duration::from_secs(25);

fn test_schema() -> jazz_tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("todos")
                .column("title", ColumnType::Text)
                .column("completed", ColumnType::Boolean),
        )
        .build()
}

fn todo(title: &str) -> HashMap<String, Value> {
    HashMap::from([
        ("title".to_string(), Value::Text(title.to_string())),
        ("completed".to_string(), Value::Boolean(false)),
    ])
}

/// Wait for a live subscription to deliver a set of rows, or say what never came.
///
/// The app renders from subscriptions, not from one-shot queries, and the two do not fail
/// together: a `query()` fetches from the server on the spot and would paper over a
/// delivery bug. The first version of this test polled `query()` and passed while the
/// field was broken.
async fn expect_delivered(
    stream: &mut jazz_tools::SubscriptionStream,
    expected: &BTreeSet<ObjectId>,
    what: &str,
) {
    let mut seen: BTreeSet<ObjectId> = BTreeSet::new();
    let deadline = tokio::time::Instant::now() + QUERY_TIMEOUT;
    while !expected.is_subset(&seen) {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            let missing: Vec<_> = expected.difference(&seen).collect();
            panic!("{what}: subscription never delivered {missing:?} (saw {seen:?})");
        }
        let delta = tokio::time::timeout(remaining, stream.next())
            .await
            .unwrap_or_else(|_| {
                let missing: Vec<_> = expected.difference(&seen).collect();
                panic!("{what}: timed out waiting for {missing:?} (saw {seen:?})")
            })
            .unwrap_or_else(|| panic!("{what}: subscription stream closed early"));
        for added in &delta.added {
            seen.insert(added.id);
        }
        for updated in &delta.updated {
            seen.insert(updated.id);
        }
    }
}

/// Drive the server's TTL sweep so `bob` is reaped while he is away.
async fn reap_offline_peer(server: &JazzServer) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while server.disconnect_candidate_count().await == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for the disconnect candidate to register",
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    server.set_client_ttl(Duration::from_millis(1)).await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    let reaped = server.run_sweep_once().await;
    assert!(!reaped.is_empty(), "the sweep should have reaped the peer");
}

#[tokio::test]
async fn a_reaped_peer_receives_what_was_written_while_it_was_away() {
    let schema = test_schema();
    let server = JazzServer::start_with_schema(schema.clone()).await;

    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("alice-reap-delivery")
        .ready_on("todos", READY_TIMEOUT)
        .connect()
        .await;

    // Persistent storage, because that is what makes this a RECONNECT rather than a fresh
    // peer: a brand-new store catches up from nothing and would pass whatever the server
    // does with its per-client bookkeeping.
    let (bob_ctx, bob) = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("bob-reap-delivery")
        .with_persistent_storage()
        .ready_on("todos", READY_TIMEOUT)
        .connect_with_context()
        .await;

    let (first_id, _, _) = alice
        .insert("todos", todo("before-offline"))
        .expect("insert");
    let query = QueryBuilder::new("todos").build();

    // The pipe works while both are connected — otherwise the assertion at the end would
    // be ambiguous between "delivery is broken" and "delivery never worked here".
    wait_for_query(
        &bob,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "bob sees the row written while he was online",
        |rows| (rows.len() == 1).then_some(()),
    )
    .await;

    bob.shutdown().await.expect("bob goes offline");
    reap_offline_peer(&server).await;

    // Everything below happens while bob is both offline and reaped.
    alice
        .update(
            first_id,
            vec![(
                "title".to_string(),
                Value::Text("edited-while-away".to_string()),
            )],
        )
        .expect("alice edits the existing row");
    let (second_id, _, _) = alice
        .insert("todos", todo("added-while-away"))
        .expect("alice adds a row");

    wait_for_query(
        &alice,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "alice's writes reached the server",
        |rows| (rows.len() == 2).then_some(()),
    )
    .await;

    // Bob comes back with the same identity and the same local store, and subscribes — a
    // phone reopening the chat. What must arrive is BOTH the row added while he was away
    // and the edit to the row he already had.
    let bob_back = JazzClient::connect(bob_ctx.clone())
        .await
        .expect("bob reconnects");
    let mut sub = bob_back
        .subscribe(query.clone())
        .await
        .expect("bob subscribes after reconnecting");

    let expected: BTreeSet<ObjectId> = [first_id, second_id].into_iter().collect();
    expect_delivered(&mut sub, &expected, "bob after a reap").await;

    bob_back.shutdown().await.ok();
    alice.shutdown().await.ok();
    server.shutdown().await;
}
