//! Live-subscription delivery under remote ingest and server restarts.
//!
//! Field failure this pins down (2026-08-01, linsa): the server storage held
//! rows (verified with direct backend queries), while long-lived CLIENT
//! subscriptions for exactly those rows delivered nothing — and the outage
//! began around ungraceful server restarts. The incremental IndexScanNode
//! path was only ever exercised by tests that mutate through the LOCAL write
//! API; these tests drive the two uncovered axes:
//!
//!   1. rows arriving via ANOTHER client over the sync protocol (remote
//!      ingest), observed through a live `JazzClient::subscribe` stream, and
//!   2. a server restart over a persisted RocksDB data dir, after which both
//!      the pre-restart subscription and a fresh one must see new rows.
//!
//! Debug builds arm the incremental-scan parity assertions, so a scan that
//! diverges from a full rescan fails loudly here rather than silently
//! serving stale sets.

#![cfg(feature = "test")]

mod support;

use std::collections::BTreeSet;
use std::time::Duration;

use futures::StreamExt as _;
use jazz_tools::object::ObjectId;
use jazz_tools::row_input;
use jazz_tools::server::{JazzServer, TestJwtIssuer};
use jazz_tools::sync_manager::DurabilityTier;
use jazz_tools::{
    AppContext, ClientStorage, ColumnType, JazzClient, QueryBuilder, SchemaBuilder, TableSchema,
    Value,
};
use support::{publish_allow_all_permissions, push_catalogue_in_memory, wait_for_query};
use tempfile::TempDir;

const READY_TIMEOUT: Duration = Duration::from_secs(30);
const DELIVERY_DEADLINE: Duration = Duration::from_secs(20);

fn parts_schema() -> jazz_tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("parts")
                .column("file_id", ColumnType::Text)
                .column("idx", ColumnType::Integer),
        )
        .build()
}

async fn make_client(server: &JazzServer, user_id: &str) -> JazzClient {
    let schema = parts_schema();
    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
        "main",
        std::slice::from_ref(&schema),
        &[],
    )
    .await
    .expect("push schema catalogue");

    let context = AppContext {
        app_id: server.app_id(),
        client_id: None,
        schema: schema.clone(),
        server_url: server.base_url(),
        data_dir: TempDir::new().expect("temp client dir").keep(),
        storage: ClientStorage::Memory,
        jwt_token: Some(TestJwtIssuer::jwt_for_user(user_id)),
        backend_secret: None,
        admin_secret: None,
        sync_tracer: None,
    };
    let client = JazzClient::connect(context).await.expect("connect client");

    publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &schema,
    )
    .await;
    wait_for_query(
        &client,
        QueryBuilder::new("parts").build(),
        Some(DurabilityTier::EdgeServer),
        READY_TIMEOUT,
        format!("EdgeServer readiness for {user_id}"),
        |_| Some(()),
    )
    .await;
    client
}

fn matching_query() -> jazz_tools::query_manager::query::Query {
    QueryBuilder::new("parts")
        .filter_eq("file_id", Value::Text("file-under-test".into()))
        .build()
}

async fn insert_part(client: &JazzClient, file_id: &str, idx: i32) -> ObjectId {
    let (id, _, batch_id) = client
        .insert(
            "parts",
            row_input!("file_id" => file_id, "idx" => idx),
        )
        .expect("insert part");
    client
        .wait_for_batch(batch_id, DurabilityTier::EdgeServer)
        .await
        .expect("part reaches the server");
    id
}

/// Drain `stream` until every id in `expected` was seen as ADDED, or panic at
/// the deadline listing what never arrived. Rows outside `allowed` fail fast —
/// a filtered subscription must not leak non-matching rows.
async fn expect_added(
    stream: &mut jazz_tools::SubscriptionStream,
    expected: &BTreeSet<ObjectId>,
    allowed: &BTreeSet<ObjectId>,
    what: &str,
) {
    let mut seen: BTreeSet<ObjectId> = BTreeSet::new();
    let deadline = tokio::time::Instant::now() + DELIVERY_DEADLINE;
    while !expected.is_subset(&seen) {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            let missing: Vec<_> = expected.difference(&seen).collect();
            panic!("{what}: live subscription never delivered {missing:?} (saw {seen:?})");
        }
        let delta = tokio::time::timeout(remaining, stream.next())
            .await
            .unwrap_or_else(|_| {
                let missing: Vec<_> = expected.difference(&seen).collect();
                panic!("{what}: timed out waiting for {missing:?} (saw {seen:?})")
            })
            .unwrap_or_else(|| panic!("{what}: subscription stream closed early"));
        for added in &delta.added {
            assert!(
                allowed.contains(&added.id),
                "{what}: filtered subscription leaked unexpected row {:?}",
                added.id,
            );
            seen.insert(added.id);
        }
    }
}

/// Both scenarios run sequentially in one test so only a single RocksDB server
/// lives at a time (same fd-exhaustion discipline as rocksdb_storage_integration).
#[tokio::test]
async fn live_subscription_sync() {
    let jwks = TestJwtIssuer::start().await;
    let data_dir = TempDir::new().expect("server data dir");

    // ── server₁ ────────────────────────────────────────────────────────────
    let server1 = JazzServer::builder()
        .with_rocksdb_storage()
        .with_data_dir(data_dir.path())
        .with_jwks_url(jwks.endpoint())
        .start()
        .await;
    let port = server1.port();

    let writer = make_client(&server1, "writer").await;
    let reader = make_client(&server1, "reader").await;

    // Live subscription registered BEFORE any matching rows exist.
    let mut sub = reader
        .subscribe(matching_query())
        .await
        .expect("subscribe reader");

    // Scenario 1: rows ingested from ANOTHER client must reach the live stream.
    let mut matching: BTreeSet<ObjectId> = BTreeSet::new();
    for idx in 0..4 {
        matching.insert(insert_part(&writer, "file-under-test", idx).await);
        // Interleave non-matching writes so the scan's row-precise marks see
        // mixed traffic on the table, not a clean single-file burst.
        insert_part(&writer, "other-file", idx).await;
    }
    expect_added(&mut sub, &matching, &matching, "scenario 1 (remote ingest)").await;

    // ── restart over the same data dir ─────────────────────────────────────
    server1.shutdown().await;
    let server2 = JazzServer::builder()
        .with_rocksdb_storage()
        .with_data_dir(data_dir.path())
        .with_port(port)
        .with_jwks_url(jwks.endpoint())
        .start()
        .await;

    // Both clients reconnect to the same URL. Readiness = their queries
    // answer at the EdgeServer tier again.
    for (client, name) in [(&writer, "writer"), (&reader, "reader")] {
        wait_for_query(
            client,
            QueryBuilder::new("parts").build(),
            Some(DurabilityTier::EdgeServer),
            READY_TIMEOUT,
            format!("{name} reconnects after restart"),
            |_| Some(()),
        )
        .await;
    }

    // Scenario 2: the PRE-RESTART live subscription must deliver rows written
    // after the restart. This is the shape of the field failure: storage had
    // the rows, long-lived subscriptions never learned about them.
    let mut post_restart: BTreeSet<ObjectId> = BTreeSet::new();
    for idx in 10..13 {
        post_restart.insert(insert_part(&writer, "file-under-test", idx).await);
    }
    let mut allowed = matching.clone();
    allowed.extend(&post_restart);
    expect_added(
        &mut sub,
        &post_restart,
        &allowed,
        "scenario 2 (pre-restart subscription, post-restart rows)",
    )
    .await;

    // Scenario 3: a FRESH subscription over the restarted store sees the whole
    // set — pre-restart rows restored from disk plus post-restart rows.
    let mut fresh = reader
        .subscribe(matching_query())
        .await
        .expect("fresh subscribe post-restart");
    expect_added(
        &mut fresh,
        &allowed,
        &allowed,
        "scenario 3 (fresh subscription over restarted store)",
    )
    .await;

    writer.shutdown().await.expect("shutdown writer");
    reader.shutdown().await.expect("shutdown reader");
    server2.shutdown().await;
}
