//! Row-content sharing across subscribers.
//!
//! The server retains ~85% of store size PER SUBSCRIBER (see
//! `server_fleet_load.rs`): every subscription's row loads mint a private
//! `Arc<[u8]>` for the same underlying bytes, and the server-side
//! authorization loader even copies content byte-for-byte in the identity
//! case (`transform_content_to_authorization_schema` → `content.to_vec()`).
//!
//! These tests pin the fix: identical row content must be SHARED between
//! subscriber graphs (one allocation per (row, batch), refcounted), while
//! visibility, versioning, and lifecycle semantics stay untouched:
//!
//! - sharing: N parked sessions cost ~1× payload total, not N×
//! - version isolation: an update reaches every live subscriber (no stale
//!   cache hits) and replaces, not duplicates, the shared bytes
//! - policy isolation: a warm cache never leaks rows to a session whose
//!   policies deny them
//! - lifecycle: reaping every session releases the shared bytes too

#![cfg(feature = "test")]

mod support;

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use jazz_tools::object::ObjectId;
use jazz_tools::query_manager::policy::Operation;
use jazz_tools::query_manager::types::{permissions, policy_expr as pe};
use jazz_tools::row_input;
use jazz_tools::server::JazzServer;
use jazz_tools::sync_manager::DurabilityTier;
use jazz_tools::{ColumnType, QueryBuilder, SchemaBuilder, TableSchema, Value};
use support::{connect_ready_client, connect_ready_user, wait_for_query};

// ── tracking allocator (live bytes) ─────────────────────────────────────────

struct TrackingAllocator;

static LIVE_BYTES: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            LIVE_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        LIVE_BYTES.fetch_sub(layout.size() as u64, Ordering::Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            LIVE_BYTES.fetch_sub(layout.size() as u64, Ordering::Relaxed);
            LIVE_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        }
        new_ptr
    }
}

#[global_allocator]
static GLOBAL: TrackingAllocator = TrackingAllocator;

fn live_now() -> u64 {
    LIVE_BYTES.load(Ordering::Relaxed)
}

const MIB: u64 = 1 << 20;

// ── fixture ─────────────────────────────────────────────────────────────────

const CHATS: usize = 8;
const FILE_PARTS_PER_CHAT: usize = 4;
const FILE_PART_BYTES: usize = 64 * 1024;
/// Blob payload: 32 × 64 KiB = 2 MiB.
const PAYLOAD_BYTES: u64 = (CHATS * FILE_PARTS_PER_CHAT * FILE_PART_BYTES) as u64;

const READERS: usize = 6;

fn media_schema() -> jazz_tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("chats")
                .column("name", ColumnType::Text)
                .column(
                    "members",
                    ColumnType::Array {
                        element: Box::new(ColumnType::Text),
                    },
                )
                .policies(permissions(|p| {
                    p.allow_insert().always();
                    p.allow_update().always();
                    p.allow_read()
                        .where_(pe::contains("members", pe::session("user_id")));
                })),
        )
        .table(
            TableSchema::builder("file_parts")
                .nullable_fk_column("chat_id", "chats")
                .column("part_index", ColumnType::Integer)
                .column("data", ColumnType::Bytea)
                .policies(permissions(|p| {
                    p.allow_insert().always();
                    p.allow_update().always();
                    p.allow_read().where_(pe::all_of([
                        pe::is_not_null("chat_id"),
                        pe::allowed_to(Operation::Select, "chat_id"),
                    ]));
                })),
        )
        .build()
}

struct Seeded {
    server: JazzServer,
    schema: jazz_tools::Schema,
    writer: jazz_tools::JazzClient,
    chat_ids: Vec<ObjectId>,
    part_ids: Vec<ObjectId>,
}

async fn seed(members: &[&str]) -> Seeded {
    let schema = media_schema();
    // RocksDB on purpose: the production server runs RocksDB, where every
    // row load deserializes a PRIVATE copy of the bytes. The in-memory
    // backend Arc-shares loads with the store itself and would mask the
    // per-subscription duplication this suite pins.
    let server = JazzServer::builder()
        .with_schema(schema.clone())
        .with_rocksdb_storage()
        .start()
        .await;
    let writer =
        connect_ready_client(&server, &schema, "writer", "chats", Duration::from_secs(30)).await;

    let member_values: Vec<Value> = members
        .iter()
        .map(|m| (*m).into())
        .chain(std::iter::once("writer".into()))
        .collect();
    let mut chat_ids = Vec::new();
    let mut part_ids = Vec::new();
    let mut last_batch = None;
    for chat_index in 0..CHATS {
        let (chat_id, _, batch) = writer
            .insert(
                "chats",
                row_input!(
                    "name" => format!("chat {chat_index}"),
                    "members" => Value::Array(member_values.clone()),
                ),
            )
            .expect("insert chat");
        last_batch = Some(batch);
        for part_index in 0..FILE_PARTS_PER_CHAT {
            let payload = vec![(part_index % 251) as u8; FILE_PART_BYTES];
            let (part_id, _, batch) = writer
                .insert(
                    "file_parts",
                    row_input!(
                        "chat_id" => Value::Uuid(chat_id),
                        "part_index" => part_index as i32,
                        "data" => Value::Bytea(payload),
                    ),
                )
                .expect("insert part");
            last_batch = Some(batch);
            part_ids.push(part_id);
        }
        chat_ids.push(chat_id);
    }
    writer
        .wait_for_batch(last_batch.expect("seeded"), DurabilityTier::EdgeServer)
        .await
        .expect("seed reaches the server");

    Seeded {
        server,
        schema,
        writer,
        chat_ids,
        part_ids,
    }
}

async fn drain_parts(
    reader: &jazz_tools::JazzClient,
    expected_parts: usize,
    label: &str,
) -> jazz_tools::SubscriptionStream {
    let mut sub = reader
        .subscribe(QueryBuilder::new("file_parts").build())
        .await
        .expect("subscribe parts");
    let mut seen = 0usize;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while seen < expected_parts {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "{label}: only {seen}/{expected_parts} parts delivered"
        );
        tokio::select! {
            delta = sub.next() => {
                seen += delta.expect("parts stream").added.len();
            }
            _ = tokio::time::sleep(remaining) => {}
        }
    }
    sub
}

// ── 1. sharing: N parked sessions must cost ~1× payload, not N× ────────────

/// Budget for the server-retained bytes of N parked sessions together.
///
/// Without sharing each parked session privately retains ~0.8× payload
/// (measured in `server_fleet_load.rs`), so N=6 sessions ≈ 4.8× payload —
/// far over this budget. With shared row content the fleet retains ~1×
/// payload (single Arc per row) plus per-session bookkeeping.
const PARKED_FLEET_BUDGET: u64 = (PAYLOAD_BYTES * 2) + 4 * MIB;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parked_sessions_share_row_content() {
    let user_names: Vec<String> = (0..READERS).map(|i| format!("user{i:03}")).collect();
    let member_refs: Vec<&str> = user_names.iter().map(String::as_str).collect();
    let fixture = seed(&member_refs).await;
    let expected_parts = CHATS * FILE_PARTS_PER_CHAT;

    let baseline = live_now();

    let mut fleet = Vec::new();
    for user in &user_names {
        let reader = connect_ready_user(
            &fixture.server,
            &fixture.schema,
            user,
            "chats",
            Duration::from_secs(30),
        )
        .await;
        let sub = drain_parts(&reader, expected_parts, user).await;
        fleet.push((reader, sub));
    }

    // Abrupt drop: client-side memory frees, server keeps the parked
    // sessions until the sweep — this is the server-attributable cost.
    drop(fleet);
    tokio::time::sleep(Duration::from_secs(2)).await;

    let parked = live_now().saturating_sub(baseline);
    eprintln!(
        "{READERS} parked sessions retain {} KiB total (payload {} KiB, budget {} KiB)",
        parked / 1024,
        PAYLOAD_BYTES / 1024,
        PARKED_FLEET_BUDGET / 1024,
    );
    assert!(
        parked < PARKED_FLEET_BUDGET,
        "{READERS} parked sessions retain {} KiB — row content is being \
         copied per subscriber instead of shared (payload {} KiB, budget {} KiB)",
        parked / 1024,
        PAYLOAD_BYTES / 1024,
        PARKED_FLEET_BUDGET / 1024,
    );

    // Lifecycle: reaping the fleet must release the shared bytes too.
    fixture.server.set_client_ttl(Duration::ZERO).await;
    let reaped = fixture.server.run_sweep_once().await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        reaped.len() >= READERS - 1,
        "sweep reaped only {} of {READERS}",
        reaped.len()
    );
    let residual = live_now().saturating_sub(baseline);
    assert!(
        residual < PAYLOAD_BYTES / 2 + 2 * MIB,
        "after reaping every session the server still holds {} KiB — shared \
         row content is not released with its last subscriber",
        residual / 1024,
    );

    fixture.writer.shutdown().await.expect("shutdown writer");
    fixture.server.shutdown().await;
}

// ── 1b. LIVE subscriptions must share row content ──────────────────────────
//
// Parked sessions turned out to hold only per-row bookkeeping (measured
// above); the VALUE duplication lives in ACTIVE subscription graphs — every
// live subscription over the same rows re-loads and privately retains the
// same bytes, on the server and in the client runtime alike. Eight live
// subscriptions over a 2 MiB blob set must not cost ~16×2 MiB.

/// The first (warm-up) batch of extra subscriptions also fills bounded
/// shared caches (RocksDB block cache); the MEASURED batch afterwards must
/// cost per-subscription bookkeeping only. Without sharing each measured
/// subscription retains ~2× payload (client graph + server graph) ≈ 16 MiB
/// for the batch; with a shared row-content cache it is bookkeeping only.
const WARM_SUBSCRIPTIONS: usize = 4;
const MEASURED_SUBSCRIPTIONS: usize = 4;
const MEASURED_SUBS_BUDGET: u64 = 2 * PAYLOAD_BYTES + 2 * MIB;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_subscriptions_share_row_content() {
    let fixture = seed(&["alice"]).await;
    let expected_parts = CHATS * FILE_PARTS_PER_CHAT;

    let alice = connect_ready_user(
        &fixture.server,
        &fixture.schema,
        "alice",
        "chats",
        Duration::from_secs(30),
    )
    .await;

    // First subscription: pays for the store copy + one graph on each side.
    let first = drain_parts(&alice, expected_parts, "first sub").await;

    // Warm-up batch: fills per-process shared caches (RocksDB block cache)
    // so the measured batch below sees only per-subscription costs.
    let mut warm = Vec::new();
    for i in 0..WARM_SUBSCRIPTIONS {
        warm.push(drain_parts(&alice, expected_parts, &format!("warm sub {i}")).await);
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    let after_warm = live_now();

    // Measured batch: identical subscriptions over warm caches — must share
    // the row content and cost bookkeeping only.
    let mut measured = Vec::new();
    for i in 0..MEASURED_SUBSCRIPTIONS {
        measured.push(drain_parts(&alice, expected_parts, &format!("measured sub {i}")).await);
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    let after_measured = live_now();

    let marginal = after_measured.saturating_sub(after_warm);
    eprintln!(
        "{MEASURED_SUBSCRIPTIONS} measured live subscriptions cost +{} KiB \
         (payload {} KiB, budget {} KiB)",
        marginal / 1024,
        PAYLOAD_BYTES / 1024,
        MEASURED_SUBS_BUDGET / 1024,
    );
    assert!(
        marginal < MEASURED_SUBS_BUDGET,
        "{MEASURED_SUBSCRIPTIONS} live subscriptions over the same warm rows \
         cost +{} KiB — row content is retained per subscription instead of \
         shared (payload {} KiB, budget {} KiB)",
        marginal / 1024,
        PAYLOAD_BYTES / 1024,
        MEASURED_SUBS_BUDGET / 1024,
    );

    drop(measured);
    drop(warm);
    drop(first);
    alice.shutdown().await.expect("shutdown alice");
    fixture.writer.shutdown().await.expect("shutdown writer");
    fixture.server.shutdown().await;
}

// ── 2. version isolation: updates reach every subscriber, no stale hits ────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn updates_reach_all_subscribers_with_shared_content() {
    let fixture = seed(&["alice", "bob"]).await;
    let expected_parts = CHATS * FILE_PARTS_PER_CHAT;

    let alice = connect_ready_user(
        &fixture.server,
        &fixture.schema,
        "alice",
        "chats",
        Duration::from_secs(30),
    )
    .await;
    let bob = connect_ready_user(
        &fixture.server,
        &fixture.schema,
        "bob",
        "chats",
        Duration::from_secs(30),
    )
    .await;
    let mut alice_sub = drain_parts(&alice, expected_parts, "alice").await;
    let mut bob_sub = drain_parts(&bob, expected_parts, "bob").await;

    // Update one part's payload; both live subscribers must observe the NEW
    // bytes — a stale shared-cache hit would deliver the old content.
    let target = fixture.part_ids[0];
    let new_payload = vec![0xEEu8; FILE_PART_BYTES];
    fixture
        .writer
        .update(
            target,
            vec![("data".to_string(), Value::Bytea(new_payload.clone()))],
        )
        .expect("update part");

    // Live delta must mention the target row for both subscribers…
    for (name, sub) in [("alice", &mut alice_sub), ("bob", &mut bob_sub)] {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let mut delivered = false;
        while !delivered {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            assert!(
                !remaining.is_zero(),
                "{name}: never received a delta for the updated part"
            );
            tokio::select! {
                delta = sub.next() => {
                    let delta = delta.expect("stream");
                    delivered = delta.updated.iter().map(|u| u.id)
                        .chain(delta.added.iter().map(|a| a.id))
                        .any(|id| id == target);
                }
                _ = tokio::time::sleep(remaining) => {}
            }
        }
    }

    // …and the decoded row must carry the NEW bytes for both sessions.
    for (name, client) in [("alice", &alice), ("bob", &bob)] {
        wait_for_query(
            client,
            QueryBuilder::new("file_parts").build(),
            Some(DurabilityTier::EdgeServer),
            Duration::from_secs(30),
            format!("{name} sees updated part bytes"),
            |rows| {
                rows.iter()
                    .any(|(id, values)| {
                        *id == target
                            && values.iter().any(|value| {
                                matches!(value, Value::Bytea(bytes) if bytes == &new_payload)
                            })
                    })
                    .then_some(())
            },
        )
        .await;
    }

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    fixture.writer.shutdown().await.expect("shutdown writer");
    fixture.server.shutdown().await;
}

// ── 3. policy isolation: warm cache never leaks denied rows ────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn warm_cache_does_not_leak_to_denied_session() {
    let fixture = seed(&["alice"]).await; // bob is NOT a member
    let expected_parts = CHATS * FILE_PARTS_PER_CHAT;

    // Warm every row through alice.
    let alice = connect_ready_user(
        &fixture.server,
        &fixture.schema,
        "alice",
        "chats",
        Duration::from_secs(30),
    )
    .await;
    let _alice_sub = drain_parts(&alice, expected_parts, "alice").await;

    // bob subscribes to the same table with the cache fully warm: policies
    // must still deny every row.
    let bob = connect_ready_user(
        &fixture.server,
        &fixture.schema,
        "bob",
        "chats",
        Duration::from_secs(30),
    )
    .await;
    let rows = wait_for_query(
        &bob,
        QueryBuilder::new("file_parts").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(15),
        "bob parts query settles",
        Some,
    )
    .await;
    assert!(
        rows.is_empty(),
        "bob is not a member of any chat but received {} file_parts rows — \
         shared row content must never bypass policy filtering",
        rows.len(),
    );

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    fixture.writer.shutdown().await.expect("shutdown writer");
    fixture.server.shutdown().await;
}

// ── 4. one-shot queries agree with subscriptions under sharing ─────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_shot_query_sees_shared_content_correctly() {
    let fixture = seed(&["alice", "bob"]).await;
    let expected_parts = CHATS * FILE_PARTS_PER_CHAT;

    let alice = connect_ready_user(
        &fixture.server,
        &fixture.schema,
        "alice",
        "chats",
        Duration::from_secs(30),
    )
    .await;
    let _alice_sub = drain_parts(&alice, expected_parts, "alice").await;

    // A second session running one-shot queries over warm rows must see the
    // same count and intact bytes.
    let bob = connect_ready_user(
        &fixture.server,
        &fixture.schema,
        "bob",
        "chats",
        Duration::from_secs(30),
    )
    .await;
    let rows = wait_for_query(
        &bob,
        QueryBuilder::new("file_parts").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(30),
        "bob sees all parts",
        |rows| (rows.len() == expected_parts).then_some(rows),
    )
    .await;
    let expected_ids: BTreeSet<ObjectId> = fixture.part_ids.iter().copied().collect();
    let got_ids: BTreeSet<ObjectId> = rows.iter().map(|(id, _)| *id).collect();
    assert_eq!(got_ids, expected_ids, "one-shot result set mismatch");
    for (id, values) in &rows {
        let blob_ok = values
            .iter()
            .any(|value| matches!(value, Value::Bytea(bytes) if bytes.len() == FILE_PART_BYTES));
        assert!(blob_ok, "row {id} delivered without intact blob bytes");
    }

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    fixture.writer.shutdown().await.expect("shutdown writer");
    fixture.server.shutdown().await;
}
