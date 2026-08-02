//! Fleet load: 100 concurrent clients against one sync server.
//!
//! Question under test (linsa, 2026-08-02): the server's per-client costs are
//! store-scale — a catch-up transiently materializes ~3× the subscription
//! payload, and every connected client retains a delivery frontier
//! (`sent_batch_ids`) plus compiled subscription graphs. Does a fleet of 100
//! peers subscribing at once walk the process into OOM territory?
//!
//! Clients here are in-process `JazzClient`s over real localhost WebSockets —
//! the same wire protocol a web (WASM) client speaks, so server-side costs are
//! representative. Client-side allocations share this process's allocator, so
//! phase accounting separates the two:
//!   - PEAK during the mass catch-up: server burst + client ingest combined
//!     (upper bound on the server-only figure)
//!   - PARKED delta after all 100 clients are abruptly dropped: client memory
//!     is freed at drop, so live-bytes over baseline ≈ server-retained state
//!     for 100 dead sessions (frontiers + graphs), i.e. the per-client
//!     server-side resident cost
//!   - RESIDUAL after a zero-TTL sweep: must return to ~baseline

#![cfg(feature = "test")]

mod support;

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures::StreamExt as _;
use jazz_tools::object::ObjectId;
use jazz_tools::query_manager::policy::Operation;
use jazz_tools::query_manager::types::{permissions, policy_expr as pe};
use jazz_tools::row_input;
use jazz_tools::server::JazzServer;
use jazz_tools::sync_manager::DurabilityTier;
use jazz_tools::{ColumnType, QueryBuilder, SchemaBuilder, TableSchema, Value};
use support::{connect_ready_client, connect_ready_user};

// ── tracking allocator: live bytes + high-water mark ───────────────────────

struct TrackingAllocator;

static LIVE_BYTES: AtomicU64 = AtomicU64::new(0);
static PEAK_BYTES: AtomicU64 = AtomicU64::new(0);

fn bump_live(size: u64) {
    let live = LIVE_BYTES.fetch_add(size, Ordering::Relaxed) + size;
    PEAK_BYTES.fetch_max(live, Ordering::Relaxed);
}

fn drop_live(size: u64) {
    LIVE_BYTES.fetch_sub(size, Ordering::Relaxed);
}

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            bump_live(layout.size() as u64);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        drop_live(layout.size() as u64);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            drop_live(layout.size() as u64);
            bump_live(new_size as u64);
        }
        new_ptr
    }
}

#[global_allocator]
static GLOBAL: TrackingAllocator = TrackingAllocator;

fn live_now() -> u64 {
    LIVE_BYTES.load(Ordering::Relaxed)
}

fn reset_peak() {
    PEAK_BYTES.store(live_now(), Ordering::Relaxed);
}

fn peak_now() -> u64 {
    PEAK_BYTES.load(Ordering::Relaxed)
}

const MIB: u64 = 1 << 20;

// ── fixture ─────────────────────────────────────────────────────────────────

/// Store sized so 100 clients fit in a test process: ~5 MiB of payload.
/// Per-client server costs scale with rows/bytes, so results extrapolate
/// linearly to the field store (129 MB ≈ 25× this fixture).
const CHATS: usize = 20;
const MESSAGES_PER_CHAT: usize = 20;
const FILE_PARTS_PER_CHAT: usize = 8;
const FILE_PART_BYTES: usize = 32 * 1024;

const FLEET: usize = 100;

fn linsa_like_schema() -> jazz_tools::Schema {
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
            TableSchema::builder("messages")
                .nullable_fk_column("chat_id", "chats")
                .column("body", ColumnType::Text)
                .column("created_at", ColumnType::Integer)
                .policies(permissions(|p| {
                    p.allow_insert().always();
                    p.allow_read().where_(pe::all_of([
                        pe::is_not_null("chat_id"),
                        pe::allowed_to(Operation::Select, "chat_id"),
                    ]));
                })),
        )
        .table(
            TableSchema::builder("file_parts")
                .nullable_fk_column("chat_id", "chats")
                .column("part_index", ColumnType::Integer)
                .column("data", ColumnType::Bytea)
                .policies(permissions(|p| {
                    p.allow_insert().always();
                    p.allow_read().where_(pe::all_of([
                        pe::is_not_null("chat_id"),
                        pe::allowed_to(Operation::Select, "chat_id"),
                    ]));
                })),
        )
        .build()
}

// ── budgets ─────────────────────────────────────────────────────────────────

/// Server-retained bytes per parked (dead) session, measured after all 100
/// clients are dropped. This is the number that decides how many peers a
/// server survives.
///
/// Measured 2026-08-02: ~4.35 MiB per client on a ~5 MiB store — the server
/// retains ~85% of the store size PER SUBSCRIBER (sent_batch_ids frontier +
/// subscription graph state). Extrapolated to the linsa field store (129 MB):
/// ~108 MB/client, i.e. a 3 GiB container dies at ~25-30 concurrent peers.
/// The budget sits just above today's measured value as a no-regression
/// gate; ratchet it down hard (target ≤256 KiB) when frontier compaction /
/// windowed delivery land (linsa-i#183).
const PARKED_PER_CLIENT_BUDGET_BYTES: u64 = 5 * MIB;

/// After a zero-TTL sweep of all 100 dead sessions the server must return
/// to near-baseline.
const RESIDUAL_AFTER_SWEEP_BUDGET_BYTES: u64 = 32 * MIB;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn fleet_of_100_clients_survives_and_releases() {
    let schema = linsa_like_schema();
    let server = JazzServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;

    let ready_timeout = Duration::from_secs(60);
    let writer = connect_ready_client(&server, &schema, "writer", "chats", ready_timeout).await;

    // Seed — every fleet member is a chat member so all rows are visible.
    let mut members: Vec<Value> = (0..FLEET).map(|i| format!("user{i:03}").into()).collect();
    members.push("writer".into());
    let mut chat_ids: BTreeSet<ObjectId> = BTreeSet::new();
    let mut last_batch = None;
    for chat_index in 0..CHATS {
        let (chat_id, _, batch) = writer
            .insert(
                "chats",
                row_input!(
                    "name" => format!("chat {chat_index}"),
                    "members" => Value::Array(members.clone()),
                ),
            )
            .expect("insert chat");
        last_batch = Some(batch);
        for message_index in 0..MESSAGES_PER_CHAT {
            let (_, _, batch) = writer
                .insert(
                    "messages",
                    row_input!(
                        "chat_id" => Value::Uuid(chat_id),
                        "body" => format!("message {message_index}"),
                        "created_at" => message_index as i32,
                    ),
                )
                .expect("insert message");
            last_batch = Some(batch);
        }
        for part_index in 0..FILE_PARTS_PER_CHAT {
            let payload = vec![(part_index % 251) as u8; FILE_PART_BYTES];
            let (_, _, batch) = writer
                .insert(
                    "file_parts",
                    row_input!(
                        "chat_id" => Value::Uuid(chat_id),
                        "part_index" => part_index as i32,
                        "data" => Value::Bytea(payload),
                    ),
                )
                .expect("insert file part");
            last_batch = Some(batch);
        }
        chat_ids.insert(chat_id);
    }
    writer
        .wait_for_batch(last_batch.expect("seeded"), DurabilityTier::EdgeServer)
        .await
        .expect("seed reaches the server");

    let baseline = live_now();
    reset_peak();
    eprintln!(
        "baseline (seeded, writer connected): {} MiB",
        baseline / MIB
    );

    // ── phase 1: 100 clients connect + subscribe + drain, all at once ──────
    let expected_parts = CHATS * FILE_PARTS_PER_CHAT;
    let mut tasks = Vec::new();
    for i in 0..FLEET {
        let server_ref = &server;
        let schema_ref = &schema;
        let chat_ids = chat_ids.clone();
        tasks.push(async move {
            let user = format!("user{i:03}");
            let reader =
                connect_ready_user(server_ref, schema_ref, &user, "chats", ready_timeout).await;

            let include_query = QueryBuilder::new("chats")
                .with_array("messages", |sub| {
                    sub.from("messages").correlate("chat_id", "chats.id")
                })
                .build();
            let mut chats_sub = reader.subscribe(include_query).await.expect("subscribe");
            let mut parts_sub = reader
                .subscribe(QueryBuilder::new("file_parts").build())
                .await
                .expect("subscribe parts");

            let mut seen: BTreeSet<ObjectId> = BTreeSet::new();
            let mut seen_parts = 0usize;
            let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
            while !chat_ids.is_subset(&seen) || seen_parts < expected_parts {
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                assert!(
                    !remaining.is_zero(),
                    "{user}: catch-up incomplete (chats {}/{}, parts {seen_parts}/{expected_parts})",
                    seen.len(),
                    chat_ids.len(),
                );
                tokio::select! {
                    delta = chats_sub.next() => {
                        for added in &delta.expect("chats stream").added {
                            seen.insert(added.id);
                        }
                    }
                    delta = parts_sub.next() => {
                        seen_parts += delta.expect("parts stream").added.len();
                    }
                    _ = tokio::time::sleep(remaining) => {}
                }
            }
            (reader, chats_sub, parts_sub)
        });
    }
    let fleet = futures::future::join_all(tasks).await;

    let live_all_connected = live_now();
    let peak_catchup = peak_now();
    eprintln!(
        "all {FLEET} clients caught up: live +{} MiB over baseline, peak +{} MiB \
         (client ingest + server, combined)",
        (live_all_connected.saturating_sub(baseline)) / MIB,
        (peak_catchup.saturating_sub(baseline)) / MIB,
    );

    // ── phase 2: mass abrupt disconnect — clients free, server parks ───────
    drop(fleet);
    tokio::time::sleep(Duration::from_secs(2)).await;

    let live_parked = live_now();
    let parked_delta = live_parked.saturating_sub(baseline);
    let parked_per_client = parked_delta / FLEET as u64;
    eprintln!(
        "after dropping all {FLEET} clients: +{} MiB over baseline \
         (~{} KiB per parked session, server-side)",
        parked_delta / MIB,
        parked_delta / FLEET as u64 / 1024,
    );

    // ── phase 3: sweep must release everything ─────────────────────────────
    server.set_client_ttl(Duration::ZERO).await;
    let reaped = server.run_sweep_once().await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let live_swept = live_now();
    let residual = live_swept.saturating_sub(baseline);
    eprintln!(
        "sweep reaped {} clients; residual over baseline: {} MiB",
        reaped.len(),
        residual / MIB,
    );

    assert!(
        reaped.len() >= FLEET - 1,
        "sweep reaped only {} of {FLEET} dead sessions",
        reaped.len(),
    );
    assert!(
        parked_per_client < PARKED_PER_CLIENT_BUDGET_BYTES,
        "server retains {} KiB per parked session ({} MiB for the fleet) — \
         frontier/graph state is too fat to survive real fleets \
         (budget {} KiB per client)",
        parked_per_client / 1024,
        parked_delta / MIB,
        PARKED_PER_CLIENT_BUDGET_BYTES / 1024,
    );
    assert!(
        residual < RESIDUAL_AFTER_SWEEP_BUDGET_BYTES,
        "server still holds +{} MiB after reaping the whole fleet \
         (budget {} MiB)",
        residual / MIB,
        RESIDUAL_AFTER_SWEEP_BUDGET_BYTES / MIB,
    );

    writer.shutdown().await.expect("shutdown writer");
    server.shutdown().await;
}
