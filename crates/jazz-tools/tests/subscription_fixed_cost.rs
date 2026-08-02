//! Fixed server-side cost of ONE live subscription, independent of data size.
//!
//! Field observation (linsa, 2026-08-03): a device with a TINY visible slice
//! (~3 chats, ~100 messages — single-digit MB) costs the sync server ~1 GB on
//! connect. Row payload cannot explain it; the app opens on the order of a
//! hundred subscriptions per runtime (chat list, per-chat threads, per-message
//! satellites), so the suspect is the per-subscription fixed cost: compiled
//! graph + scan/materialize node state + include-tree instantiations.
//!
//! This test measures the marginal cost of additional include-tree
//! subscriptions over a small, warm dataset on RocksDB (production backend).

#![cfg(feature = "test")]

mod support;

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use jazz_tools::object::ObjectId;
use jazz_tools::query_manager::policy::Operation;
use jazz_tools::query_manager::types::{permissions, policy_expr as pe};
use jazz_tools::row_input;
use jazz_tools::server::JazzServer;
use jazz_tools::sync_manager::DurabilityTier;
use jazz_tools::{ColumnType, QueryBuilder, SchemaBuilder, TableSchema, Value};
use support::{connect_ready_client, connect_ready_user};

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

const KIB: u64 = 1 << 10;

/// Small dataset: the field device's slice is tiny — the cost under test is
/// per-SUBSCRIPTION, not per-row.
const CHATS: usize = 3;
const MESSAGES_PER_CHAT: usize = 40;

const WARM_SUBS: usize = 20;
const MEASURED_SUBS: usize = 80;

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
        .build()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn per_subscription_fixed_cost_on_small_data() {
    let schema = linsa_like_schema();
    let server = JazzServer::builder()
        .with_schema(schema.clone())
        .with_rocksdb_storage()
        .start()
        .await;

    let ready = Duration::from_secs(30);
    let writer = connect_ready_client(&server, &schema, "writer", "chats", ready).await;

    let mut chat_ids: Vec<ObjectId> = Vec::new();
    let mut last_batch = None;
    for chat_index in 0..CHATS {
        let (chat_id, _, batch) = writer
            .insert(
                "chats",
                row_input!(
                    "name" => format!("chat {chat_index}"),
                    "members" => Value::Array(vec!["alice".into(), "writer".into()]),
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
        chat_ids.push(chat_id);
    }
    writer
        .wait_for_batch(last_batch.expect("seeded"), DurabilityTier::EdgeServer)
        .await
        .expect("seed reaches server");

    let alice = connect_ready_user(&server, &schema, "alice", "chats", ready).await;

    let include_query = || {
        QueryBuilder::new("chats")
            .with_array("messages", |sub| {
                sub.from("messages").correlate("chat_id", "chats.id")
            })
            .build()
    };
    let thread_query = |chat_id: ObjectId| {
        QueryBuilder::new("messages")
            .filter_eq("chat_id", Value::Uuid(chat_id))
            .order_by_desc("created_at")
            .limit(50)
            .build()
    };

    // Warm phase: caches, first compiles.
    let mut warm = Vec::new();
    for i in 0..WARM_SUBS {
        let sub = if i % 2 == 0 {
            alice.subscribe(include_query()).await.expect("subscribe")
        } else {
            alice
                .subscribe(thread_query(chat_ids[i % CHATS]))
                .await
                .expect("subscribe")
        };
        warm.push(sub);
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    let after_warm = live_now();

    // Measured phase: identical shapes over warm everything.
    let mut measured = Vec::new();
    for i in 0..MEASURED_SUBS {
        let sub = if i % 2 == 0 {
            alice.subscribe(include_query()).await.expect("subscribe")
        } else {
            alice
                .subscribe(thread_query(chat_ids[i % CHATS]))
                .await
                .expect("subscribe")
        };
        measured.push(sub);
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    let after_measured = live_now();

    let marginal_total = after_measured.saturating_sub(after_warm);
    let per_sub = marginal_total / MEASURED_SUBS as u64;
    eprintln!(
        "{MEASURED_SUBS} extra subscriptions: +{} KiB total, ~{} KiB per subscription \
         (client+server combined, in-proc)",
        marginal_total / KIB,
        per_sub / KIB,
    );

    drop(measured);
    drop(warm);
    alice.shutdown().await.expect("shutdown alice");
    writer.shutdown().await.expect("shutdown writer");
    server.shutdown().await;
}
