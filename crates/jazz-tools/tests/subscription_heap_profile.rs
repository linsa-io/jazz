//! Byte-exact attribution of per-subscription retained memory.
//!
//! Runs the same scenario as `subscription_fixed_cost` (RocksDB server,
//! 3 chats × 40 tiny messages, warmed caches) but under the DHAT heap
//! profiler: every allocation records its call stack, and the profile is
//! written while the 80 measured subscriptions are STILL ALIVE — so the
//! "bytes live at profiler drop" statistic is exactly the retained cost of
//! those subscriptions, attributed to the allocating code line.
//!
//! Run:
//!   cargo test -p jazz-tools --features test,rocksdb \
//!     --test subscription_heap_profile -- --nocapture
//! Output: dhat-subscriptions.json (analyze with scripts or dh_view.html).

#![cfg(feature = "test")]

mod support;

use std::time::Duration;

use jazz_tools::object::ObjectId;
use jazz_tools::query_manager::policy::Operation;
use jazz_tools::query_manager::types::{permissions, policy_expr as pe};
use jazz_tools::row_input;
use jazz_tools::server::JazzServer;
use jazz_tools::sync_manager::DurabilityTier;
use jazz_tools::{ColumnType, QueryBuilder, SchemaBuilder, TableSchema, Value};
use support::{connect_ready_client, connect_ready_user};

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

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
async fn heap_profile_of_80_flat_subscriptions() {
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
    let chat0 = chat_ids[0];
    let thread_query = || {
        QueryBuilder::new("messages")
            .filter_eq("chat_id", Value::Uuid(chat0))
            .order_by_desc("created_at")
            .limit(50)
            .build()
    };

    // Warm: caches, compiles, storage block cache.
    let mut warm = Vec::new();
    for _ in 0..WARM_SUBS {
        warm.push(alice.subscribe(thread_query()).await.expect("subscribe"));
    }
    tokio::time::sleep(Duration::from_secs(2)).await;

    // ── profiled region: ONLY the marginal batch ───────────────────────────
    let profiler = dhat::Profiler::builder()
        .file_name("dhat-subscriptions.json")
        .trim_backtraces(None)
        .build();

    let mut measured = Vec::new();
    for _ in 0..MEASURED_SUBS {
        measured.push(alice.subscribe(thread_query()).await.expect("subscribe"));
    }
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Subscriptions are still alive here: bytes live at profiler drop are
    // exactly their retained cost.
    drop(profiler);
    eprintln!("dhat profile written: dhat-subscriptions.json");

    drop(measured);
    drop(warm);
    alice.shutdown().await.expect("shutdown alice");
    writer.shutdown().await.expect("shutdown writer");
    server.shutdown().await;
}
