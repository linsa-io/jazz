//! Allocation budget for include-subquery compilation.
//!
//! Field failure (2026-08-02, linsa): a device client ingesting a chat with an
//! include-tree subscription ballooned to iOS's 3 GB per-process limit and was
//! jetsam-killed. Two lldb captures on the frozen process showed the JS thread
//! inside `evaluate_subgraph → instantiate → compile_array_subquery`, cloning
//! `(TableName, TableSchema)` pairs — the engine clones the ENTIRE application
//! schema for every nested array-subquery node of every instantiation of every
//! outer row. On a progressively-syncing client that multiplies into tens of
//! thousands of full-schema clones.
//!
//! This test pins an allocation budget on exactly that path: a deliberately fat
//! schema (so every stray schema clone is expensive and visible), an include
//! subscription, and outer rows arriving ONE BY ONE (each with its own settle
//! wave, like a device catching up). The counting allocator measures bytes
//! allocated across the ingest phase; a full-schema-clone-per-compile regime
//! blows the budget by an order of magnitude.

#![cfg(feature = "test")]

mod support;

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
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

// ── counting allocator ──────────────────────────────────────────────────────

struct CountingAllocator;

static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCATED_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

fn allocated_now() -> u64 {
    ALLOCATED_BYTES.load(Ordering::Relaxed)
}

// ── fat schema ──────────────────────────────────────────────────────────────

/// Number of padding tables. Sized so one full schema clone is ~man MBs of
/// small allocations — cheap enough to build once, expensive enough that a
/// clone-per-compile regime dominates every other allocation in the test.
const PAD_TABLES: usize = 120;
const PAD_COLUMNS: usize = 32;

fn fat_schema() -> jazz_tools::Schema {
    let mut builder = SchemaBuilder::new()
        .table(
            TableSchema::builder("messages")
                .column("chat_id", ColumnType::Text)
                .column("body", ColumnType::Text)
                .column("created_at", ColumnType::Integer),
        )
        .table(
            TableSchema::builder("attachments")
                .fk_column("message_id", "messages")
                .column("kind", ColumnType::Text),
        )
        .table(
            TableSchema::builder("variants")
                .fk_column("attachment_id", "attachments")
                .column("tier", ColumnType::Text),
        );
    for table_index in 0..PAD_TABLES {
        let mut table = TableSchema::builder(&format!(
            "padding_table_with_a_deliberately_long_name_{table_index:03}"
        ));
        for column_index in 0..PAD_COLUMNS {
            table = table.column(
                &format!("padding_column_with_a_deliberately_long_name_{column_index:03}"),
                ColumnType::Text,
            );
        }
        builder = builder.table(table);
    }
    builder.build()
}

// ── the test ────────────────────────────────────────────────────────────────

const OUTER_ROWS: usize = 60;

/// Bytes allowed across the whole ingest phase (subscription live, rows landing
/// one wave at a time). Calibration on this suite's rig:
///   with full-schema clones per nested compile:  ~462 MiB (measured)
///   with shared (Arc) schema/context handles:    ~324 MiB (measured)
/// The residual schema-scale term is tracked separately; this budget guards
/// the Arc sharing from regressing back to clone-per-compile.
/// The budget sits far above the healthy figure and far below the pathological
/// one, so it neither flaps nor forgives the regression.
const INGEST_ALLOCATION_BUDGET_BYTES: u64 = 400 * 1024 * 1024;

#[tokio::test]
async fn include_ingest_stays_within_allocation_budget() {
    let schema = fat_schema();
    let server = JazzServer::start_with_schema(schema.clone()).await;

    let writer = make_client(&server, "alloc-writer").await;
    let reader = make_client(&server, "alloc-reader").await;

    let include_query = QueryBuilder::new("messages")
        .filter_eq("chat_id", Value::Text("chat-under-test".into()))
        .order_by_desc("created_at")
        .limit(50)
        .with_array("attachments", |sub| {
            sub.from("attachments")
                .correlate("message_id", "messages.id")
                .with_array("variants", |nested| {
                    nested
                        .from("variants")
                        .correlate("attachment_id", "attachments.id")
                })
        })
        .build();
    let mut sub = reader.subscribe(include_query).await.expect("subscribe");

    // ── measured phase: rows arrive one settle wave at a time ──────────────
    let started_bytes = allocated_now();

    let mut expected: BTreeSet<ObjectId> = BTreeSet::new();
    for index in 0..OUTER_ROWS {
        let (message_id, _, batch_id) = writer
            .insert(
                "messages",
                row_input!(
                    "chat_id" => "chat-under-test",
                    "body" => format!("message {index}"),
                    "created_at" => index as i32,
                ),
            )
            .expect("insert message");
        writer
            .wait_for_batch(batch_id, DurabilityTier::EdgeServer)
            .await
            .expect("message reaches the server");
        let (_, _, attachment_batch) = writer
            .insert(
                "attachments",
                row_input!("message_id" => Value::Uuid(message_id), "kind" => "photo"),
            )
            .expect("insert attachment");
        writer
            .wait_for_batch(attachment_batch, DurabilityTier::EdgeServer)
            .await
            .expect("attachment reaches the server");
        expected.insert(message_id);
    }

    // Drain until every message was delivered to the live include subscription.
    let mut seen: BTreeSet<ObjectId> = BTreeSet::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while !expected.is_subset(&seen) {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "include subscription never delivered all rows (saw {seen:?})"
        );
        if let Ok(Some(delta)) = tokio::time::timeout(remaining, sub.next()).await {
            for added in &delta.added {
                seen.insert(added.id);
            }
        } else {
            panic!("subscription stream closed early");
        }
    }

    let ingest_bytes = allocated_now() - started_bytes;
    eprintln!(
        "include ingest allocated {} MiB across {} rows",
        ingest_bytes / (1 << 20),
        OUTER_ROWS
    );
    assert!(
        ingest_bytes < INGEST_ALLOCATION_BUDGET_BYTES,
        "include ingest allocated {} MiB — the subquery compile path is \
         allocating like it clones the schema per compile again (budget {} MiB)",
        ingest_bytes / (1 << 20),
        INGEST_ALLOCATION_BUDGET_BYTES / (1 << 20),
    );

    writer.shutdown().await.expect("shutdown writer");
    reader.shutdown().await.expect("shutdown reader");
    server.shutdown().await;
}

async fn make_client(server: &JazzServer, user_id: &str) -> JazzClient {
    let schema = fat_schema();
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
        data_dir: TempDir::new().expect("client dir").keep(),
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
        QueryBuilder::new("messages").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(30),
        format!("EdgeServer readiness for {user_id}"),
        |_| Some(()),
    )
    .await;
    client
}
