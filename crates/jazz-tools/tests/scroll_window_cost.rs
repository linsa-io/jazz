//! What one scroll window costs, as the chat grows.
//!
//! The product question: opening the last 50 messages of a chat, and then the
//! previous 50, in chats of 10 000 and 100 000 messages. This measures it on
//! the app's real thread shape — a self-join `reply_to_message_id -> messages`
//! include carrying a `width x depth` satellite subtree — rather than fitting
//! a curve to the small production store.
//!
//! Two known engine behaviours decide the answer, both measured elsewhere in
//! this repo and unchanged here:
//!
//! - array subqueries are compiled BELOW `LimitOffset` (`graph/compile.rs`
//!   adds them at :740, the limit at :854), so an instance exists per SCANNED
//!   outer row, not per delivered row;
//! - a range filter on the ordering column is not pushed into the index scan,
//!   so a cursor narrows the RESULT but not the SCAN.
//!
//! Together they predict a window costing O(chat size) instead of O(50). This
//! file exists to say by how much, in instances, allocator churn, index reads
//! and wall time, at sizes three orders of magnitude apart.
//!
//! Reported, not asserted: it is a cost model for a product decision (infinite
//! virtual scroll), and the numbers are meant to change once the two defects
//! above are fixed. The gates that DO assert live in
//! `include_self_join_instance_blowup.rs`.
//!
//! ```text
//! cargo test --release -p jazz-tools --features test \
//!   --test scroll_window_cost -- --nocapture --ignored
//! ```
//!
//! `SCROLL_SIZES=1000,10000` overrides the swept chat sizes.

#![cfg(feature = "test")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use jazz_tools::object::ObjectId;
use jazz_tools::query_manager::manager::QueryManager;
use jazz_tools::query_manager::query::{ArraySubqueryBuilder, QueryBuilder};
use jazz_tools::query_manager::settle_cost::{INDEX_READS, LIVE_SUBQUERY_INSTANCES, ROW_LOADS};
use jazz_tools::query_manager::types::{
    ColumnDescriptor, ColumnType, RowDescriptor, Schema, TableName, Value,
};
use jazz_tools::storage::MemoryStorage;
use jazz_tools::sync_manager::SyncManager;
use jazz_tools::test_support::seeded_memory_storage;

// ============================================================================
// Tracking allocator: cumulative allocated bytes (churn), monotone.
// ============================================================================

struct TrackingAllocator;

/// Cumulative bytes handed out — churn, i.e. a CPU proxy.
static TOTAL_ALLOCATED: AtomicU64 = AtomicU64::new(0);
/// Bytes currently held — what the subscription actually costs in RSS.
static LIVE_BYTES: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            TOTAL_ALLOCATED.fetch_add(layout.size() as u64, Ordering::Relaxed);
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
            TOTAL_ALLOCATED.fetch_add(new_size as u64, Ordering::Relaxed);
            LIVE_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
            LIVE_BYTES.fetch_sub(layout.size() as u64, Ordering::Relaxed);
        }
        new_ptr
    }
}

#[global_allocator]
static GLOBAL: TrackingAllocator = TrackingAllocator;

fn total_allocated() -> u64 {
    TOTAL_ALLOCATED.load(Ordering::Relaxed)
}

fn live_bytes() -> u64 {
    LIVE_BYTES.load(Ordering::Relaxed)
}

// ============================================================================
// Schema and include tree — the app's thread shape
// ============================================================================

/// Branches under the self-join edge, and levels per branch.
const WIDTH: usize = 3;
const DEPTH: usize = 2;
/// Messages per window, i.e. the app's `.limit()`.
const WINDOW: usize = 50;
/// One message in this many carries a reply FK — the production ratio was
/// 8 of 115.
const REPLY_EVERY: usize = 14;

fn satellite(w: usize, d: usize) -> String {
    format!("sat_{w}_{d}")
}

fn schema() -> Schema {
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("messages"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Uuid),
            ColumnDescriptor::new("chat_id", ColumnType::Uuid),
            ColumnDescriptor::new("seq", ColumnType::Integer),
            ColumnDescriptor::new("body", ColumnType::Text),
            ColumnDescriptor::new("reply_to_message_id", ColumnType::Uuid).nullable(),
        ])
        .into(),
    );
    for w in 0..WIDTH {
        for d in 0..DEPTH {
            schema.insert(
                TableName::new(satellite(w, d)),
                RowDescriptor::new(vec![
                    ColumnDescriptor::new("id", ColumnType::Uuid),
                    ColumnDescriptor::new("parent_id", ColumnType::Uuid),
                    ColumnDescriptor::new("label", ColumnType::Text),
                ])
                .into(),
            );
        }
    }
    schema
}

fn nest(sub: ArraySubqueryBuilder, w: usize, d: usize) -> ArraySubqueryBuilder {
    if d + 1 >= DEPTH {
        return sub;
    }
    sub.with_array(satellite(w, d + 1), move |child| {
        nest(
            child.from(satellite(w, d + 1)).correlate("parent_id", "id"),
            w,
            d + 1,
        )
    })
}

fn reply_include(sub: ArraySubqueryBuilder) -> ArraySubqueryBuilder {
    let mut sub = sub.from("messages").correlate("id", "reply_to_message_id");
    for w in 0..WIDTH {
        sub = sub.with_array(satellite(w, 0), move |child| {
            nest(
                child.from(satellite(w, 0)).correlate("parent_id", "id"),
                w,
                0,
            )
        });
    }
    sub
}

// ============================================================================
// Measurement
// ============================================================================

struct Cost {
    instances: u64,
    churn: u64,
    held: i64,
    index_reads: u64,
    row_loads: u64,
    millis: u128,
    delivered: usize,
}

fn insert(
    qm: &mut QueryManager,
    storage: &mut MemoryStorage,
    schema: &Schema,
    branch: &str,
    table: &str,
    id: ObjectId,
    values: &[Value],
) {
    qm.insert_on_branch_with_schema_and_write_context_and_id(
        storage,
        table,
        branch,
        values,
        Some(id),
        schema,
        None,
        true,
    )
    .unwrap_or_else(|error| panic!("insert into {table} failed: {error:?}"));
}

/// Subscribe, settle, and read every counter as a delta.
///
/// `include` false is the CONTROL: the same window with no include at all,
/// which separates what the unbounded SCAN costs from what the include costs
/// on top of it.
fn open_window(
    qm: &mut QueryManager,
    storage: &mut MemoryStorage,
    cursor: Option<i32>,
    include: bool,
) -> Cost {
    let mut builder = QueryBuilder::new("messages");
    if let Some(cursor) = cursor {
        // Exactly how a scroll addresses the previous page: everything older
        // than the oldest row currently on screen.
        builder = builder.filter_lt("seq", Value::Integer(cursor));
    }
    if include {
        builder = builder.with_array("reply", reply_include);
    }
    let query = builder.order_by_desc("seq").limit(WINDOW).build();

    let instances = LIVE_SUBQUERY_INSTANCES.load(Ordering::Relaxed);
    let index_reads = INDEX_READS.load(Ordering::Relaxed);
    let row_loads = ROW_LOADS.load(Ordering::Relaxed);
    let churn = total_allocated();
    let held = live_bytes();
    let started = Instant::now();

    qm.subscribe(query).expect("subscribe to the window query");
    qm.process(storage);

    let millis = started.elapsed().as_millis();
    let delivered: usize = qm
        .take_updates()
        .iter()
        .map(|update| update.delta.added.len())
        .sum();

    Cost {
        instances: LIVE_SUBQUERY_INSTANCES.load(Ordering::Relaxed) - instances,
        churn: total_allocated() - churn,
        held: live_bytes() as i64 - held as i64,
        index_reads: INDEX_READS.load(Ordering::Relaxed) - index_reads,
        row_loads: ROW_LOADS.load(Ordering::Relaxed) - row_loads,
        millis,
        delivered,
    }
}

fn print(size: usize, page: &str, cost: &Cost) {
    eprintln!(
        "{size:>8} {page:>10} | {:>5} {:>10} {:>9.1} {:>9.1} {:>11} {:>10} {:>7}",
        cost.delivered,
        cost.instances,
        cost.held as f64 / 1_048_576.0,
        cost.churn as f64 / 1_048_576.0,
        cost.index_reads,
        cost.row_loads,
        cost.millis,
    );
}

fn sizes() -> Vec<usize> {
    std::env::var("SCROLL_SIZES")
        .ok()
        .map(|raw| {
            raw.split(',')
                .filter_map(|part| part.trim().parse().ok())
                .collect()
        })
        .unwrap_or_else(|| vec![1_000, 10_000, 100_000])
}

/// Seeding 100 000 rows takes minutes, so this is opt-in rather than part of
/// the default suite.
#[test]
#[ignore = "cost model, minutes to seed; run explicitly with --ignored --nocapture"]
fn scroll_window_cost_by_chat_size() {
    eprintln!(
        "\nwindow={WINDOW} msgs, include = self-join reply + {WIDTH}x{DEPTH} satellite subtree\n\
         one message in {REPLY_EVERY} carries a reply FK (production ratio was 8 of 115)\n\
         {:>8} {:>10} | {:>5} {:>10} {:>9} {:>9} {:>11} {:>10} {:>7}",
        "chat",
        "page",
        "deliv",
        "instances",
        "held MB",
        "churn MB",
        "index_reads",
        "row_loads",
        "ms",
    );

    for (size, include) in sizes()
        .into_iter()
        .flat_map(|size| [(size, true), (size, false)])
    {
        let mut qm = QueryManager::new(SyncManager::new());
        qm.set_current_schema(schema(), "dev", "main");
        let schema = qm.schema_context().current_schema.clone();
        let branch = qm.schema_context().branch_name().as_str().to_string();
        let mut storage = seeded_memory_storage(&schema);

        let chat = ObjectId::new();
        let seeding = Instant::now();

        // Message 0 is the reply target and the only one carrying satellite
        // chains, so a resolved include really walks WIDTH*DEPTH nodes and
        // finds a row at each.
        let mut first_id: Option<ObjectId> = None;
        for index in 0..size {
            let id = ObjectId::new();
            let reply_to = match first_id {
                Some(target) if index % REPLY_EVERY == 0 => Value::Uuid(target),
                _ => Value::Null,
            };
            insert(
                &mut qm,
                &mut storage,
                &schema,
                &branch,
                "messages",
                id,
                &[
                    Value::Uuid(id),
                    Value::Uuid(chat),
                    Value::Integer(index as i32),
                    Value::Text(format!("message {index}")),
                    reply_to,
                ],
            );
            first_id.get_or_insert(id);
        }
        let target = first_id.expect("at least one message");
        for w in 0..WIDTH {
            let mut parent = target;
            for d in 0..DEPTH {
                let id = ObjectId::new();
                insert(
                    &mut qm,
                    &mut storage,
                    &schema,
                    &branch,
                    &satellite(w, d),
                    id,
                    &[
                        Value::Uuid(id),
                        Value::Uuid(parent),
                        Value::Text(format!("sat {w}/{d}")),
                    ],
                );
                parent = id;
            }
        }
        // Drain the write-side updates so the window measurement below sees
        // only its own settle.
        qm.process(&mut storage);
        qm.take_updates();
        eprintln!(
            "{size:>8}   ({} include, seeded in {:.1}s)",
            if include { "with" } else { "no" },
            seeding.elapsed().as_secs_f64()
        );

        let last = open_window(&mut qm, &mut storage, None, include);
        print(size, "last 50", &last);

        // The previous page, addressed by the oldest `seq` on screen.
        let cursor = size as i32 - WINDOW as i32;
        let previous = open_window(&mut qm, &mut storage, Some(cursor), include);
        print(size, "prev 50", &previous);
    }

    eprintln!(
        "\nrow_loads is the scan width — every row the FILTER admits, not the 50 delivered,\n\
         because the include sits below the limit. The cursor narrows it only to \"everything\n\
         older\", so each page still costs O(chat). Held bytes stop growing once the\n\
         instance cache hits its per-node ceiling (MAX_CACHED_SUBGRAPHS x include nodes);\n\
         past that the cost lands entirely on CPU, as thrash.\n"
    );
}
