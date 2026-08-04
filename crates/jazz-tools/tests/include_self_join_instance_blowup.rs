//! Subgraph-instance blow-up under a SELF-JOIN include that carries a subtree.
//!
//! Measured on a copy of the production store: an app query
//! `messages.where({chat_id, is_deleted: false}).orderBy(...).limit(50)` with an
//! include tree whose one self-referential edge is `reply_to_message_id ->
//! messages.id`. 115 live messages, 8 of which actually carry a non-null
//! `reply_to_message_id`. `LIVE_SUBQUERY_INSTANCES` on that subscription set:
//!
//! | subscription                                        | live instances |
//! |-----------------------------------------------------|---------------:|
//! | full include tree (3 subs)                          |         86 140 |
//! | same tree WITHOUT the self-join edge                |            904 |
//! | `replyToMessage: true` BARE, nothing under it       |            115 |
//! | `replyToMessage` + its subtree, nothing else        |         85 236 |
//! | same, outer `.limit()` 50 / 25 / 10                 | 85 236 each    |
//!
//! Three facts to explain: the count is independent of the outer LIMIT, it is
//! unrelated to the 8 rows that actually have a reply, and it appears only when
//! something is NESTED under the self-join — the bare edge costs one instance
//! per outer row.
//!
//! This file reproduces the shape on a synthetic schema and parameterises the
//! three axes independently (seeded rows, subtree width/depth, outer limit) so
//! the LAW can be read off the printed table instead of curve-fitted from a
//! single production data point.
//!
//! THE LAW, read off `self_join_include_instance_law` and exact on every row
//! of its table (`O` = outer rows the scan feeds the include, `Z` = those whose
//! correlation value is NULL, `T` = rows in the WHOLE inner table, `W` = the
//! include's top-level nested edges, `S` = nested instances a matched row's
//! deeper levels resolve):
//!
//! ```text
//! instances = O + Z x T x (W + S/T) + (O - Z) x (W + S_target)
//!           ~ Z x T x W          for the shapes that matter
//! ```
//!
//! `100 subscribed, 8 replies, W=3, D=2` -> `100 + 92x100x3 + 92x3 + 8x6
//! = 28 024`, measured 28 024. The outer LIMIT appears nowhere.
//!
//! TWO INDEPENDENT DEFECTS PRODUCE IT:
//!
//! 1. OPEN — the include runs BELOW the limit. `graph/compile.rs:740` adds the
//!    array-subquery nodes; `graph/compile.rs:854` adds the `LimitOffset` node
//!    afterwards, so every scanned outer row gets an instance no matter how few
//!    rows are delivered. That is the `O`, and the measured LIMIT independence
//!    (50/25/10/1 all give 28 024). Moving the include above the window is a
//!    planner change, not a patch — see the gate below.
//! 2. FIXED — a NULL correlation degenerated into a full inner-table scan.
//!    `graph_nodes/subgraph.rs:105` binds the correlation as
//!    `filter_eq(inner_column, correlation_value)`. For a null literal
//!    `Condition::is_index_scannable` (`query_manager/query.rs:281`) returns
//!    false, so `index_scan_plan` (`graph/compile.rs:2194`) fell back to
//!    `{ column: "_id", condition: ScanCondition::All }` — the whole table —
//!    and the residual became a `FilterNode` that `graph/compile.rs:837`
//!    places ABOVE the nested array subqueries, so every row of the inner
//!    table instantiated the entire nested subtree before the filter threw it
//!    away. That is the `Z x T x W`. `ArraySubqueryNode::
//!    evaluate_subgraph_for_single` now answers a null correlation with the
//!    empty array without compiling anything: 28 024 -> 56 instances,
//!    967 MB -> 4.4 MB of settle churn, 47 s -> 1 s on this file.
//!
//! The bare edge was always cheap because a full scan with no subtree under it
//! costs exactly one instance — which is why the production report saw 115 for
//! the bare edge and 85 236 the moment anything was nested under it.

#![cfg(feature = "test")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, OnceLock, PoisonError};

use jazz_tools::object::ObjectId;
use jazz_tools::query_manager::manager::QueryManager;
use jazz_tools::query_manager::query::{ArraySubqueryBuilder, QueryBuilder};
use jazz_tools::query_manager::settle_cost::LIVE_SUBQUERY_INSTANCES;
use jazz_tools::query_manager::types::{
    ColumnDescriptor, ColumnType, RowDescriptor, Schema, TableName, Value,
};
use jazz_tools::row_format::decode_row;
use jazz_tools::storage::MemoryStorage;
use jazz_tools::sync_manager::SyncManager;
use jazz_tools::test_support::seeded_memory_storage;

// ============================================================================
// Tracking allocator: cumulative allocated bytes (churn), monotone.
// Same pattern as `tests/include_instance_flatness.rs`.
// ============================================================================

struct TrackingAllocator;

static TOTAL_ALLOCATED: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            TOTAL_ALLOCATED.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            TOTAL_ALLOCATED.fetch_add(new_size as u64, Ordering::Relaxed);
        }
        new_ptr
    }
}

#[global_allocator]
static GLOBAL: TrackingAllocator = TrackingAllocator;

fn total_allocated() -> u64 {
    TOTAL_ALLOCATED.load(Ordering::Relaxed)
}

/// Serialises the measuring tests: the allocator counter is process-global.
fn measure_lock() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}

// ============================================================================
// Schema: a self-referential FK plus a grid of satellite tables
// ============================================================================

/// Satellite table at branch `w`, level `d` of the subtree under the include.
///
/// Level 0 hangs off a message (`parent_id -> messages.id`); level `d > 0`
/// hangs off level `d - 1` (`parent_id -> sat_{w}_{d-1}.id`). `width` branches
/// x `depth` levels therefore compile to exactly `width * depth` include nodes
/// inside the self-join's inner query.
fn satellite(w: usize, d: usize) -> String {
    format!("sat_{w}_{d}")
}

/// `messages` with a self-FK, plus `width * depth` satellite tables.
fn schema(width: usize, depth: usize) -> Schema {
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("messages"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Uuid),
            ColumnDescriptor::new("chat_id", ColumnType::Uuid),
            ColumnDescriptor::new("body", ColumnType::Text),
            // The self-join edge. Nullable, and null on nearly every row — the
            // production store had 8 non-null out of 115.
            ColumnDescriptor::new("reply_to_message_id", ColumnType::Uuid).nullable(),
        ])
        .into(),
    );
    for w in 0..width {
        for d in 0..depth {
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

// ============================================================================
// The include tree
// ============================================================================

/// Hang levels `d..depth` of branch `w` under an already-configured builder.
fn nest(sub: ArraySubqueryBuilder, w: usize, d: usize, depth: usize) -> ArraySubqueryBuilder {
    if d + 1 >= depth {
        return sub;
    }
    sub.with_array(satellite(w, d + 1), move |child| {
        nest(
            child.from(satellite(w, d + 1)).correlate("parent_id", "id"),
            w,
            d + 1,
            depth,
        )
    })
}

/// The self-join include: `reply_to_message_id -> messages.id`, carrying
/// `width` satellite branches of `depth` levels each.
fn reply_include(sub: ArraySubqueryBuilder, width: usize, depth: usize) -> ArraySubqueryBuilder {
    let mut sub = sub.from("messages").correlate("id", "reply_to_message_id");
    for w in 0..width {
        sub = sub.with_array(satellite(w, 0), move |child| {
            nest(
                child.from(satellite(w, 0)).correlate("parent_id", "id"),
                w,
                0,
                depth,
            )
        });
    }
    sub
}

// ============================================================================
// Scenario
// ============================================================================

#[derive(Clone, Copy)]
struct Scenario {
    /// Rows in the SUBSCRIBED chat.
    subscribed: usize,
    /// Rows in a second chat the subscription never selects. Present so the
    /// table size and the result-set size can move independently — that is
    /// what separates "quadratic in chat size" from "outer rows x table rows".
    other_chat: usize,
    /// How many subscribed rows carry a non-null `reply_to_message_id`.
    replies: usize,
    /// Satellite branches under the self-join edge.
    width: usize,
    /// Levels per branch. `width * depth` include nodes in the subtree.
    depth: usize,
    /// Outer `.limit()`.
    limit: usize,
}

impl Scenario {
    /// Include nodes inside the self-join's inner query.
    fn subtree_nodes(&self) -> usize {
        self.width * self.depth
    }

    /// What a correct engine may spend: one instance per DELIVERED outer row,
    /// plus that row's resolved subtree. Nothing here is a function of the
    /// table size, and nothing is a function of the rows the limit discards.
    fn budget(&self) -> u64 {
        let delivered = self.limit.min(self.subscribed);
        (delivered * (1 + self.subtree_nodes())) as u64
    }
}

struct Outcome {
    instances: u64,
    delivered: usize,
    bytes: u64,
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

/// Seed the store, subscribe, settle, and read the live-instance gauge.
///
/// `nested` false is the CONTROL: the same self-join edge with nothing under
/// it, which is what proves the multiplier lives in the nesting and not in the
/// self-join.
fn run(scenario: Scenario, nested: bool) -> Outcome {
    let (width, depth) = if nested {
        (scenario.width, scenario.depth)
    } else {
        (0, 0)
    };
    let mut qm = QueryManager::new(SyncManager::new());
    qm.set_current_schema(schema(scenario.width, scenario.depth), "dev", "main");
    let schema = qm.schema_context().current_schema.clone();
    let branch = qm.schema_context().branch_name().as_str().to_string();
    let mut storage = seeded_memory_storage(&schema);

    let subscribed_chat = ObjectId::new();
    let other_chat = ObjectId::new();

    // Message 0 is the one every reply points at, and the only one carrying a
    // satellite chain — so the subtree has real rows to find when it resolves.
    let mut message_ids = Vec::with_capacity(scenario.subscribed);
    for index in 0..scenario.subscribed {
        let id = ObjectId::new();
        // The LAST `replies` rows carry the FK; everything before them is null,
        // which is the production ratio (8 of 115).
        let reply_to = if index >= scenario.subscribed - scenario.replies && index > 0 {
            Value::Uuid(message_ids[0])
        } else {
            Value::Null
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
                Value::Uuid(subscribed_chat),
                Value::Text(format!("message {index}")),
                reply_to,
            ],
        );
        message_ids.push(id);
    }
    for index in 0..scenario.other_chat {
        let id = ObjectId::new();
        insert(
            &mut qm,
            &mut storage,
            &schema,
            &branch,
            "messages",
            id,
            &[
                Value::Uuid(id),
                Value::Uuid(other_chat),
                Value::Text(format!("other {index}")),
                Value::Null,
            ],
        );
    }

    // One satellite chain per branch, hanging off the reply TARGET, so a
    // resolved include really does walk `width * depth` nodes and find a row
    // at each of them.
    if let Some(target) = message_ids.first().copied() {
        for w in 0..scenario.width {
            let mut parent = target;
            for d in 0..scenario.depth {
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
    }

    let query = QueryBuilder::new("messages")
        .filter_eq("chat_id", Value::Uuid(subscribed_chat))
        .with_array("reply", move |sub| reply_include(sub, width, depth))
        .order_by("body")
        .limit(scenario.limit)
        .build();

    let baseline = LIVE_SUBQUERY_INSTANCES.load(Ordering::Relaxed);
    let bytes_before = total_allocated();
    qm.subscribe(query).expect("subscribe to the include query");
    qm.process(&mut storage);
    let bytes = total_allocated() - bytes_before;
    let instances = LIVE_SUBQUERY_INSTANCES.load(Ordering::Relaxed) - baseline;
    let delivered: usize = qm
        .take_updates()
        .iter()
        .map(|update| update.delta.added.len())
        .sum();

    Outcome {
        instances,
        delivered,
        bytes,
    }
}

fn header() {
    eprintln!(
        "\n{:>5} {:>5} {:>4} {:>3} {:>3} {:>5} | {:>4} {:>9} {:>7} {:>11} | {:>7}",
        "subs", "other", "repl", "W", "D", "limit", "deliv", "instances", "budget", "bytes", "bare",
    );
}

fn row(scenario: Scenario) -> (Outcome, Outcome) {
    let nested = run(scenario, true);
    let bare = run(scenario, false);
    eprintln!(
        "{:>5} {:>5} {:>4} {:>3} {:>3} {:>5} | {:>4} {:>9} {:>7} {:>11} | {:>7}",
        scenario.subscribed,
        scenario.other_chat,
        scenario.replies,
        scenario.width,
        scenario.depth,
        scenario.limit,
        nested.delivered,
        nested.instances,
        scenario.budget(),
        nested.bytes,
        bare.instances,
    );
    (nested, bare)
}

const BASE: Scenario = Scenario {
    subscribed: 100,
    other_chat: 0,
    replies: 8,
    width: 3,
    depth: 2,
    limit: 50,
};

// ============================================================================
// The measurement
// ============================================================================

/// Print the instance count across every axis, one variable at a time.
///
/// A probe, not a gate. It asserts only what must hold in BOTH worlds — before
/// and after any fix — so it stays green while the engine work is open: the
/// bare self-join edge never costs more than one instance per scanned outer
/// row, and the window really is the window. The contrasts that diagnose the
/// mechanism are REPORTED, not asserted, because their sign flips when the
/// engine is fixed; the assertions that pin them live in
/// [`self_join_include_instances_are_bounded_by_delivered_rows`].
#[test]
fn self_join_include_instance_law() {
    let _serialised = measure_lock();

    eprintln!(
        "\n=== live subgraph instances under a self-join include, one axis at a time ===\n\
         'bare' is the same self-join edge with NOTHING nested under it."
    );
    header();

    // Axis 1: rows in the subscribed chat. Table size moves with it.
    for subscribed in [25usize, 50, 100, 200] {
        row(Scenario { subscribed, ..BASE });
    }

    // Axis 2: rows in a chat the subscription never selects. The result set is
    // fixed at 25 rows; only the TABLE grows. A law that were quadratic in the
    // subscribed chat would be flat here.
    for other_chat in [0usize, 75, 175] {
        row(Scenario {
            subscribed: 25,
            other_chat,
            ..BASE
        });
    }

    // Axis 3: the outer limit. Production saw 50/25/10 give the same count.
    for limit in [50usize, 25, 10, 1] {
        row(Scenario { limit, ..BASE });
    }

    // Axis 4: subtree width and depth — the nodes compiled INSIDE the
    // self-join's inner query.
    for (width, depth) in [
        (0usize, 0usize),
        (1, 1),
        (2, 1),
        (3, 1),
        (1, 2),
        (3, 2),
        (3, 3),
    ] {
        row(Scenario {
            width,
            depth,
            ..BASE
        });
    }

    // Axis 5: how many outer rows actually HAVE a reply. The production report
    // said the count was unrelated to the 8 real replies; this is that claim.
    for replies in [0usize, 8, 50, 100] {
        row(Scenario { replies, ..BASE });
    }

    // ── world-independent invariants ────────────────────────────────────────
    let scenario = BASE;
    let nested = run(scenario, true);
    let bare = run(scenario, false);
    assert!(
        bare.instances <= scenario.subscribed as u64,
        "the BARE self-join edge must cost at most one instance per scanned outer row, \
         got {} for {} rows",
        bare.instances,
        scenario.subscribed,
    );
    assert_eq!(
        nested.delivered,
        scenario.limit.min(scenario.subscribed),
        "the window must be the window — a probe over a subscription that did not deliver \
         its limit is measuring something else"
    );

    // ── the three diagnostic contrasts, reported ────────────────────────────
    //
    // 1. TABLE SIZE, result set held fixed: rows in a chat the subscription
    //    never selects. Any movement here is the correlated inner query
    //    scanning the whole table (`ScanCondition::All` in `index_scan_plan`).
    let narrow = run(
        Scenario {
            subscribed: 25,
            other_chat: 0,
            ..BASE
        },
        true,
    );
    let widened = run(
        Scenario {
            subscribed: 25,
            other_chat: 175,
            ..BASE
        },
        true,
    );
    // 2. NULL vs resolved correlation over the same rows: a null FK must not
    //    cost more than a real one (`Condition::is_index_scannable`).
    let all_null = run(Scenario { replies: 0, ..BASE }, true);
    let all_bound = run(
        Scenario {
            replies: BASE.subscribed,
            ..BASE
        },
        true,
    );
    // 3. The outer limit, with every correlation resolved so nothing else
    //    moves. This is the include running BELOW `LimitOffset`.
    let wide_window = run(
        Scenario {
            replies: BASE.subscribed,
            limit: 50,
            ..BASE
        },
        true,
    );
    let narrow_window = run(
        Scenario {
            replies: BASE.subscribed,
            limit: 1,
            ..BASE
        },
        true,
    );

    eprintln!(
        "\ncontrasts (reported, not asserted — their sign flips when the engine is fixed):\
         \n  bare edge {} vs {}-node subtree {} for {} delivered rows\
         \n  table 25 -> 200 rows, result set fixed at 25: {} -> {} instances\
         \n  correlation all-NULL vs all-resolved, same 100 rows: {} -> {} instances\
         \n  limit 50 -> 1, all correlations resolved: {} -> {} instances for {} -> {} \
         delivered rows\n",
        bare.instances,
        scenario.subtree_nodes(),
        nested.instances,
        nested.delivered,
        narrow.instances,
        widened.instances,
        all_null.instances,
        all_bound.instances,
        wide_window.instances,
        narrow_window.instances,
        wide_window.delivered,
        narrow_window.delivered,
    );
}

/// The gate. RED.
///
/// Instances must be proportional to the rows the subscription actually
/// DELIVERS and to the subtree those rows resolve — never to the size of the
/// inner table, and never to outer rows the limit discards.
///
/// The matrix covers both defects named in this file's header, so the gate
/// keeps holding the fixed one while the open one keeps it red:
/// - the first two scenarios resolve EVERY correlation value, so nothing but
///   the outer limit can move: they isolate the include running below
///   `LimitOffset` (`graph/compile.rs:740` vs `:854`), which is OPEN;
/// - the last two are all-null / mostly-null correlations over a table much
///   larger than the result set: they hold the null-correlation short-circuit
///   in `array_subquery.rs::evaluate_subgraph_for_single`, which is FIXED, and
///   would go red again if it regressed.
///
/// IGNORED, like `sync_telemetry_otel`'s collector test, so CI stays green
/// while the engine work is open: this is the self-join include-instance
/// blow-up investigation (production `LIVE_SUBQUERY_INSTANCES` 86 140 for a
/// 115-row store; see this file's header for the measured table). Remove the
/// `#[ignore]` when the include is placed above the outer limit.
#[test]
#[ignore = "self-join include-instance blow-up investigation: RED until the include is bounded by the outer limit (graph/compile.rs places array subqueries below LimitOffset)"]
fn self_join_include_instances_are_bounded_by_delivered_rows() {
    let _serialised = measure_lock();

    for scenario in [
        Scenario {
            replies: BASE.subscribed,
            limit: 10,
            ..BASE
        },
        Scenario {
            replies: BASE.subscribed,
            subscribed: 200,
            limit: 50,
            ..BASE
        },
        Scenario {
            subscribed: 25,
            other_chat: 175,
            ..BASE
        },
        Scenario { replies: 0, ..BASE },
    ] {
        let outcome = run(scenario, true);
        assert!(
            outcome.instances <= scenario.budget(),
            "{} subscribed rows ({} more in the table), {} with a real reply, subtree {}x{} \
             = {} nodes, limit {}: {} delivered rows but {} live subgraph instances \
             (budget {}). Instances must scale with the DELIVERED rows and the subtree \
             they resolve — not with outer rows the limit discards, and not with the \
             size of the inner table.",
            scenario.subscribed,
            scenario.other_chat,
            scenario.replies,
            scenario.width,
            scenario.depth,
            scenario.subtree_nodes(),
            scenario.limit,
            outcome.delivered,
            outcome.instances,
            scenario.budget(),
        );
    }
}

// ============================================================================
// The semantic guard for the null-correlation short-circuit
// ============================================================================

/// `x = NULL` is UNKNOWN, so the include is EMPTY — it is not `x IS NULL`.
///
/// This is the only observable behaviour change in the null-correlation
/// short-circuit (`ArraySubqueryNode::evaluate_subgraph_for_single`), and
/// nothing else in the suite can see it:
///
/// - the fixtures above correlate on `id`, a primary key that is never null, so
///   both the old and the new path return the empty array and any assertion
///   over them passes in either world;
/// - `subscription_output_oracle` is a differential between two engines running
///   the SAME code, and its generator has no nullable inner correlation column
///   either, so it cannot distinguish the two semantics at all.
///
/// WHAT THE OLD PATH DID. `SubgraphTemplate::instantiate` binds the correlation
/// as `filter_eq(inner_column, correlation_value)`. With a null binding
/// `Condition::null_literal_predicate` (`query_manager/query.rs:400`) rewrote
/// `Eq { value: NULL }` into `Predicate::IsNull`, so the include for an outer
/// row with a null FK returned EVERY inner row whose correlation column was
/// itself null — rows that are related to nothing, handed back as if they were
/// that row's children.
///
/// WHY EMPTY IS RIGHT. In three-valued logic `x = NULL` evaluates to UNKNOWN
/// for every `x`, including when `x` is itself NULL, and a WHERE clause keeps
/// only rows for which the predicate is TRUE. So a correlated subquery bound to
/// a null value matches no row, ever. `IS NULL` is a different operator with a
/// different truth table — it is TRUE exactly where `= NULL` is UNKNOWN — and
/// the compile path silently substituted one for the other. NULL means "unknown
/// key", and two rows with unknown keys are not thereby related.
///
/// Asserted on the DELIVERED array column, not on instance counters: the
/// counters are identical between the two semantics (both paths end up caching
/// one instance per outer row), so only the contents can tell them apart.
#[test]
fn null_correlation_include_is_empty_not_every_null_keyed_inner_row() {
    let _serialised = measure_lock();

    let mut schema = Schema::new();
    schema.insert(
        TableName::new("holders"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Uuid),
            ColumnDescriptor::new("label", ColumnType::Text),
            // The outer side of the correlation, null on one of the two rows.
            ColumnDescriptor::new("note_key", ColumnType::Uuid).nullable(),
        ])
        .into(),
    );
    schema.insert(
        TableName::new("notes"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Uuid),
            // The INNER side, nullable — this is what makes the difference
            // observable. Correlating on a primary key cannot show it.
            ColumnDescriptor::new("key", ColumnType::Uuid).nullable(),
            ColumnDescriptor::new("body", ColumnType::Text),
        ])
        .into(),
    );

    let mut qm = QueryManager::new(SyncManager::new());
    qm.set_current_schema(schema, "dev", "main");
    let schema = qm.schema_context().current_schema.clone();
    let branch = qm.schema_context().branch_name().as_str().to_string();
    let mut storage = seeded_memory_storage(&schema);

    // A key value that no note's id equals — it is a correlation key, not a row
    // identity, so the include must resolve it by VALUE.
    let key = ObjectId::new();
    for (note_key, body) in [
        (Value::Uuid(key), "bound"),
        // The bait: inner rows that DO carry null in the correlation column.
        // `IS NULL` returns exactly these two; `= NULL` returns neither.
        (Value::Null, "orphan-a"),
        (Value::Null, "orphan-b"),
    ] {
        let id = ObjectId::new();
        insert(
            &mut qm,
            &mut storage,
            &schema,
            &branch,
            "notes",
            id,
            &[Value::Uuid(id), note_key, Value::Text(body.into())],
        );
    }
    for (label, note_key) in [
        ("bound-holder", Value::Uuid(key)),
        ("null-holder", Value::Null),
    ] {
        let id = ObjectId::new();
        insert(
            &mut qm,
            &mut storage,
            &schema,
            &branch,
            "holders",
            id,
            &[Value::Uuid(id), Value::Text(label.into()), note_key],
        );
    }

    // A bare subscription over the inner table, so a fixture that failed to
    // seed the orphan rows cannot make the assertion below pass vacuously.
    let notes_sub = qm
        .subscribe(QueryBuilder::new("notes").order_by("body").build())
        .expect("subscribe to notes");
    let holders_sub = qm
        .subscribe(
            QueryBuilder::new("holders")
                .with_array("notes", |sub| {
                    sub.from("notes").correlate("key", "note_key")
                })
                .order_by("label")
                .build(),
        )
        .expect("subscribe to holders with the include");
    qm.process(&mut storage);

    let updates = qm.take_updates();
    let notes_update = updates
        .iter()
        .find(|update| update.subscription_id == notes_sub)
        .expect("the notes subscription must deliver");
    assert_eq!(
        notes_update.delta.added.len(),
        3,
        "the fixture must actually contain the two null-keyed notes the old \
         `IS NULL` path would have returned — without them this test proves nothing"
    );

    let holders_update = updates
        .iter()
        .find(|update| update.subscription_id == holders_sub)
        .expect("the holders subscription must deliver");
    let arrays: Vec<(String, Vec<String>)> = holders_update
        .delta
        .added
        .iter()
        .map(|row| {
            let values = decode_row(&holders_update.descriptor, &row.data)
                .expect("decode the delivered holder row");
            let label = match &values[1] {
                Value::Text(label) => label.clone(),
                other => panic!("expected a holder label, got {other:?}"),
            };
            let notes = values[3]
                .as_array()
                .expect("the include must deliver an array column")
                .iter()
                .map(|element| {
                    let note = element.as_row().expect("include elements are rows");
                    match &note[2] {
                        Value::Text(body) => body.clone(),
                        other => panic!("expected a note body, got {other:?}"),
                    }
                })
                .collect();
            (label, notes)
        })
        .collect();

    assert_eq!(
        arrays.len(),
        2,
        "both holders must be delivered, got {arrays:?}"
    );
    let (bound_label, bound_notes) = &arrays[0];
    assert_eq!(bound_label, "bound-holder");
    assert_eq!(
        bound_notes,
        &vec!["bound".to_string()],
        "a holder whose FK resolves must still get its note — an include that \
         returned nothing here would make the null case below vacuous"
    );

    let (null_label, null_notes) = &arrays[1];
    assert_eq!(null_label, "null-holder");
    assert!(
        null_notes.is_empty(),
        "an outer row whose correlation value is NULL must get an EMPTY include, \
         got {null_notes:?}. `x = NULL` is UNKNOWN for every x, so the correlated \
         subquery matches no row; returning the null-keyed inner rows is `IS NULL` \
         semantics, which is what the compile path substituted before the \
         short-circuit in `ArraySubqueryNode::evaluate_subgraph_for_single`"
    );
}
