//! Does a settle pass cost what changed, or what is subscribed?
//!
//! Every write marks subscriptions dirty through three passes — `mark_subscriptions_dirty_with_origin`,
//! `mark_subscriptions_rows_changed`, `mark_rows_updated_in_subscriptions` — and each one
//! walks EVERY registered subscription, local and server, filtered only by
//! `subscription_involves_table`. That predicate is itself five linear scans over the
//! graph's table lists (`QueryGraph::involves_table`). None of it is counted by
//! `settle_cost`, so a production settle line cannot show it — exactly the blind spot that
//! hid the sealed-batch sweep until it was profiled.
//!
//! MEASURED in production 2026-08-19, several clients connecting: 1492 settle passes
//! totalling 63.0 s, the worst single pass 2.21 s at 414 subscriptions and 44482 graph
//! nodes, and cost correlating no more strongly with any one counter than with the others
//! (r between +0.35 and +0.61) — i.e. the pass costs what it is big, not what it did.
//!
//! This probe separates the two candidates the production log cannot: it holds the number
//! of subscriptions that CARE about the written table at one, and grows the number that do
//! not. Flat means the fan-out is already filtered and the cost is in the graph work of the
//! interested subscriptions. Rising means every write pays for every subscription in the
//! process, and an index from table to interested subscriptions removes it.
//!
//! A measurement, not a gate: it prints. Run with
//! `cargo test -p jazz-tools --release --features test --lib subscription_fanout -- --ignored --nocapture`.

use super::*;
use crate::storage::SqliteStorage;

fn two_table_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("docs")
                .column("owner", ColumnType::Text)
                .column("body", ColumnType::Text),
        )
        .table(
            TableSchema::builder("other")
                .column("owner", ColumnType::Text)
                .column("label", ColumnType::Text),
        )
        .build()
}

fn runtime(storage: SqliteStorage) -> RuntimeCore<SqliteStorage, NoopScheduler> {
    let app_id = AppId::from_name("subscription-fanout-cost");
    let schema_manager = SchemaManager::new(
        SyncManager::new(),
        two_table_schema(),
        app_id,
        "dev",
        "main",
    )
    .unwrap();
    let mut core = new_test_core(schema_manager, storage, NoopScheduler);
    core.immediate_tick();
    core
}

/// One write to `docs` with `interested` subscriptions watching `docs` and `uninterested`
/// watching `other`. Returns the mean microseconds a write-plus-settle took.
fn measure_split(interested: usize, uninterested: usize, writes: usize) -> f64 {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let storage = SqliteStorage::open(&dir.path().join("fanout.sqlite")).expect("sqlite");
    let mut core = runtime(storage);
    let alice = WriteContext::from_session(Session::new("alice"));

    // Seed both tables so every subscription has something to hold.
    for table in ["docs", "other"] {
        let values = HashMap::from([
            ("owner".to_string(), Value::Text("alice".to_string())),
            (
                if table == "docs" { "body" } else { "label" }.to_string(),
                Value::Text("seed".to_string()),
            ),
        ]);
        core.insert(table, values, Some(&alice)).expect("seed");
    }
    core.batched_tick();
    core.immediate_tick();

    {
        let qm = core.schema_manager_mut().query_manager_mut();
        for _ in 0..interested {
            qm.subscribe(qm.query("docs").build())
                .expect("subscribe docs");
        }
        // Everything else watches a table this write never touches.
        for _ in 0..uninterested {
            qm.subscribe(qm.query("other").build())
                .expect("subscribe other");
        }
    }
    core.batched_tick();
    core.immediate_tick();

    let started = web_time::Instant::now();
    for index in 0..writes {
        core.insert(
            "docs",
            HashMap::from([
                ("owner".to_string(), Value::Text("alice".to_string())),
                ("body".to_string(), Value::Text(format!("write {index}"))),
            ]),
            Some(&alice),
        )
        .expect("write");
        core.immediate_tick();
    }
    started.elapsed().as_secs_f64() / writes as f64 * 1e6
}

#[test]
#[ignore = "measurement, not a gate: prints the fan-out curve"]
fn a_write_should_cost_the_subscriptions_that_care_not_the_ones_that_do_not() {
    const WRITES: usize = 40;
    if std::env::var("JAZZ_FANOUT_PROFILE").is_ok() {
        // A long run at one point on the curve, so a sampler has something to catch.
        println!("profiling 400 interested subscriptions...");
        let us = measure_split(400, 0, 400);
        println!("  {us:.1} us per write");
        return;
    }
    println!("A) one interested subscription, growing the UNinterested crowd:");
    println!("  uninterested |  us per write | ratio");
    let mut base_a = 0.0;
    for uninterested in [0usize, 25, 100, 400] {
        let us = measure_split(1, uninterested, WRITES);
        if base_a == 0.0 {
            base_a = us;
        }
        println!(
            "  {uninterested:12} | {us:13.1} | {:.2}x",
            us / base_a.max(0.001)
        );
    }

    println!();
    println!("B) no uninterested crowd, growing the subscriptions that DO care:");
    println!("    interested |  us per write | ratio | us per interested sub");
    let mut base_b = 0.0;
    for interested in [1usize, 25, 100, 400] {
        let us = measure_split(interested, 0, WRITES);
        if base_b == 0.0 {
            base_b = us;
        }
        println!(
            "  {interested:12} | {us:13.1} | {:.2}x | {:.1}",
            us / base_b.max(0.001),
            (us - base_b) / interested.max(1) as f64
        );
    }
}
