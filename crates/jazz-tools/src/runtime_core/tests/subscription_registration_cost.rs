//! What one client action costs the server.
//!
//! `build_server_subscription_context` runs once per registered server subscription and
//! rebuilds the schema context from scratch every time: a full `Schema` clone for the
//! target, a clone of every registered lens, a clone of every live generation's schema, a
//! clone of every known schema again, and then `try_activate_pending()` over the result.
//! Nothing is cached, and the result depends only on the set of known schemas and lenses —
//! not on the subscription.
//!
//! MEASURED in production 2026-08-18, one person lightly active in one chat: 390
//! subscription registrations in three minutes, and 390 `identity crossing: activated a
//! pending schema` lines — one rebuild per registration, better than two per second, from a
//! single user. The server's cost per client action is what decides whether fifty active
//! users are ordinary load or an outage, and a client is entitled to behave badly.
//!
//! This is a MEASUREMENT, not a gate: it prints and asserts only a loose ceiling, so it
//! fails when the cost regresses by an order of magnitude rather than pinning a number that
//! varies with the machine.

use super::*;
use crate::storage::SqliteStorage;

fn docs_v1() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("docs")
                .column("owner", ColumnType::Text)
                .column("body", ColumnType::Text),
        )
        .build()
}

fn docs_with(extra: usize) -> Schema {
    let mut b = SchemaBuilder::new().table(
        TableSchema::builder("docs")
            .column("owner", ColumnType::Text)
            .column("body", ColumnType::Text),
    );
    for i in 0..extra {
        b = b.table(TableSchema::builder(&format!("filler{i}")).column("label", ColumnType::Text));
    }
    b.build()
}

fn runtime_over(
    schema: Schema,
    app_name: &str,
    storage: SqliteStorage,
) -> RuntimeCore<SqliteStorage, NoopScheduler> {
    let app_id = AppId::from_name(app_name);
    let mut schema_manager =
        SchemaManager::new(SyncManager::new(), schema, app_id, "dev", "main").unwrap();
    crate::schema_manager::rehydrate_schema_manager_from_catalogue(
        &mut schema_manager,
        &storage,
        app_id,
    )
    .expect("rehydrate from the persisted catalogue");
    let mut core = new_test_core(schema_manager, storage, NoopScheduler);
    core.immediate_tick();
    core
}

/// Registrations per second one core can serve, at `generations` live generations.
fn measure(generations: usize) -> (f64, usize) {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let path = dir.path().join("cost.sqlite");
    let storage = SqliteStorage::open(&path).expect("sqlite storage should open");

    let mut core = runtime_over(docs_v1(), "registration-cost", storage);
    let alice = WriteContext::from_session(Session::new("alice"));
    let _ = insert_and_wait_for_batch(
        &mut core,
        "docs",
        HashMap::from([
            ("owner".to_string(), Value::Text("alice".to_string())),
            ("body".to_string(), Value::Text("draft".to_string())),
        ]),
        Some(&alice),
        DurabilityTier::Local,
    )
    .expect("seed the row");
    core.batched_tick();
    core.immediate_tick();

    // Each crossing leaves one more generation behind in the context.
    for generation in 1..generations {
        let storage = core.into_storage();
        core = runtime_over(docs_with(generation), "registration-cost", storage);
        core.batched_tick();
        core.immediate_tick();
    }

    let branch = crate::storage::sole_branch_name(core.storage())
        .expect("branch registry readable")
        .map(|b| b.as_str().to_string())
        .unwrap_or_else(|| "dev-000000000000-main".to_string());

    let query = core
        .schema_manager_mut()
        .query_manager_mut()
        .query("docs")
        .branches(&[branch.as_str()])
        .build();

    const ROUNDS: usize = 200;
    let manager = core.schema_manager().query_manager();
    let started = web_time::Instant::now();
    let mut built = 0usize;
    for _ in 0..ROUNDS {
        if manager.build_server_subscription_context(&query).is_some() {
            built += 1;
        }
    }
    let elapsed = started.elapsed();
    (elapsed.as_secs_f64() / ROUNDS as f64 * 1e6, built)
}

#[test]
fn registering_a_server_subscription_does_not_rebuild_the_schema_world() {
    let (one_us, built_one) = measure(1);
    let (three_us, built_three) = measure(3);

    eprintln!("schema-context build per subscription registration:");
    eprintln!("  1 generation  : {one_us:8.1} us   ({built_one}/200 built)");
    eprintln!("  3 generations : {three_us:8.1} us   ({built_three}/200 built)");
    eprintln!(
        "  at 3 generations one core serves {:.0} registrations/s from this step alone",
        1e6 / three_us.max(0.001)
    );

    assert!(
        built_three > 0,
        "fixture precondition: the context must actually be built, else this measures nothing"
    );
    assert!(
        three_us < 500.0,
        "building the schema context for ONE subscription registration took {three_us:.0} us \
         at three generations. It clones every schema and every lens and re-runs pending \
         activation, per registration, though the result depends only on the set of known \
         schemas — not on the subscription. Production saw 390 of these in three minutes \
         from a single lightly-active user; the cost per client action is what decides \
         whether fifty active users are load or an outage."
    );
}
