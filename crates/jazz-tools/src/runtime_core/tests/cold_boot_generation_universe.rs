//! A cold-booted runtime must be able to read the schema generations its own
//! store already holds.
//!
//! MEASURED shape this gate models (rpc-server, 2026-08-16). A store that spans
//! two schema generations serves rows from both when the runtime is constructed
//! with `rehydrate_schema_manager_from_catalogue`, and serves only the CURRENT
//! generation's branch when it is constructed without it. Four independent
//! tables in the live process matched the current-branch-only counts exactly
//! (`users` 2, `user_emails` 0, `apple_identities` 0, `unique_names` 0), and the
//! membership row the 403s hang on lives only on the older branch.
//!
//! Why a cold boot can be missing generations it has already persisted:
//!
//! * `RuntimeCore::new` (runtime_core/mod.rs:619) does NOT read the catalogue.
//!   The branch universe comes from `SchemaContext::all_branch_names`
//!   (schema_manager/context.rs:212) = current branch + one per `live_schemas`,
//!   and a freshly constructed `SchemaManager` has `live_schemas` empty.
//! * Every binding is expected to close that gap itself. `jazz-rn`
//!   (rust/src/lib.rs:734), `jazz-wasm` (src/runtime.rs:1364), the tokio client
//!   (client.rs:127, :150) and the server builder (server/builder.rs:236) all
//!   call `rehydrate_schema_manager_from_catalogue`. `jazz-napi` did not,
//!   until crates/jazz-napi/src/lib.rs:624.
//! * Nor does sync heal it: `SyncManager::persist_catalogue_entry`
//!   (sync_manager/sync_logic.rs:199) reported "storage unchanged" for an entry already
//!   byte-identical in storage, so a re-sent catalogue entry is never queued
//!   into `pending_catalogue_updates` and the in-memory `SchemaManager` never
//!   learns a generation its own store has held since the last restart.
//!
//! Two invariants are pinned here, and deliberately NOT a third. Reading the
//! catalogue stays the binding's job, done before construction: `RuntimeCore`
//! knows nothing of catalogue semantics, four bindings already do the read
//! before they hand storage over (it is moved into the constructor), and
//! folding it in would make a constructor silently mutate the manager it was
//! given. So this file does not assert that a runtime built WITHOUT that read
//! serves the older generation — it cannot, by design. What it asserts is:
//!
//! 1. built the way every binding builds it, a runtime serves every generation
//!    its own store records;
//! 2. built without the read, a runtime SAYS SO
//!    (`RuntimeCore::unknown_store_schema_generations`) instead of silently
//!    serving half its store — which is how this went unnoticed for nine days.
//!
//! The binding's own obligation is gated where the defect lived, in
//! jazz-napi's `a_runtime_reads_the_schema_generations_its_own_store_records`.
//!
//! A durability tier cannot rescue any of this — tiers gate delivery and filter
//! tuples the graph already produced
//! (`QueryManager::filter_synced_query_scope_tuples`, query_manager/manager.rs:3051,
//! which only ever REMOVES tuples), so a row outside the branch universe is
//! unreachable at every tier.
//!
//! Must be `SqliteStorage`: the catalogue and the per-generation raw-table
//! families are the state under test, and `MemoryStorage` overrides the
//! visible-row reads against in-memory structs (storage/memory.rs:899, :929).

use super::*;
use crate::storage::SqliteStorage;

/// Generation A.
fn docs_schema_v1() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("docs")
                .column("owner", ColumnType::Text)
                .column("body", ColumnType::Text),
        )
        .build()
}

/// Generation B: `docs` is byte-identical, an added table moves the schema
/// hash and therefore the composed branch name. This is the shape of the
/// migration that added `chat_activities` to the production app.
fn docs_schema_v2() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("docs")
                .column("owner", ColumnType::Text)
                .column("body", ColumnType::Text),
        )
        .table(TableSchema::builder("tags").column("label", ColumnType::Text))
        .build()
}

const APP: &str = "cold-boot-generation-universe";

/// The construction every binding EXCEPT jazz-napi performs.
fn runtime_with_rehydrate<S: Storage>(schema: Schema, storage: S) -> RuntimeCore<S, NoopScheduler> {
    let app_id = AppId::from_name(APP);
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

/// The construction `jazz-napi` performs: declared schema, straight into
/// `RuntimeCore::new`, and no catalogue read — the shape jazz-napi had before
/// crates/jazz-napi/src/lib.rs:624.
fn runtime_cold_boot<S: Storage>(schema: Schema, storage: S) -> RuntimeCore<S, NoopScheduler> {
    let app_id = AppId::from_name(APP);
    let schema_manager =
        SchemaManager::new(SyncManager::new(), schema, app_id, "dev", "main").unwrap();
    let mut core = new_test_core(schema_manager, storage, NoopScheduler);
    core.immediate_tick();
    core
}

fn branch_universe<S: Storage>(core: &RuntimeCore<S, NoopScheduler>) -> Vec<String> {
    core.schema_manager().query_manager().all_query_branches()
}

/// Every `docs` row the runtime's own read path serves. `RuntimeCore::query`
/// resolves inside `immediate_tick`, so the future is ready when it returns.
fn docs_rows<S: Storage>(core: &mut RuntimeCore<S, NoopScheduler>) -> Vec<ObjectId> {
    let future = core.query(QueryBuilder::new("docs").build(), None);
    futures::executor::block_on(future)
        .expect("the docs query should resolve")
        .into_iter()
        .map(|(row_id, _)| row_id)
        .collect()
}

/// Seed a store that spans both generations, with the row written under
/// generation A and never rewritten. Returns the row id and the branch it
/// physically lives on.
fn two_generation_store(path: &std::path::Path) -> (SqliteStorage, ObjectId, String) {
    let storage = SqliteStorage::open(path).expect("sqlite storage should open");

    let mut core = runtime_with_rehydrate(docs_schema_v1(), storage);
    let alice = WriteContext::from_session(Session::new("alice"));
    let ((row_id, _), _) = insert_and_wait_for_batch(
        &mut core,
        "docs",
        HashMap::from([
            ("owner".to_string(), Value::Text("alice".to_string())),
            ("body".to_string(), Value::Text("draft".to_string())),
        ]),
        Some(&alice),
        DurabilityTier::Local,
    )
    .expect("the row inserts under generation A");
    core.batched_tick();
    core.immediate_tick();
    let generation_a_branch = branch_universe(&core)
        .first()
        .cloned()
        .expect("generation A has a branch");
    let storage = core.into_storage();

    // The migration: the same store comes up under generation B, which
    // persists B's catalogue entry alongside A's. The row is NOT rewritten.
    let mut core = runtime_with_rehydrate(docs_schema_v2(), storage);
    core.batched_tick();
    core.immediate_tick();
    assert!(
        branch_universe(&core).contains(&generation_a_branch),
        "precondition: a rehydrating runtime must see generation A's branch after the \
         migration, or this fixture never created a crossing: {:?}",
        branch_universe(&core)
    );
    assert!(
        core.storage()
            .load_visible_region_row("docs", &generation_a_branch, row_id)
            .expect("visible read should succeed")
            .is_some(),
        "precondition: the row must still live on generation A's branch"
    );

    (core.into_storage(), row_id, generation_a_branch)
}

/// What a runtime built without the catalogue read actually does — recorded so
/// the shape of the outage stays visible in the suite, and so the `unknown`
/// signal below is measured against a universe we know to be narrow.
///
/// This deliberately asserts the CURRENT behaviour, not the desired one: the
/// desired behaviour belongs to the binding, and is gated in jazz-napi. If this
/// test ever fails because the cold-boot universe widened on its own, the read
/// moved into the core and this file's contract needs rewriting rather than
/// patching.
#[test]
fn a_runtime_built_without_the_catalogue_read_serves_only_its_declared_generation() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let path = dir.path().join("cold-boot.sqlite");
    let (storage, row_id, generation_a_branch) = two_generation_store(&path);

    let mut core = runtime_cold_boot(docs_schema_v2(), storage);

    let universe = branch_universe(&core);
    assert!(
        !universe.contains(&generation_a_branch),
        "reading the catalogue is the binding's job, done before construction; if a runtime \
         built without it now covers generation A ({universe:?}), the read moved into the \
         core and this file's contract must be rewritten"
    );
    let served = docs_rows(&mut core);
    assert!(
        !served.contains(&row_id),
        "and the generation-A row must be the thing that goes missing — that is the outage: \
         a row present in storage, indexed, and unreachable at every durability tier"
    );
}

/// The control. Same store, same assertions, constructed the way every other
/// binding constructs it. Green today — it is what proves the gate above is
/// about the constructor and not about the fixture.
#[test]
fn a_rehydrating_runtime_serves_the_generations_its_own_store_records() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let path = dir.path().join("rehydrated.sqlite");
    let (storage, row_id, generation_a_branch) = two_generation_store(&path);

    let mut core = runtime_with_rehydrate(docs_schema_v2(), storage);

    let universe = branch_universe(&core);
    assert!(
        universe.contains(&generation_a_branch),
        "control: a rehydrating runtime must include generation A's branch; got {universe:?}"
    );

    let served = docs_rows(&mut core);
    assert!(
        served.contains(&row_id),
        "control: a rehydrating runtime must serve the generation-A row {row_id}; it served {served:?}"
    );
}

/// The class gate: whatever the constructor is handed, a runtime standing over
/// a store whose visible rows span generations it cannot enumerate must say so.
///
/// The cold-boot gate above pins the outage. This one pins the thing that let
/// the outage go unnoticed for nine days: reading the catalogue before
/// construction is a per-binding obligation with no enforcement anywhere —
/// jazz-rn (rust/src/lib.rs:734), jazz-wasm (src/runtime.rs:1364), the tokio
/// client (client.rs:127, :150) and the server builder (builder.rs:236) all
/// honour it, jazz-napi did not, and nothing failed. A runtime that silently
/// serves half its store is indistinguishable, from the outside, from a runtime
/// whose store really only holds half. This signal is the difference.
///
/// It is deliberately NOT an error return: `RuntimeCore::new` cannot refuse to
/// construct, and a store legitimately holds generations whose lens has not
/// arrived yet. The contract is "loud and queryable", not "fatal".
#[test]
fn a_runtime_reports_the_store_generations_it_cannot_enumerate() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let path = dir.path().join("collapsed-universe.sqlite");
    let (storage, _row_id, _generation_a_branch) = two_generation_store(&path);

    let cold = runtime_cold_boot(docs_schema_v2(), storage);
    let unknown = cold.unknown_store_schema_generations().to_vec();
    assert!(
        !unknown.is_empty(),
        "a runtime whose universe is {:?} while its store holds visible rows under \
         two generations must report the generations it cannot enumerate — otherwise \
         a binding that forgets to read its catalogue looks exactly like a healthy one",
        branch_universe(&cold)
    );

    // The control: the same store, constructed the way every other binding
    // constructs it, must report nothing. Without this the assertion above
    // would be satisfied by a signal that is simply always on.
    let storage = cold.into_storage();
    let warm = runtime_with_rehydrate(docs_schema_v2(), storage);
    assert!(
        warm.unknown_store_schema_generations().is_empty(),
        "a runtime that read its own catalogue must report nothing unknown; it reported {:?}",
        warm.unknown_store_schema_generations()
    );
}

/// Every value the runtime serves for one `docs` row.
fn docs_row_values<S: Storage>(
    core: &mut RuntimeCore<S, NoopScheduler>,
    row_id: ObjectId,
) -> Vec<Value> {
    let future = core.query(QueryBuilder::new("docs").build(), None);
    futures::executor::block_on(future)
        .expect("the docs query should resolve")
        .into_iter()
        .find(|(id, _)| *id == row_id)
        .map(|(_, values)| values)
        .unwrap_or_default()
}

/// How a row that exists on more than one generation's branch is resolved:
/// newest wins, with NO preference for the current generation.
///
/// Pinned because it is a conflict-resolution RULE, not an implementation
/// detail, and because it only became reachable on the rpc-server when the
/// universe widened back to two branches. `load_best_visible_row_batch_from_storage_with_locator`
/// (query_manager/manager.rs:2749) probes every branch — no early exit is
/// possible, it is a max-reduction — and keeps the head with the greatest
/// `(updated_at, batch_id)` (:2809-2817). An exact tie goes to the first branch,
/// which `all_branch_names` guarantees is the current one (context.rs:212).
///
/// The consequence worth knowing before shipping the widened universe: a larger
/// `updated_at` on the OLD branch shadows a newer current-branch write.
/// Timestamps come from the writer, so clock skew between nodes or an explicit
/// `WriteContext::with_updated_at` produces exactly that. Both halves below are
/// asserted, so if the current branch ever starts winning on its own, that is a
/// deliberate semantic change and this is where to start reading.
#[test]
fn a_row_on_two_generations_resolves_newest_first_with_no_branch_preference() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let path = dir.path().join("cross-branch-order.sqlite");
    let (storage, row_id, generation_a_branch) = two_generation_store(&path);

    let mut core = runtime_with_rehydrate(docs_schema_v2(), storage);
    let alice = Session::new("alice");

    // A copy-on-write update lands on the CURRENT branch and is newer, so it
    // wins. This is the ordinary post-migration shape.
    // No explicit timestamp: the ordinary path stamps the wall clock, which is
    // necessarily later than the fixture's insert.
    core.update(
        row_id,
        vec![(
            "body".to_string(),
            Value::Text("from-generation-b".to_string()),
        )],
        Some(&WriteContext::from_session(alice.clone())),
    )
    .expect("the update should land on the current branch");
    core.batched_tick();
    core.immediate_tick();
    assert!(
        docs_row_values(&mut core, row_id).contains(&Value::Text("from-generation-b".to_string())),
        "a newer copy on the current branch must win over the generation-A original"
    );

    // Now the same row written on the OLD branch with a later timestamp. Before
    // the universe widened this was impossible — `resolve_target_branch`
    // rejected the old generation with UnknownSchema (`resolve_target_branch`).
    core.update(
        row_id,
        vec![(
            "body".to_string(),
            Value::Text("from-generation-a-later".to_string()),
        )],
        Some(
            // Timestamps are MICROseconds — measured on this fixture, the
            // fresh current-branch head carried 1.79e15. 9e15 is genuinely
            // ahead of it; the point is the ORDER, and a skewed peer or an
            // explicit stamp produces exactly this.
            &WriteContext::from_session(alice)
                .with_updated_at(9_000_000_000_000_000)
                .with_target_branch_name(generation_a_branch.clone()),
        ),
    )
    .expect("writing to the older generation's branch is legal once it is in the universe");
    core.batched_tick();
    core.immediate_tick();
    assert!(
        docs_row_values(&mut core, row_id)
            .contains(&Value::Text("from-generation-a-later".to_string())),
        "resolution is by (updated_at, batch_id) across the whole universe with no branch \
         precedence: a later write on {generation_a_branch} shadows the current branch's copy"
    );

    // And the write really did land on the older branch. Without this, the gate
    // would stay green if `resolve_target_branch` ever fell back to the current
    // branch for an unknown target instead of erroring: the assertion above
    // would be satisfied by a write that never crossed generations, and this
    // test would quietly stop testing the thing it is named for.
    let current_branch = branch_universe(&core)
        .first()
        .cloned()
        .expect("the current branch is always first");
    let on_older = core
        .storage()
        .load_visible_region_row("docs", &generation_a_branch, row_id)
        .expect("visible read should succeed")
        .expect("generation A must still hold a head for this row");
    let on_current = core
        .storage()
        .load_visible_region_row("docs", &current_branch, row_id)
        .expect("visible read should succeed")
        .expect("the current branch holds the copy-on-write copy");
    assert_eq!(
        on_older.updated_at, 9_000_000_000_000_000,
        "the targeted write must have landed on {generation_a_branch}, not on the current branch"
    );
    assert!(
        on_current.updated_at < on_older.updated_at,
        "and the current branch's copy must be the older of the two, or the assertion above \
         proved nothing about cross-branch ordering"
    );
}
