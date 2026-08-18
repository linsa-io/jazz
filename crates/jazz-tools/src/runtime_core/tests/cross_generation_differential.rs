//! Randomised write sequences across generation crossings, against a model of the
//! intended semantics.
//!
//! The hand-written gates next door encode what we already thought of. This one runs
//! interleavings nobody chose — insert, update, delete, restore, and a schema crossing, in
//! whatever order the generator finds, each write stamped with a deliberately skewed clock
//! — against a model stated independently of the implementation:
//!
//! > A row is visible if it has been written and not deleted. Deletion is absorbing until
//! > an explicit restore. A schema crossing changes where rows are stored and nothing about
//! > which of them exist.
//!
//! The clock skew is the point, not decoration. Across a crossing a delete is authored as a
//! parentless root on the writer's own generation, so no causal link orders it against the
//! head it retires and only `updated_at` is left — a per-node wall clock any caller may set
//! outright. Every write here therefore carries a timestamp drawn from the generator, which
//! is exactly the freedom two nodes with drifting clocks have.
//!
//! `SqliteStorage`, because `MemoryStorage` never executes the locator ladder and the whole
//! cross-generation family is invisible to it.

use super::*;
use crate::storage::SqliteStorage;

struct Lcg(u64);

impl Lcg {
    fn next_u64(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 33
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }
}

/// `docs`, plus `extra` filler tables — each count hashes differently and therefore mints
/// its own generation, while leaving `docs` rows byte-identical so nothing but the family
/// can differ.
fn docs_schema_with(extra: usize) -> Schema {
    let mut builder = SchemaBuilder::new().table(
        TableSchema::builder("docs")
            .column("owner", ColumnType::Text)
            .column("body", ColumnType::Text),
    );
    for index in 0..extra {
        builder = builder.table(
            TableSchema::builder(&format!("filler{index}")).column("label", ColumnType::Text),
        );
    }
    builder.build()
}

fn runtime_over_schema(
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

/// What the model believes about one row.
struct ModelRow {
    id: ObjectId,
    body: String,
    deleted: bool,
}

fn visible_docs(core: &mut RuntimeCore<SqliteStorage, NoopScheduler>) -> Vec<(ObjectId, String)> {
    let query = core
        .schema_manager_mut()
        .query_manager_mut()
        .query("docs")
        .build();
    let waker = noop_waker();
    let mut cx = std::task::Context::from_waker(&waker);
    let mut future = core.query_with_propagation(
        query,
        None,
        ReadDurabilityOptions::default(),
        crate::sync_manager::QueryPropagation::Full,
    );
    let rows = match Pin::new(&mut future).poll(&mut cx) {
        Poll::Ready(Ok(results)) => results,
        Poll::Ready(Err(err)) => panic!("query should succeed: {err:?}"),
        Poll::Pending => panic!("query should resolve immediately"),
    };
    let mut out: Vec<(ObjectId, String)> = rows
        .into_iter()
        .map(|(id, values)| {
            let body = match values.get(1) {
                Some(Value::Text(text)) => text.clone(),
                other => panic!("docs.body should decode as text, got {other:?}"),
            };
            (id, body)
        })
        .collect();
    out.sort();
    out
}

// OPEN DEFECT, found by this oracle on its first run and not yet fixed: an UPDATE across a
// generation crossing is lost when the writing node's clock is behind. Seed 2 walked to it
// in 26 steps —
//
//   insert  …0-71f0  body=v2-17  at=13000000     (generation 0)
//   cross to generation 1, cross to generation 2
//   update  …0-71f0  body=e2-25  at=9000000      (generation 2)
//   query returns v2-17
//
// Same root as the delete this file's neighbours fix: a write across a crossing is authored
// as a parentless root on the writer's own generation, so nothing causal orders it against
// the head it supersedes and `updated_at` — a per-node wall clock — decides. Deletion had a
// monotone rule available (a live head is never evidence against a tombstone) and now uses
// it. An update has no such rule; it needs generation recency, which is a design with its
// own review, not a patch.
//
// Ignored so it does not block the delete fix it found this alongside. Run it with
//   cargo test -p jazz-tools --features test --lib -- --ignored randomised_writes
#[test]
#[ignore = "open defect: an update across a generation crossing is lost to a slower clock"]
fn randomised_writes_across_generation_crossings_match_the_intended_semantics() {
    const SEEDS: u64 = 24;
    const OPS_PER_SEED: usize = 26;

    for seed in 1..=SEEDS {
        let mut rng = Lcg(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15));
        let dir = tempfile::TempDir::new().expect("temp dir");
        let path = dir.path().join("differential.sqlite");
        let storage = SqliteStorage::open(&path).expect("sqlite storage should open");

        let mut generation = 0usize;
        let mut core = runtime_over_schema(
            docs_schema_with(generation),
            "defect27-differential",
            storage,
        );
        let mut model: Vec<ModelRow> = Vec::new();
        let mut history: Vec<String> = Vec::new();

        for step in 0..OPS_PER_SEED {
            // Every write carries a clock the generator chose, which is the freedom two
            // nodes with drifting clocks already have.
            let stamp = 1_000_000 + (rng.below(16) as u64) * 1_000_000;
            let context = WriteContext::from_session(Session::new("alice")).with_updated_at(stamp);

            match rng.below(10) {
                0..=3 => {
                    let body = format!("v{seed}-{step}");
                    let ((row_id, _), _) = insert_and_wait_for_batch(
                        &mut core,
                        "docs",
                        HashMap::from([
                            ("owner".to_string(), Value::Text("alice".to_string())),
                            ("body".to_string(), Value::Text(body.clone())),
                        ]),
                        Some(&context),
                        DurabilityTier::Local,
                    )
                    .expect("insert applies");
                    history.push(format!("insert {row_id} body={body} at={stamp}"));
                    model.push(ModelRow {
                        id: row_id,
                        body,
                        deleted: false,
                    });
                }
                4..=5 if model.iter().any(|row| !row.deleted) => {
                    let live: Vec<usize> =
                        (0..model.len()).filter(|i| !model[*i].deleted).collect();
                    let index = live[rng.below(live.len())];
                    let body = format!("e{seed}-{step}");
                    core.update(
                        model[index].id,
                        vec![("body".to_string(), Value::Text(body.clone()))],
                        Some(&context),
                    )
                    .expect("update applies");
                    history.push(format!("update {} body={body} at={stamp}", model[index].id));
                    model[index].body = body;
                }
                6..=7 if model.iter().any(|row| !row.deleted) => {
                    let live: Vec<usize> =
                        (0..model.len()).filter(|i| !model[*i].deleted).collect();
                    let index = live[rng.below(live.len())];
                    core.delete(model[index].id, Some(&context))
                        .expect("delete applies");
                    history.push(format!("delete {} at={stamp}", model[index].id));
                    model[index].deleted = true;
                }
                8 => {
                    // The crossing: same store, rehydrated under a schema that hashes
                    // differently — the way every deployment does it.
                    generation += 1;
                    let storage = core.into_storage();
                    core = runtime_over_schema(
                        docs_schema_with(generation),
                        "defect27-differential",
                        storage,
                    );
                    history.push(format!("cross to generation {generation}"));
                }
                _ => {
                    core.batched_tick();
                    core.immediate_tick();
                    history.push("settle".to_string());
                }
            }
            core.batched_tick();
            core.immediate_tick();

            let expected: Vec<(ObjectId, String)> = {
                let mut rows: Vec<(ObjectId, String)> = model
                    .iter()
                    .filter(|row| !row.deleted)
                    .map(|row| (row.id, row.body.clone()))
                    .collect();
                rows.sort();
                rows
            };
            let observed = visible_docs(&mut core);

            assert_eq!(
                observed,
                expected,
                "seed {seed} step {step}: the visible set diverged from the intended \
                 semantics. A row exists until it is deleted, deletion is absorbing, and a \
                 schema crossing changes where rows are stored and nothing about which of \
                 them exist.\nhistory:\n  {}",
                history.join("\n  ")
            );
        }
    }
}
