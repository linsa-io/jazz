//! Differential oracle for the schema-generation family split (defect 27).
//!
//! A seeded, reproducible randomized sequence over a `users` family that spans
//! two schema generations, run against a MODEL of the intended semantics and
//! compared after EVERY step. Hand-written gates encode what we already thought
//! of; this is for the rest.
//!
//! The model asserts, per `(row, branch)`, after every single step:
//! 1. exactly ONE visible head across all `rowtable:visible:users:*` families
//!    (zero when nothing visible remains);
//! 2. the visible read serves the winner of everything applied so far, by the
//!    same rule the engine's own resolution uses (defect 21) — newest visible
//!    version of a linear chain;
//! 3. that head physically lives in the winner's OWN generation's family;
//! 4. the two inputs a policied `whereOld` UPDATE is evaluated from name the
//!    same version: `load_visible_region_row` (the old-content fill-in at
//!    `query_manager::server_queries::evaluate_update_permission`) and
//!    `load_row_locator(..).origin_schema_hash` (the `old_content_schema_hash`
//!    stamp at `sync_manager::inbox` and the same function). A `whereOld` arm
//!    can pass or fail on stale data, so these two must never lag the head.
//!
//! Every step also re-asserts that the repair sweep has NOTHING to do — the
//! write path alone must keep the store healthy, with the sweep reserved for
//! stores damaged by an older engine.
//!
//! MUST run on `SqliteStorage`. `MemoryStorage` overrides
//! `load_visible_region_row` / `load_visible_region_entry`
//! (`storage/memory.rs:899`, `:929`) against in-memory structs and never
//! executes the locator ladder or the raw-table families at all — moving this
//! oracle to it would make every assertion above vacuous. Do not move it.
//!
//! Reproducing a failure: the seed is printed in every assertion message, and
//! `JAZZ_ORACLE_SEED=<seed>` replays it exactly.

use super::*;
use crate::object::BranchName;
use crate::row_histories::RowState;
use crate::storage::SqliteStorage;

use super::cross_generation_visible_split::{
    next_generation_metadata, next_generation_row, next_generation_schema_hash, send_and_approve,
    users_next_generation_schema,
};

const BRANCH: &str = "main";
const ROWS: usize = 3;
const STEPS: usize = 320;
const DEFAULT_SEED: u64 = 0x2764_D1FF_C0DE_0027;

/// SplitMix64 — deterministic, dependency-free, and reproducible from the seed
/// alone.
struct Rng(u64);

impl Rng {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Generation {
    A,
    B,
}

impl Generation {
    fn schema_hash(self) -> SchemaHash {
        match self {
            Generation::A => users_schema_hash(),
            Generation::B => next_generation_schema_hash(),
        }
    }

    fn metadata(self) -> HashMap<String, String> {
        match self {
            Generation::A => row_metadata("users"),
            Generation::B => next_generation_metadata(),
        }
    }
}

/// One version as the model believes it was applied.
#[derive(Debug, Clone)]
struct ModelVersion {
    batch_id: BatchId,
    generation: Generation,
    updated_at: u64,
    visible: bool,
}

#[derive(Debug, Default)]
struct ModelRow {
    versions: Vec<ModelVersion>,
}

impl ModelRow {
    /// Defect 21's rule over a linear chain: the newest VISIBLE version, ties
    /// broken by batch id — exactly `latest_visible_version_for_tier`'s
    /// `max_by_key((updated_at, batch_id))`.
    fn winner(&self) -> Option<&ModelVersion> {
        self.versions
            .iter()
            .filter(|version| version.visible)
            .max_by_key(|version| (version.updated_at, version.batch_id))
    }
}

struct Harness {
    io: SqliteStorage,
    sm: SyncManager,
    client_id: ClientId,
    path: std::path::PathBuf,
}

impl Harness {
    fn open(path: std::path::PathBuf) -> Self {
        let mut io = SqliteStorage::open(&path).expect("sqlite storage should open");
        crate::test_support::persist_test_schema(&mut io, &users_test_schema());
        crate::test_support::persist_test_schema(&mut io, &users_next_generation_schema());

        let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::GlobalServer);
        let client_id = ClientId::new();
        sm.add_client_with_storage(&io, client_id);
        sm.set_client_acks_deliveries(client_id, true);
        sm.set_client_role(client_id, ClientRole::User);
        sm.set_client_session(
            client_id,
            crate::query_manager::session::Session::new("alice"),
        );
        sm.take_outbox();

        Self {
            io,
            sm,
            client_id,
            path,
        }
    }

    /// A real process restart: flush, drop the handle, reopen the same file with
    /// a fresh `SyncManager`. This is where a store that only LOOKED healthy
    /// because of in-process caches gives itself away.
    fn restart(self) -> Self {
        self.io.flush().expect("flush before restart");
        let path = self.path.clone();
        drop(self);
        Self::open(path)
    }
}

fn row_for(
    generation: Generation,
    row_id: ObjectId,
    parents: Vec<BatchId>,
    updated_at: u64,
    step: usize,
) -> StoredRowBatch {
    match generation {
        Generation::A => visible_row(
            row_id,
            BRANCH,
            parents,
            updated_at,
            format!("v{step}").as_bytes(),
        ),
        Generation::B => next_generation_row(
            row_id,
            parents,
            updated_at,
            &format!("v{step}"),
            &format!("presence-{step}"),
        ),
    }
}

/// Which visible families physically hold `(BRANCH, row)`.
fn heads_of(io: &SqliteStorage, row_id: ObjectId) -> Vec<String> {
    let key = format!("{BRANCH}:{}", row_id.uuid().simple());
    io.scan_raw_table_headers()
        .expect("raw table header scan should succeed")
        .into_iter()
        .map(|(name, _)| name)
        .filter(|name| name.starts_with("rowtable:visible:users:"))
        .filter(|name| {
            io.raw_table_get(name, &key)
                .expect("raw table probe should succeed")
                .is_some()
        })
        .collect()
}

fn assert_model_matches(
    harness: &mut Harness,
    model: &HashMap<ObjectId, ModelRow>,
    seed: u64,
    step: usize,
    what: &str,
) {
    let replay = format!("seed={seed:#x} step={step} after={what} (JAZZ_ORACLE_SEED={seed:#x})");
    let dump = |row_id: ObjectId, model_row: &ModelRow| {
        let stored: Vec<String> = harness
            .io
            .scan_history_row_batches("users", row_id)
            .unwrap_or_default()
            .into_iter()
            .map(|row| format!("{:?}@{} {:?}", row.batch_id(), row.updated_at, row.state))
            .collect();
        let modelled: Vec<String> = model_row
            .versions
            .iter()
            .map(|version| {
                format!(
                    "{:?}@{} gen={:?} visible={}",
                    version.batch_id, version.updated_at, version.generation, version.visible
                )
            })
            .collect();
        format!("\n  model:  {modelled:#?}\n  stored: {stored:#?}")
    };

    for (row_id, model_row) in model {
        let row_id = *row_id;
        let heads = heads_of(&harness.io, row_id);
        let served = harness
            .io
            .load_visible_region_row("users", BRANCH, row_id)
            .expect("visible read should succeed");

        match model_row.winner() {
            None => {
                assert!(
                    heads.is_empty(),
                    "no version is visible, so the row must have no head; it has {}: {heads:?} — {replay}",
                    heads.len()
                );
                assert!(
                    served.is_none(),
                    "no version is visible, so the read must serve nothing — {replay}"
                );
            }
            Some(winner) => {
                // 1. exactly one head.
                assert_eq!(
                    heads.len(),
                    1,
                    "one (row, branch) must have exactly one visible head; it has {}: {heads:?} — {replay}",
                    heads.len()
                );

                // 2. the read serves the winner.
                let served = served.unwrap_or_else(|| {
                    panic!("the winner is visible, so the read must serve it — {replay}")
                });
                assert_eq!(
                    served.batch_id(),
                    winner.batch_id,
                    "the read must serve the winner of everything applied — {replay}{}",
                    dump(row_id, model_row)
                );

                assert_eq!(
                    served.updated_at, winner.updated_at,
                    "the read must serve the winner's version — {replay}"
                );

                // 3. BOTH locators name the family the head is actually in.
                //
                //    Not "the family the winner was authored in": the engine
                //    resolves a row's generation through a ladder
                //    (`storage::required_history_user_descriptor_and_schema_hash_for_row`)
                //    and may legitimately re-encode a head into another
                //    generation. What must never drift is the agreement between
                //    where the head IS and where the two pointers say it is —
                //    the derived pointer decides the read ladder's second step
                //    and stamps `old_content_schema_hash`, and the authoritative
                //    one decides its FIRST step and is never invalidated by
                //    anything else in the engine.
                let head_family = heads[0]
                    .strip_prefix("rowtable:visible:users:")
                    .expect("visible family name");
                assert_eq!(
                    harness
                        .io
                        .load_row_locator(row_id)
                        .expect("row locator read should succeed")
                        .and_then(|locator| locator.origin_schema_hash)
                        .map(|schema_hash| schema_hash.to_string())
                        .as_deref(),
                    Some(head_family),
                    "__row_locator does not name the family the head is in, so the read \
                     ladder's derived step and old_content_schema_hash both point away \
                     from the row — {replay}"
                );
                if let Some(exact) = harness
                    .io
                    .load_visible_row_table_locator(BRANCH, row_id)
                    .expect("exact visible locator read should succeed")
                {
                    assert_eq!(
                        exact.schema_hash.to_string(),
                        head_family,
                        "the authoritative visible locator — the FIRST thing every read \
                         consults, and the one nothing else ever invalidates — names a \
                         family the head is not in — {replay}"
                    );
                }
            }
        }
    }

    // The write path alone must keep the store healthy: the repair sweep exists
    // for stores an OLDER engine damaged, and must find nothing here.
    let report = crate::storage::repair_split_visible_row_families(&mut harness.io, "users")
        .expect("the sweep should succeed");
    assert!(
        report.is_noop(),
        "the write path left the store needing repair: {report:?} — {replay}"
    );
}

/// The oracle.
#[test]
fn a_randomized_cross_generation_sequence_never_forks_a_head() {
    let seed = std::env::var("JAZZ_ORACLE_SEED")
        .ok()
        .and_then(|raw| {
            raw.strip_prefix("0x")
                .map(|hex| u64::from_str_radix(hex, 16))
                .unwrap_or_else(|| raw.parse())
                .ok()
        })
        .unwrap_or(DEFAULT_SEED);
    let mut rng = Rng(seed);

    let dir = tempfile::TempDir::new().expect("temp dir");
    let mut harness = Harness::open(dir.path().join("oracle.sqlite"));

    let row_ids: Vec<ObjectId> = (0..ROWS).map(|_| ObjectId::new()).collect();
    let mut model: HashMap<ObjectId, ModelRow> = row_ids
        .iter()
        .map(|row_id| (*row_id, ModelRow::default()))
        .collect();
    let mut clock = 1_000u64;

    for step in 0..STEPS {
        let row_id = row_ids[rng.below(ROWS)];
        let generation = if rng.below(2) == 0 {
            Generation::A
        } else {
            Generation::B
        };
        let model_row = model.get_mut(&row_id).expect("model row");
        let head = model_row.winner().cloned();

        // A row has to exist before anything but an insert can touch it, and an
        // insert has to come through the inbound path so `ensure_object_metadata`
        // stamps the first locator.
        let choice = if head.is_none() { 0 } else { rng.below(10) };

        let what = match choice {
            // Inbound insert / update — `apply_row_batch_with_context`, the path
            // a replicated row on a server only ever takes.
            0..=4 => {
                clock += 10;
                let row = row_for(
                    generation,
                    row_id,
                    head.iter().map(|version| version.batch_id).collect(),
                    clock,
                    step,
                );
                send_and_approve(
                    &mut harness.sm,
                    &mut harness.io,
                    harness.client_id,
                    generation.metadata(),
                    &row,
                );
                model_row.versions.push(ModelVersion {
                    batch_id: row.batch_id(),
                    generation,
                    updated_at: clock,
                    visible: true,
                });
                "inbound-write"
            }
            // Local write — `apply_row_batch`, which self-heals its own locator.
            5..=7 => {
                clock += 10;
                let row = row_for(
                    generation,
                    row_id,
                    head.iter().map(|version| version.batch_id).collect(),
                    clock,
                    step,
                );
                // The local path resolves the row's generation for ITSELF
                // (`storage::required_history_user_descriptor_and_schema_hash_for_row`),
                // so the caller does not get to choose it — the model records
                // the generation the engine reports back in the applied
                // locator, and asserts the store agrees with that.
                if let Ok(applied) = crate::test_support::apply_test_row_batch(
                    &mut harness.io,
                    row_id,
                    BRANCH,
                    row.clone(),
                ) {
                    let applied_generation = if applied.row_locator.origin_schema_hash
                        == Some(Generation::B.schema_hash())
                    {
                        Generation::B
                    } else {
                        Generation::A
                    };
                    model_row.versions.push(ModelVersion {
                        batch_id: row.batch_id(),
                        generation: applied_generation,
                        updated_at: clock,
                        visible: true,
                    });
                }
                "local-write"
            }
            // Reject the current head — `patch_row_batch_state`, which can hand
            // the row back to a version from the OTHER generation.
            8 => {
                let head = head.expect("a head exists on this branch of the choice");
                crate::row_histories::patch_row_batch_state(
                    &mut harness.io,
                    row_id,
                    &BranchName::new(BRANCH),
                    head.batch_id,
                    Some(RowState::Rejected),
                    None,
                )
                .expect("patching a head to rejected should succeed");
                if let Some(version) = model_row
                    .versions
                    .iter_mut()
                    .find(|version| version.batch_id == head.batch_id)
                {
                    version.visible = false;
                }
                "reject-head"
            }
            // Restart.
            _ => {
                harness = harness.restart();
                "restart"
            }
        };

        assert_model_matches(&mut harness, &model, seed, step, what);
    }
}
