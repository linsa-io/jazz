//! Defect-27 invariant gate for the LOCAL write path across a schema-generation
//! crossing.
//!
//! The inbound sync path is the surface the split was measured on, but it is not
//! the only writer that could fork a head. `QueryManager`'s local CRUD path
//! (`query_manager::writes::apply_local_row_history_write_with_prepared_context`,
//! reached from insert / update / soft-delete) stamps `__row_locator` at the
//! BRANCH's current schema hash itself, and a round-2 review argued that after a
//! deployment this writes a pre-existing row's head into a new family while
//! leaving the old one — the same shape as the defect, on the writer that runs
//! on every client.
//!
//! It does not, and this gate is here to keep it that way rather than to
//! demonstrate a fix. What the review's argument misses is that a generation
//! change on this path also moves the write to a NEW BRANCH: the head invariant
//! is per `(row, branch)`, and `(row, new-branch)` has no prior head anywhere to
//! fork from. The old branch keeps its own head, which is the shelf model
//! working, not a split. A candidate fix that computed the crossing honestly was
//! written, measured to change nothing anywhere in the suite, and dropped.
//!
//! So this file pins the INVARIANT, not a mechanism: whatever the local path
//! does across a crossing, one `(row, branch)` ends with one visible head, both
//! pointers name the family that head is in, and the read serves the update. If
//! a future change ever makes a same-branch generation crossing reachable, this
//! is what catches it.
//!
//! Must be `SqliteStorage`. `MemoryStorage` keeps visible entries as structs and
//! has no raw-table families at all (`storage/memory.rs:899`, `:929`), so it
//! cannot express the state this gate asserts against.

use super::*;
use crate::storage::SqliteStorage;

fn docs_schema_v1() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("docs")
                .column("owner", ColumnType::Text)
                .column("body", ColumnType::Text),
        )
        .build()
}

/// Same `docs` shape, different SCHEMA — the added table changes the schema hash
/// and therefore the `rowtable:*:docs:<hash>` family, while leaving `docs` rows
/// byte-identical so nothing but the family can differ.
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

fn runtime_over<S: Storage>(
    schema: Schema,
    app_name: &str,
    storage: S,
) -> RuntimeCore<S, NoopScheduler> {
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

fn visible_docs_families<S: Storage>(storage: &S) -> Vec<String> {
    storage
        .scan_raw_table_headers()
        .expect("raw table header scan should succeed")
        .into_iter()
        .map(|(name, _)| name)
        .filter(|name| name.starts_with("rowtable:visible:docs:"))
        .collect()
}

#[test]
fn a_local_update_across_a_generation_crossing_moves_the_head_instead_of_forking_it() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let path = dir.path().join("local.sqlite");
    let storage = SqliteStorage::open(&path).expect("sqlite storage should open");

    let mut core = runtime_over(docs_schema_v1(), "defect27-local", storage);
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

    // Precondition: generation A, one family, one head.
    assert_eq!(
        visible_docs_families(core.storage()).len(),
        1,
        "before the crossing the store must hold exactly one docs family"
    );

    // The crossing: same store, rehydrated under a schema that hashes
    // differently, the way every production construction does it.
    let storage = core.into_storage();
    let mut core = runtime_over(docs_schema_v2(), "defect27-local", storage);

    // Precondition: the deployment really did create a second family, so this
    // gate cannot pass by there being nowhere else for a head to go.
    // The write under test — a plain local update, no sync, no policy.
    core.update(
        row_id,
        vec![("body".to_string(), Value::Text("edited".to_string()))],
        Some(&alice),
    )
    .expect("the owner's local update across the crossing applies");
    core.batched_tick();
    core.immediate_tick();

    let families = visible_docs_families(core.storage());
    assert_eq!(
        families.len(),
        2,
        "the deployment must have created a second docs family, or the crossing did \
         not happen and this gate proves nothing: {families:?}"
    );

    // The invariant is per `(row, BRANCH)`. A crossing legitimately puts the row
    // on a new branch while the old branch keeps its own head — that is the
    // shelf model, not a fork. What must never happen is ONE branch's head
    // existing in two families at once.
    let row_hex = row_id.uuid().simple().to_string();
    let mut heads_by_branch: std::collections::BTreeMap<String, Vec<String>> =
        std::collections::BTreeMap::new();
    for family in &families {
        for key in core
            .storage()
            .raw_table_scan_prefix_keys(family, "")
            .expect("raw table scan should succeed")
        {
            if let Some(branch) = key.strip_suffix(&format!(":{row_hex}")) {
                heads_by_branch
                    .entry(branch.to_string())
                    .or_default()
                    .push(family.clone());
            }
        }
    }
    assert!(
        !heads_by_branch.is_empty(),
        "the row must have a visible head somewhere after the update"
    );
    for (branch, heads) in &heads_by_branch {
        assert_eq!(
            heads.len(),
            1,
            "a local update across a generation crossing must MOVE branch {branch}'s head, \
             not fork it; it left {} heads: {heads:?}",
            heads.len()
        );
    }
    let (write_branch, heads) = heads_by_branch
        .iter()
        .max_by_key(|(_, heads)| heads.len())
        .expect("at least one branch holds the row");

    // And the pointers must name the family the head is actually in, or the
    // read ladder and `old_content_schema_hash` both aim away from it.
    let head_family = heads[0]
        .strip_prefix("rowtable:visible:docs:")
        .expect("visible family name");
    assert_eq!(
        core.storage()
            .load_row_locator(row_id)
            .expect("row locator read should succeed")
            .and_then(|locator| locator.origin_schema_hash)
            .map(|schema_hash| schema_hash.to_string())
            .as_deref(),
        Some(head_family),
        "__row_locator must name the family the head is in"
    );

    let served = core
        .storage()
        .load_visible_region_row("docs", write_branch, row_id)
        .expect("visible read should succeed")
        .expect("the row must still be visible after the crossing");
    assert_eq!(
        served.data.as_ref(),
        crate::query_manager::encoding::encode_row(
            &docs_schema_v2()[&"docs".into()].columns,
            &[
                Value::Text("alice".to_string()),
                Value::Text("edited".to_string()),
            ],
        )
        .expect("generation-B row should encode")
        .as_slice(),
        "the read must serve the update, not the pre-crossing version"
    );
}
