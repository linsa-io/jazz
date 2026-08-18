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

/// A DELETE across a generation crossing must retire the row on every branch, not only on
/// the one it was issued from.
///
/// The update twin above establishes the shelf model: a crossing may legitimately leave the
/// old branch holding its own head, because an update changes a row's SHAPE and an old
/// reader is entitled to the old shape. Deletion is not a shape change. It changes whether
/// the row exists, and existence cannot differ per shelf — a reader still on the previous
/// generation would go on serving a row the authority has deleted, forever, with nothing
/// left to correct it.
///
/// MEASURED on the dev stack 2026-08-18. A user renamed their handle; the rpc-server
/// deleted the previous `unique_names` row and inserted the new one, and the inspector — a
/// reader that resolves each row to its newest version across generations — showed exactly
/// one row, correctly. The device showed the old handle for hours. Dumping the same row
/// from both stores:
///
/// ```text
///   server  019fbaa1…  dev-53710882d8e0-main  VisibleDirect  deleted=false   (1 Aug)
///           019fbaa1…  dev-b32dae47bbd9-main  deleted=true   kind=Soft       (the rename)
///   device  019fbaa1…  dev-53710882d8e0-main  VisibleDirect  deleted=false
/// ```
///
/// The deletion was recorded as a new version on the authority's OWN generation while the
/// row's live head sat on the previous one. The device held only that head, never received
/// a deleted version for it, and so kept the row — and the app's own query returned BOTH
/// handles, `["timer","timer3"]`, straight out of its backref.
// What a diverged PEER depends on. Read-side resolution is tombstone-dominant, so this
// store already answers queries correctly; the representation is what reaches everyone
// else. Fan-out is keyed on `(row, branch)`, so a client whose scope names the previous
// generation is never told anything and per-branch backfill re-serves the live head —
// only a tombstone authored on that branch reaches it.
#[test]
fn a_delete_across_a_generation_crossing_retires_the_row_on_every_branch() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let path = dir.path().join("delete.sqlite");
    let storage = SqliteStorage::open(&path).expect("sqlite storage should open");

    let mut core = runtime_over(docs_schema_v1(), "defect27-delete", storage);
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

    let branch_before = crate::storage::sole_branch_name(core.storage())
        .expect("branch registry readable")
        .expect("the seeded row registered a branch");
    assert!(
        core.storage()
            .load_visible_region_row("docs", branch_before.as_str(), row_id)
            .expect("visible row readable")
            .is_some(),
        "fixture precondition: the row must be visible on generation A before the crossing"
    );

    // The crossing: same store, rehydrated under a schema that hashes differently.
    let storage = core.into_storage();
    let mut core = runtime_over(docs_schema_v2(), "defect27-delete", storage);

    core.delete(row_id, Some(&alice))
        .expect("the owner's delete across the crossing applies");
    core.batched_tick();
    core.immediate_tick();

    // Every branch that still serves this row, and whether it serves it as LIVE.
    let families = visible_docs_families(core.storage());
    assert_eq!(
        families.len(),
        2,
        "the deployment must have created a second docs family, or the crossing did not \
         happen and this gate proves nothing: {families:?}"
    );

    // Enumerated from the visible families themselves, NOT from the branch-ord registry.
    // Ords are allocated only by sealed-batch persistence, never by history application
    // (`storage/mod.rs` says so where `sole_branch_name` is defined), so a device that
    // received a generation's rows purely by inbound sync has no ord for that branch — and
    // that is the production shape this gate exists for. An ord walk would have gone green
    // on the very store the defect was measured on.
    let mut live_on: Vec<String> = Vec::new();
    for family in &families {
        for key in core
            .storage()
            .raw_table_scan_prefix_keys(family, "")
            .expect("raw table scan should succeed")
        {
            // `<branch>:<row-uuid-hex>` is the visible raw-table key layout, the same one
            // `cross_generation_visible_split` reads by hand.
            let Some((branch, keyed_row_hex)) = key.rsplit_once(':') else {
                continue;
            };
            if keyed_row_hex != row_id.uuid().simple().to_string() {
                continue;
            }
            let branch = branch.to_string();
            let head = core
                .storage()
                .load_visible_region_row("docs", branch.as_str(), row_id)
                .expect("visible row readable");
            if head.is_some_and(|row| !row.is_deleted) {
                live_on.push(branch);
            }
        }
    }
    live_on.sort();
    live_on.dedup();

    eprintln!("branches still serving the deleted row as live: {live_on:?}");
    assert!(
        live_on.is_empty(),
        "a delete must retire the row on EVERY branch, not only the one it was issued \
         from — it still reads as live on {live_on:?}. A reader left on the previous \
         generation goes on serving a row the authority deleted, and nothing later \
         corrects it: the deletion was recorded on the authority's own generation, so no \
         deleted version for the reader's branch ever exists to be delivered."
    );
}

/// A delete must not lose to a clock.
///
/// Cross-generation resolution picks a row's newest version by `(updated_at, batch_id)`
/// across branches (`query_manager/manager.rs`, `load_best_visible_row_batch_*`). Within one
/// generation that is harmless: the delete is a child of the row's chain on that branch, so
/// causality already orders it. Across a crossing the delete is authored as a PARENTLESS
/// root on the authority's own generation — the chain is gone, and a wall-clock comparison
/// between two heads that never saw each other is all that remains.
///
/// `updated_at` is per-node `SystemTime::now()` with a local monotonic bump
/// (`sync_manager/clock.rs`), and any caller may set it outright via
/// `WriteContext::with_updated_at`. One second of skew between the node that inserted and
/// the node that deleted is therefore enough to make a successful delete lose, on the
/// authority's own store, permanently.
///
/// The engine already knows this reasoning is wrong where it repairs split families:
/// `repair_split_visible_row_families` refuses to compare heads by timestamp, and says why
/// — "comparing the heads is exactly the reasoning that made the stale one look
/// defensible." The live read path does that comparison anyway.
///
/// Deletion is monotone. A live head is never evidence against a tombstone, whatever its
/// clock says.
#[test]
fn a_delete_across_a_generation_crossing_outranks_a_newer_looking_live_head() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let path = dir.path().join("skew.sqlite");
    let storage = SqliteStorage::open(&path).expect("sqlite storage should open");

    let mut core = runtime_over(docs_schema_v1(), "defect27-skew", storage);

    // The insert is stamped LATER than the delete will be — one node's clock ahead of the
    // other's, which is the only thing this fixture does differently.
    let inserting_node =
        WriteContext::from_session(Session::new("alice")).with_updated_at(9_000_000);
    let ((row_id, _), _) = insert_and_wait_for_batch(
        &mut core,
        "docs",
        HashMap::from([
            ("owner".to_string(), Value::Text("alice".to_string())),
            ("body".to_string(), Value::Text("draft".to_string())),
        ]),
        Some(&inserting_node),
        DurabilityTier::Local,
    )
    .expect("the row inserts under generation A");
    core.batched_tick();
    core.immediate_tick();

    let storage = core.into_storage();
    let mut core = runtime_over(docs_schema_v2(), "defect27-skew", storage);

    let deleting_node =
        WriteContext::from_session(Session::new("alice")).with_updated_at(8_000_000);
    core.delete(row_id, Some(&deleting_node))
        .expect("the delete across the crossing applies");
    core.batched_tick();
    core.immediate_tick();

    let query = core
        .schema_manager_mut()
        .query_manager_mut()
        .query("docs")
        .build();
    let rows = {
        let waker = noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);
        let mut future = core.query_with_propagation(
            query,
            None,
            ReadDurabilityOptions::default(),
            crate::sync_manager::QueryPropagation::Full,
        );
        match Pin::new(&mut future).poll(&mut cx) {
            Poll::Ready(Ok(results)) => results,
            Poll::Ready(Err(err)) => panic!("query should succeed: {err:?}"),
            Poll::Pending => panic!("query should resolve immediately"),
        }
    };

    eprintln!(
        "rows a deleted-but-older-stamped row still returns: {}",
        rows.len()
    );
    assert!(
        rows.is_empty(),
        "a delete must outrank a live head that merely carries a newer clock. Deletion is \
         monotone, and across a generation crossing the delete has no causal link to the \
         head it retires — so a timestamp comparison is the only thing deciding, and one \
         second of skew between two nodes silently resurrects the row for good. Got {} \
         rows.",
        rows.len()
    );
}
