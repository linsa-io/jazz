//! ADVERSARIAL REVIEW gates for the defect-27 fix. Added by the reviewer, not the
//! implementer. Each test here is an attempt to BREAK the fix, not to bless it.
//!
//! Must run on `SqliteStorage` for the same reason the implementer's gates do:
//! `MemoryStorage` never executes the raw-table family ladder at all.

use super::*;
use crate::storage::SqliteStorage;

use super::cross_generation_visible_split::{
    next_generation_metadata, next_generation_row, send_and_approve, users_next_generation_schema,
};

fn split_capable_storage() -> SqliteStorage {
    SqliteStorage::open(":memory:").expect("in-memory sqlite storage should open")
}

fn visible_users_families<H: Storage>(io: &H) -> Vec<String> {
    io.scan_raw_table_headers()
        .expect("raw table header scan should succeed")
        .into_iter()
        .map(|(name, _)| name)
        .filter(|name| name.starts_with("rowtable:visible:users:"))
        .collect()
}

fn heads_for<H: Storage>(io: &H, branch: &str, row_id: ObjectId) -> Vec<String> {
    let key = format!("{branch}:{}", row_id.uuid().simple());
    visible_users_families(io)
        .into_iter()
        .filter(|raw_table| {
            io.raw_table_get(raw_table, &key)
                .expect("raw table probe should succeed")
                .is_some()
        })
        .collect()
}

/// A harness whose store is genuinely split across two generations, in the
/// HARSHEST of the shapes an engine older than this fix can leave behind: two
/// heads, `__row_locator` rewound to the fossil, and NO authoritative
/// `__visible_row_table_locator` at all.
///
/// Deliberately not the production shape. Measured on the live sync server
/// (2026-08-16, `live_incident_store_pointers`), the authoritative locator is
/// PRESENT and names the live family — see
/// `cross_generation_visible_split::reseed_the_measured_production_split`, which
/// is the gate that pins what production actually holds. With that pointer intact
/// the read-ladder inversion alone serves the row correctly; clearing it here
/// removes that safety net so the delete, scan and sweep paths have to stand on
/// measurement alone. Read the assertions below as "even with every pointer
/// lying", never as "this is what the incident store looks like".
struct SplitStore {
    io: SqliteStorage,
    row_id: ObjectId,
    stale_family: String,
    stale_schema_hash: SchemaHash,
}

fn build_split_store() -> SplitStore {
    let mut io = split_capable_storage();
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

    let row_id = ObjectId::new();
    let generation_a = visible_row(row_id, "main", Vec::new(), 1_000, b"before");
    send_and_approve(
        &mut sm,
        &mut io,
        client_id,
        row_metadata("users"),
        &generation_a,
    );

    let key = format!("main:{}", row_id.uuid().simple());
    let stale_family = heads_for(&io, "main", row_id)
        .into_iter()
        .next()
        .expect("generation A must hold the head");
    let stale_bytes = io
        .raw_table_get(&stale_family, &key)
        .expect("raw table probe should succeed")
        .expect("generation A head bytes");
    let stale_schema_hash = users_schema_hash();

    let generation_b = next_generation_row(
        row_id,
        vec![generation_a.batch_id],
        2_000,
        "after",
        "online",
    );
    send_and_approve(
        &mut sm,
        &mut io,
        client_id,
        next_generation_metadata(),
        &generation_b,
    );

    // Re-create the pre-fix damage: generation A's head is put back, the derived
    // locator is rewound to generation A, and the authoritative pointer cleared.
    io.raw_table_put(&stale_family, &key, &stale_bytes)
        .expect("re-seeding the stale head should succeed");
    io.put_row_locator(
        row_id,
        Some(&crate::storage::RowLocator {
            table: "users".into(),
            origin_schema_hash: Some(stale_schema_hash),
        }),
    )
    .expect("rewinding the row locator should succeed");
    io.put_visible_row_table_locator("main", row_id, None)
        .expect("clearing the exact visible locator should succeed");

    assert_eq!(
        heads_for(&io, "main", row_id).len(),
        2,
        "the harness must actually produce a split store"
    );

    SplitStore {
        io,
        row_id,
        stale_family,
        stale_schema_hash,
    }
}

// ---------------------------------------------------------------------------
// ATTACK 7 — the delete path on a store that is still split.
// ---------------------------------------------------------------------------

/// `Storage::delete_visible_region_row` USED TO resolve one family through a
/// single locator (`exact_visible_row_table_locator_for_delete`, since removed)
/// and delete there, then clear the authoritative visible locator. On a store that is still split — which is
/// every store between an older engine's damage and the startup sweep, and every
/// row the sweep reports as `unresolved` — that removes ONE of the two heads and
/// leaves the other.
///
/// The authoritative pointer is now cleared, so the read ladder falls through to
/// the DERIVED `__row_locator`, which still names the surviving fossil family.
/// The deleted row keeps being served, and the repair sweep will never revisit
/// it because a row with one remaining head is not "split" any more.
#[test]
fn a_delete_on_a_still_split_row_removes_every_head() {
    let SplitStore { mut io, row_id, .. } = build_split_store();

    io.delete_visible_region_row("users", "main", row_id)
        .expect("deleting the row should succeed");

    // The sharp form first: is the DELETED row still served?
    let served_after_delete = io
        .load_visible_region_row("users", "main", row_id)
        .expect("visible read should succeed");
    assert_eq!(
        served_after_delete.map(|row| row.batch_id()),
        None,
        "RESURRECTION: a deleted row is still served, out of the family the delete did \
         not reach; surviving heads: {:?}",
        heads_for(&io, "main", row_id)
    );

    let heads = heads_for(&io, "main", row_id);
    assert!(
        heads.is_empty(),
        "a delete must remove EVERY family's head, or the row is resurrected by the \
         read ladder's derived step; {} survived: {heads:?}",
        heads.len()
    );

    assert_eq!(
        io.load_visible_region_row("users", "main", row_id)
            .expect("visible read should succeed")
            .map(|row| row.batch_id()),
        None,
        "a deleted row must not be served from any family"
    );
}

/// The same delete, followed by the startup sweep. If the sweep cannot clean up
/// what the delete left, the resurrection is permanent.
#[test]
fn the_sweep_cannot_be_relied_on_to_finish_a_half_delete() {
    let SplitStore { mut io, row_id, .. } = build_split_store();

    io.delete_visible_region_row("users", "main", row_id)
        .expect("deleting the row should succeed");
    let after_delete = heads_for(&io, "main", row_id);

    let report = crate::storage::repair_all_split_visible_row_families(&mut io)
        .expect("the sweep should succeed");
    let after_sweep = heads_for(&io, "main", row_id);

    assert!(
        after_sweep.is_empty(),
        "after a delete and a full startup sweep the row must have no head anywhere; \
         delete left {after_delete:?}, sweep left {after_sweep:?} (report: {report:?})"
    );
    assert_eq!(
        io.load_visible_region_row("users", "main", row_id)
            .expect("visible read should succeed")
            .map(|row| row.batch_id()),
        None,
        "a deleted row must not be served after the sweep"
    );
}

// ---------------------------------------------------------------------------
// ATTACK 1 — does the SCAN path serve a split row, and does the sweep fix it?
// ---------------------------------------------------------------------------

/// `scan_visible_row_bytes_with_storage` iterates EVERY family and does not
/// dedupe by `(branch, row)`. On a split store it returns one row twice, with
/// two different contents. This is the query/scan surface, not the point-read
/// surface, so the read-ladder inversion (M3) does nothing for it.
#[test]
fn the_sibling_scan_returns_a_split_row_once() {
    let SplitStore { io, row_id, .. } = build_split_store();

    let scanned = io
        .scan_visible_region("users", "main")
        .expect("scan should succeed");
    let occurrences = scanned.iter().filter(|row| row.row_id == row_id).count();
    assert_eq!(
        occurrences, 1,
        "a full visible scan must return one (row, branch) exactly once; a split row \
         is returned {occurrences} times, so every scan-driven query sees a phantom \
         duplicate with stale content"
    );
}

/// The sweep's contract as the startup path relies on it: after it runs, the
/// scan surface must also be clean.
#[test]
fn the_sweep_cleans_the_scan_surface_too() {
    let SplitStore { mut io, row_id, .. } = build_split_store();

    crate::storage::repair_all_split_visible_row_families(&mut io)
        .expect("the sweep should succeed");

    let scanned = io
        .scan_visible_region("users", "main")
        .expect("scan should succeed");
    let occurrences = scanned.iter().filter(|row| row.row_id == row_id).count();
    assert_eq!(
        occurrences, 1,
        "after the sweep the scan must return the row exactly once"
    );
}

// ---------------------------------------------------------------------------
// ATTACK 4/9 — a row the sweep cannot resolve stays split forever.
// ---------------------------------------------------------------------------

/// The sweep's `unresolved_rows` branch: two heads and no history to arbitrate.
/// The implementer's choice is to leave the row alone. That is defensible as a
/// choice, but it means the incident symptom SURVIVES the fix for such a row,
/// so the reader must still be correct on it.
///
/// This gate is RED, and the stated reason used to be the premise. That was
/// wrong. The real cause is `build_split_store` above: it CLEARS the
/// authoritative locator, and with no authoritative pointer and the derived one
/// naming the fossil, the ladder has nothing correct left to consult. On the
/// MEASURED production shape — authoritative locator present and naming the live
/// family — the same premise passes, which
/// `round2_cross_generation_review::an_unresolvable_split_reads_correctly_on_the_measured_production_shape`
/// demonstrates.
///
/// So what stays uncovered is narrower than this gate's name suggests: a row with
/// two heads, no history AND no authoritative locator reads as the fossil. Kept
/// red-and-ignored rather than deleted because that combination is reachable on a
/// store an older engine damaged, and nothing in the fix repairs it.
#[test]
#[ignore = "RED, and not for the reason first given: this harness CLEARS the authoritative locator, so the read ladder has no correct pointer left. On the measured production shape (authoritative present) the same premise passes — see round2_cross_generation_review::an_unresolvable_split_reads_correctly_on_the_measured_production_shape. Documents the uncovered corner: two heads + no history + no authoritative locator."]
fn an_unresolvable_split_row_is_still_read_correctly() {
    let SplitStore {
        mut io,
        row_id,
        stale_family,
        stale_schema_hash,
    } = build_split_store();

    // Strip the history so the sweep has nothing to arbitrate with — the
    // `unresolved_rows` branch it explicitly reports.
    for family in io
        .scan_raw_table_headers()
        .expect("raw table header scan")
        .into_iter()
        .map(|(name, _)| name)
        .filter(|name| name.starts_with("rowtable:history:users:"))
    {
        for key in io
            .raw_table_scan_prefix_keys(&family, "")
            .expect("history scan")
        {
            io.raw_table_delete(&family, &key).expect("history delete");
        }
    }

    let report = crate::storage::repair_all_split_visible_row_families(&mut io)
        .expect("sweep should succeed");
    assert_eq!(
        report.unresolved_rows, 1,
        "the harness must actually hit the unresolved branch: {report:?}"
    );

    // The row is still split. What does a read serve?
    let served = io
        .load_visible_region_row("users", "main", row_id)
        .expect("visible read should succeed")
        .expect("the row still has heads, so something is served");
    let served_family = format!("rowtable:visible:users:{stale_schema_hash}");
    assert_ne!(
        served_family,
        stale_family,
        "a row the sweep could not repair must still not read as the stale generation; \
         served {:?}",
        served.batch_id()
    );
}

// ---------------------------------------------------------------------------
// ATTACK 9 — the cache-invalidation half of the fix is inert behind Box<dyn Storage>.
// ---------------------------------------------------------------------------

/// The diff adds `put_visible_row_table_locator` overrides to `sqlite.rs:540` and
/// `rocksdb.rs:600` for one stated reason: `apply_encoded_row_mutation` dedups
/// locator persists against `inner.visible_row_table_locators`, so a DIRECT
/// write to that pointer must evict the cache entry or a later write is skipped.
///
/// `impl<T: Storage + ?Sized> Storage for Box<T>` (`storage_trait.rs:1855`)
/// forwards 77 methods but NOT `put_visible_row_table_locator` /
/// `load_visible_row_table_locator` — so on `Box<dyn Storage>` those calls take
/// the trait DEFAULT, write the raw table, and never touch the backend's cache,
/// while `apply_encoded_row_mutation` IS forwarded and still consults it.
///
/// `Box<dyn Storage + Send>` is what the Rust sync server (`server/mod.rs:31`
/// `DynStorage`), the node binding (`jazz-napi/src/lib.rs:415`) and the web
/// binding (`jazz-wasm/src/runtime.rs:388`) all run. Only jazz-rn is concrete.
///
/// The gate: the identical sequence must leave the identical store, boxed or not.
#[test]
fn boxing_the_storage_does_not_change_where_the_authoritative_locator_points() {
    fn run<H: Storage>(io: &mut H) -> (Option<String>, Vec<String>) {
        crate::test_support::persist_test_schema(io, &users_test_schema());
        crate::test_support::persist_test_schema(io, &users_next_generation_schema());

        let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::GlobalServer);
        let client_id = ClientId::new();
        sm.add_client_with_storage(io, client_id);
        sm.set_client_acks_deliveries(client_id, true);
        sm.set_client_role(client_id, ClientRole::User);
        sm.set_client_session(
            client_id,
            crate::query_manager::session::Session::new("alice"),
        );
        sm.take_outbox();

        let row_id = ObjectId::new();
        let a1 = visible_row(row_id, "main", Vec::new(), 1_000, b"a1");
        send_and_approve(&mut sm, io, client_id, row_metadata("users"), &a1);
        // Cross into generation B: the write stamps the authoritative locator
        // AND `storage::enforce_single_visible_family_after_write` re-stamps it,
        // which is the direct write the overrides exist for.
        let b1 = next_generation_row(row_id, vec![a1.batch_id], 2_000, "b1", "online");
        send_and_approve(&mut sm, io, client_id, next_generation_metadata(), &b1);
        // Cross BACK into generation A, then forward again — each crossing goes
        // through the direct locator write and then through the dedup.
        let a2 = visible_row(row_id, "main", vec![b1.batch_id], 3_000, b"a2");
        send_and_approve(&mut sm, io, client_id, row_metadata("users"), &a2);
        let b2 = next_generation_row(row_id, vec![a2.batch_id], 4_000, "b2", "online");
        send_and_approve(&mut sm, io, client_id, next_generation_metadata(), &b2);

        let locator = io
            .load_visible_row_table_locator("main", row_id)
            .expect("exact visible locator read should succeed")
            .map(|locator| locator.schema_hash.to_string());
        (locator, heads_for(io, "main", row_id))
    }

    let mut concrete = split_capable_storage();
    let concrete_result = run(&mut concrete);

    let mut boxed: Box<dyn Storage> = Box::new(split_capable_storage());
    let boxed_result = run(&mut boxed);

    assert_eq!(
        concrete_result.1.len(),
        1,
        "control: the concrete backend must keep exactly one head"
    );
    assert_eq!(
        boxed_result.1.len(),
        1,
        "a boxed backend must keep exactly one head too"
    );
    // The head family is the ground truth both stores must agree with.
    let concrete_family = concrete_result.1[0]
        .strip_prefix("rowtable:visible:users:")
        .map(str::to_string);
    let boxed_family = boxed_result.1[0]
        .strip_prefix("rowtable:visible:users:")
        .map(str::to_string);
    assert_eq!(
        concrete_result.0, concrete_family,
        "control: on the concrete backend the authoritative locator names the head's family"
    );
    assert_eq!(
        boxed_result.0, boxed_family,
        "behind Box<dyn Storage> — what the sync server, node and web all run — the \
         authoritative locator must still name the family the head is in; the \
         cache-invalidating override is not forwarded by the Box impl"
    );
}

// ---------------------------------------------------------------------------
// MEASUREMENT — what the startup sweep costs on a production-scale store.
// ---------------------------------------------------------------------------

/// `repair_all_split_visible_row_families` runs in `RuntimeCore::new`. For any
/// table with two or more registered generations it scans the KEYS of every
/// family and accumulates them in one `BTreeMap<(String, ObjectId),
/// Vec<SchemaHash>>` before deciding anything, so both the time and the peak
/// allocation are linear in the store's visible row count.
///
/// This measures the FIRST boot after a deployment. It is no longer paid every
/// boot: a per-table marker records the generation-set a completed pass covered
/// (`storage::visible_family_sweep_is_current`), so subsequent boots cost two
/// point reads until a new generation registers. `round2_cross_generation_review::
/// startup_sweep_cost_with_the_marker_in_place` measures the steady state.
///
/// `cargo test --release -p jazz-tools --features "rocksdb sqlite test-utils" \
///  --lib startup_sweep_cost -- --ignored --nocapture`
#[cfg(feature = "rocksdb")]
#[test]
#[ignore = "measurement, not a gate"]
fn startup_sweep_cost() {
    use crate::storage::RocksDBStorage;

    for rows in [50_000usize, 200_000, 1_000_000] {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let mut io = RocksDBStorage::open(dir.path().to_str().expect("path"), 256 * 1024 * 1024)
            .expect("open");
        crate::test_support::persist_test_schema(&mut io, &users_test_schema());
        crate::test_support::persist_test_schema(&mut io, &users_next_generation_schema());

        // Register BOTH families for real, through the normal inbound path, so
        // the sweep's `families.len() < 2` early-return does not fire.
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
        let seed_a = ObjectId::new();
        let a = visible_row(seed_a, "main", Vec::new(), 1_000, b"a");
        send_and_approve(&mut sm, &mut io, client_id, row_metadata("users"), &a);
        let seed_b = ObjectId::new();
        let b = next_generation_row(seed_b, Vec::new(), 2_000, "b", "online");
        send_and_approve(&mut sm, &mut io, client_id, next_generation_metadata(), &b);

        let families = visible_users_families(&io);
        assert_eq!(
            families.len(),
            2,
            "the measurement needs two REGISTERED families or the sweep early-returns: {families:?}"
        );
        let family_a = format!("rowtable:visible:users:{}", users_schema_hash());

        // Bulk: the sweep only reads KEYS, so a one-byte value is enough to make
        // each row cost the sweep exactly what a real row costs it.
        for index in 0..rows {
            io.raw_table_put(
                &family_a,
                &format!("main:{}", ObjectId::new().uuid().simple()),
                &[1u8],
            )
            .expect("seed key");
            if index % 100_000 == 0 {
                io.flush().expect("flush");
            }
        }
        io.flush().expect("flush");

        let started = std::time::Instant::now();
        let report = crate::storage::repair_all_split_visible_row_families(&mut io).expect("sweep");
        let elapsed = started.elapsed();
        println!(
            "startup sweep: {rows} visible rows across 2 registered generations -> {elapsed:?} ({report:?})"
        );
    }
}
