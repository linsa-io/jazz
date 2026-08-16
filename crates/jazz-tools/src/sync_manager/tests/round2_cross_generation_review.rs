//! ROUND-2 ADVERSARIAL REVIEW probes for the defect-27 fix. Added by the round-2
//! reviewer; they exist to FALSIFY claims in the entry and the code comments, not
//! to bless them.

use super::*;
use crate::storage::SqliteStorage;

use super::cross_generation_visible_split::{
    next_generation_metadata, next_generation_row, next_generation_schema_hash, send_and_approve,
    users_next_generation_schema,
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

/// A two-generation store with one row that has crossed generations cleanly.
/// Returns the store, the row, and generation A's head bytes/family so a caller
/// can re-seed the measured production split.
fn two_generation_store() -> (SqliteStorage, ObjectId, String, Vec<u8>) {
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
        .pop()
        .expect("generation A must hold the head");
    let stale_bytes = io
        .raw_table_get(&stale_family, &key)
        .expect("probe")
        .expect("generation A head bytes");

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

    (io, row_id, stale_family, stale_bytes)
}

fn reseed_split<H: Storage>(io: &mut H, row_id: ObjectId, stale_family: &str, stale_bytes: &[u8]) {
    let key = format!("main:{}", row_id.uuid().simple());
    io.raw_table_put(stale_family, &key, stale_bytes)
        .expect("re-seeding the stale head should succeed");
    io.put_row_locator(
        row_id,
        Some(&crate::storage::RowLocator {
            table: "users".into(),
            origin_schema_hash: Some(users_schema_hash()),
        }),
    )
    .expect("rewinding the row locator should succeed");
    io.put_visible_row_table_locator(
        "main",
        row_id,
        Some(&crate::storage::visible_row_table_locator_for(
            "users",
            next_generation_schema_hash(),
        )),
    )
    .expect("stamping the authoritative locator should succeed");
}

/// ROUND-2 ATTACK: `drop_stale_visible_row_family_entry` resolves the dropped
/// head's descriptor with `load_user_descriptor_for_schema_hash`, which reads the
/// CATALOGUE and returns `Err` when the entry is absent. Every other consumer of
/// those same bytes — `resolved_user_descriptor_for_raw_table`, which the scan and
/// the read ladder use — prefers the descriptor EMBEDDED IN THE RAW TABLE HEADER
/// and only falls back to the catalogue.
///
/// So a store whose fossil generation's catalogue entry is gone still READS fine,
/// but every visible write to a row that has a head in that family now travels
/// through the chokepoint into a hard `StorageError`. Before this change the write
/// succeeded (and forked). Turning a silent fork into a wedged write is a trade,
/// not obviously the right one, and it is not stated anywhere.
#[test]
fn a_write_still_lands_when_the_fossil_generations_catalogue_entry_is_gone() {
    let (mut io, row_id, stale_family, stale_bytes) = two_generation_store();
    reseed_split(&mut io, row_id, &stale_family, &stale_bytes);
    assert_eq!(
        heads_for(&io, "main", row_id).len(),
        2,
        "the harness must actually produce a split store"
    );

    // Control: the READ ladder is unaffected by the catalogue removal, because it
    // resolves the descriptor from the raw table header.
    let catalogue_key = format!(
        "catrow:{}",
        users_schema_hash().to_object_id().uuid().simple()
    );
    assert!(
        io.raw_table_get("catalogue", &catalogue_key)
            .expect("catalogue probe")
            .is_some(),
        "the harness must be deleting a catalogue entry that exists"
    );
    io.raw_table_delete("catalogue", &catalogue_key)
        .expect("catalogue delete");
    crate::storage::invalidate_catalogue_lookup_caches_with_storage(&io);

    assert!(
        io.load_visible_region_row("users", "main", row_id)
            .expect("the READ ladder must survive a missing catalogue entry")
            .is_some(),
        "control: the read ladder resolves the descriptor from the raw table header"
    );

    // The attack: a visible write to the split row.
    let entry = io
        .load_visible_region_entry("users", "main", row_id)
        .expect("entry read")
        .expect("the row must have an entry");
    let result = io.upsert_visible_region_rows("users", std::slice::from_ref(&entry));

    assert!(
        result.is_ok(),
        "a visible write to a row that still has a head in a generation whose \
         CATALOGUE entry is gone must not fail: the read path resolves that same \
         family's descriptor from the raw table header, so the retirement helper \
         should too (or degrade), rather than wedging the write: {result:?}"
    );
}

/// ROUND-2: settle WHY `adversarial_cross_generation::
/// an_unresolvable_split_row_is_still_read_correctly` is red behind `#[ignore]`.
///
/// Its stated reason is the premise ("two heads, zero history needs manual
/// history removal"). That is not the cause. The same premise on the MEASURED
/// production shape — authoritative `__visible_row_table_locator` present and
/// naming the live family — reads correctly, because the inverted ladder hits it
/// first. What makes that gate red is its harness ALSO clearing the authoritative
/// locator, which production does not.
///
/// So the residue is real but narrower than the red gate suggests: an unresolvable
/// split reads the fossil only when the authoritative pointer is ALSO absent.
#[test]
fn an_unresolvable_split_reads_correctly_on_the_measured_production_shape() {
    let (mut io, row_id, stale_family, stale_bytes) = two_generation_store();
    reseed_split(&mut io, row_id, &stale_family, &stale_bytes);

    // Strip every history batch so the sweep has nothing to arbitrate with.
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

    let report = crate::storage::repair_all_split_visible_row_families(&mut io).expect("sweep");
    assert_eq!(
        report.unresolved_rows, 1,
        "the harness must actually hit the unresolved branch: {report:?}"
    );
    assert_eq!(
        heads_for(&io, "main", row_id).len(),
        2,
        "an unresolved row must be left split, by design"
    );

    let served = io
        .load_visible_region_row("users", "main", row_id)
        .expect("visible read should succeed")
        .expect("the row still has heads, so something is served");
    let live_family = format!("rowtable:visible:users:{}", next_generation_schema_hash());
    let served_family = io
        .load_visible_row_table_locator("main", row_id)
        .expect("authoritative locator read")
        .map(|locator| locator.row_raw_table.to_string());
    assert_eq!(
        served_family.as_deref(),
        Some(live_family.as_str()),
        "the authoritative locator must still name the live family after an \
         unresolved pass"
    );
    assert_eq!(
        served.updated_at, 2_000,
        "with the authoritative pointer intact, an unresolvable split still reads \
         as the LIVE generation — the residue is the duplicate head, not the read"
    );
}

/// ROUND-2: a row the sweep reported as `unresolved` must NOT also be reported as
/// `repaired`. The report is the only audit trail after a pass that silently
/// retired index entries, and a row in both lists makes it useless.
#[test]
fn the_repair_report_does_not_list_an_unresolved_row_as_repaired() {
    let (mut io, row_id, stale_family, stale_bytes) = two_generation_store();
    reseed_split(&mut io, row_id, &stale_family, &stale_bytes);
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

    let report = crate::storage::repair_all_split_visible_row_families(&mut io).expect("sweep");
    assert_eq!(report.unresolved_rows, 1, "premise: {report:?}");
    assert!(
        !report
            .repaired
            .iter()
            .any(|(_, _, reported)| *reported == row_id),
        "the row is reported as REPAIRED and as UNRESOLVED at the same time; \
         `repaired` is pushed before the pass knows whether it can arbitrate, so \
         every unresolved row is double-counted: {report:?}"
    );
}

/// ROUND-1 ITEM 3 RE-MEASURE: the sweep's boot cost WITH the marker in place.
///
/// The entry quotes 32/144/807 ms — those are FIRST-sweep numbers. What a steady
/// deployment actually pays is the second boot and every boot after it. Prints
/// both.
#[cfg(feature = "rocksdb")]
#[test]
#[ignore = "measurement, not a gate"]
fn startup_sweep_cost_with_the_marker_in_place() {
    use crate::storage::RocksDBStorage;

    for rows in [50_000usize, 200_000] {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let mut io = RocksDBStorage::open(dir.path().to_str().expect("path"), 256 * 1024 * 1024)
            .expect("open");
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
        let seed_a = ObjectId::new();
        let a = visible_row(seed_a, "main", Vec::new(), 1_000, b"a");
        send_and_approve(&mut sm, &mut io, client_id, row_metadata("users"), &a);
        let seed_b = ObjectId::new();
        let b = next_generation_row(seed_b, Vec::new(), 2_000, "b", "online");
        send_and_approve(&mut sm, &mut io, client_id, next_generation_metadata(), &b);

        let family_a = format!("rowtable:visible:users:{}", users_schema_hash());
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
        crate::storage::repair_all_split_visible_row_families(&mut io).expect("first sweep");
        let first = started.elapsed();

        let started = std::time::Instant::now();
        crate::storage::repair_all_split_visible_row_families(&mut io).expect("second sweep");
        let second = started.elapsed();

        println!("sweep at {rows} rows: first boot {first:?}, marked second boot {second:?}");
    }
}
