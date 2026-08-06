//! Graft missing history links for one row from source stores into a target store.
//!
//! The repair the engine otherwise lacks: a receiver missing one link of a row's
//! parent-linked chain rejects every later batch with `ParentNotFound`, forever. When the
//! missing links still exist somewhere — an old snapshot, another peer's store — copying
//! them back makes the chain contiguous and the next incoming batch applies normally.
//!
//! What a graft writes, per missing batch, all through the sanctioned mutation verb so the
//! pieces the APPLY PATH actually reads come along — the raw-table header, the per-batch
//! exact table locator (without which the parent point-lookup returns `None` even when the
//! bytes exist), and the local-batch index:
//!
//! - the history entry, re-encoded under the SOURCE's schema hash, never re-derived by the
//!   decode heuristic — an old-schema batch that happens to decode under the current
//!   descriptor would otherwise be silently re-homed under the wrong layout;
//! - its authoritative batch fate, only where the target has none;
//! - the row locator, only if the target lacks it.
//!
//! What it never writes: visible-region entries (visibility moves through the normal apply
//! path when the next batch lands), secondary indexes (derived from visibility), sealed
//! submissions (deleted after settlement; recovery would re-validate ancient ones against
//! today's frontier), branch ords (store-local), local batch records (authoring-side).
//!
//! Conflicts are judged by AUTHORSHIP — branch, data, timestamp, author — not by bytes
//! (encoders drift), not by full struct equality (state and tier legitimately differ), and
//! not by content digest either: the digest covers parents, and a client's copy of a
//! delivered batch has its parents STRIPPED by the sender — normal, not divergence.
//! Parentless copies of non-root batches are skipped, never grafted: writing one would
//! hollow out the very chain being repaired.

use std::collections::BTreeMap;

use super::{
    Storage, StorageError, encode_history_row_bytes_with_context,
    prepared_row_write_context_for_schema_hash,
};
use crate::object::ObjectId;
use crate::row_histories::{RowState, StoredRowBatch};

/// What one graft run did — and, run again, what it should report as all zeros.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct GraftReport {
    pub batches_grafted: usize,
    pub batches_already_present: usize,
    pub fates_copied: usize,
    /// Parent-stripped delivery duplicates in a source that were not usable as truth.
    pub stripped_copies_skipped: usize,
}

/// Copy the missing history links for `row_id` from `source` into `target`.
///
/// Both stores must be offline copies — the RocksDB lock enforces it for that backend.
/// Additive and idempotent: nothing existing is overwritten, and a second run reports
/// zero grafts.
pub fn graft_row_history<T, S>(
    target: &mut T,
    source: &S,
    table: &str,
    row_id: ObjectId,
) -> Result<GraftReport, StorageError>
where
    T: Storage,
    S: Storage + ?Sized,
{
    let mut report = GraftReport::default();

    // The locator first: encode-time schema resolution hangs off it.
    let source_locator = source.load_row_locator(row_id)?;
    match (target.load_row_locator(row_id)?, source_locator.as_ref()) {
        (None, Some(locator)) => target.put_row_locator(row_id, Some(locator))?,
        (Some(existing), Some(incoming)) if existing.table != incoming.table => {
            return Err(StorageError::IoError(format!(
                "graft refused: target locates row {row_id} in {:?}, source in {:?} — this \
                 is divergence, not a missing link",
                existing.table, incoming.table
            )));
        }
        _ => {}
    }

    // Decoded batches for identity and state.
    let source_batches = source.scan_history_row_batches(table, row_id)?;

    // The target's existing batches, decoded: the conflict rule needs authorship fields.
    // A client-store copy of a delivered batch differs LEGITIMATELY in parents — the
    // sender strips them on delivery — so digest inequality alone is not divergence.
    let target_batches: BTreeMap<_, _> = target
        .scan_history_row_batches(table, row_id)?
        .into_iter()
        .map(|row| ((row.branch.to_string(), row.batch_id), row))
        .collect();

    let same_authorship = |a: &StoredRowBatch, b: &StoredRowBatch| {
        a.branch == b.branch
            && a.data == b.data
            && a.updated_at == b.updated_at
            && a.updated_by == b.updated_by
    };

    for row in &source_batches {
        if matches!(row.state, RowState::StagingPending) {
            return Err(StorageError::IoError(format!(
                "graft refused: batch {:?} of row {row_id} is staging-pending — grafting \
                 an unsettled write is not a repair",
                row.batch_id
            )));
        }

        let key = (row.branch.to_string(), row.batch_id);

        // Authorship first, before any skip: a batch the target holds under the same id
        // with different content is divergence whether or not it is reachable. Parents are
        // deliberately not compared — a delivery-stripped source copy differs there and
        // that is normal.
        if let Some(existing) = target_batches.get(&key)
            && !same_authorship(existing, row)
        {
            return Err(StorageError::IoError(format!(
                "graft refused: batch {:?} of row {row_id} exists in the target with \
                 different content — this is divergence, not a missing link",
                row.batch_id
            )));
        }

        // Reachable in the target: done. Bytes present but unreachable fall through — the
        // rewrite through the sanctioned verb heals the missing locator.
        if target
            .load_history_row_batch(table, row.branch.as_str(), row_id, row.batch_id)?
            .is_some()
        {
            report.batches_already_present += 1;
            continue;
        }

        // A parentless copy of a non-root batch is a delivery-stripped duplicate, never a
        // source of truth: grafting it would hollow out the chain being repaired. The
        // genuine root also has no parents, but a root is only graftable into a row the
        // target does not know at all.
        if row.parents.is_empty() && !target_batches.is_empty() {
            report.stripped_copies_skipped += 1;
            continue;
        }

        // The schema hash this batch was actually encoded under, resolved the way the
        // READ path resolves it: the per-batch exact locator when one exists, the row
        // locator's origin hash otherwise. Never the decode heuristic.
        let schema_hash = source
            .load_history_row_batch_table_locator(row.branch.as_str(), row_id, row.batch_id)?
            .map(|locator| locator.schema_hash)
            .or_else(|| {
                source_locator
                    .as_ref()
                    .and_then(|locator| locator.origin_schema_hash)
            })
            .ok_or_else(|| {
                StorageError::IoError(format!(
                    "graft refused: no schema placement for batch {:?} — the source has                      neither an exact locator nor a row-locator origin hash",
                    row.batch_id
                ))
            })?;
        let context =
            prepared_row_write_context_for_schema_hash(target, table, schema_hash, row_id)?;
        let encoded = encode_history_row_bytes_with_context(&context, row)?;
        let rows = std::slice::from_ref(row);
        let encoded_rows = std::slice::from_ref(&encoded);
        target.apply_encoded_row_mutation(table, encoded_rows, &[], &[])?;
        target.index_local_batch_history_rows(table, rows, encoded_rows)?;
        report.batches_grafted += 1;

        // The fate, only where the target has none. A blind upsert could clobber a live
        // fate with a snapshot's rejected one and retro-reject a visible batch.
        if target
            .load_authoritative_batch_fate(row.batch_id)?
            .is_none()
            && let Some(fate) = source.load_authoritative_batch_fate(row.batch_id)?
        {
            target.upsert_authoritative_batch_fate(&fate)?;
            report.fates_copied += 1;
        }
    }

    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemoryStorage;
    use std::collections::HashMap;

    use crate::catalogue::CatalogueEntry;
    use crate::metadata::MetadataKey;
    use crate::metadata::ObjectType;
    use crate::metadata::RowProvenance;
    use crate::query_manager::types::{
        ColumnType, Schema, SchemaBuilder, SchemaHash, TableSchema, Value,
    };
    use crate::row_format::encode_row;
    use crate::row_histories::BatchId;
    use crate::storage::RowLocator;

    fn users_schema_v1() -> Schema {
        SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build()
    }

    fn users_schema_v2() -> Schema {
        SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text)
                    .column("email", ColumnType::Text),
            )
            .build()
    }

    fn persist_schema<H: Storage>(storage: &mut H, schema: &Schema) -> SchemaHash {
        let schema_hash = SchemaHash::compute(schema);
        storage
            .upsert_catalogue_entry(&CatalogueEntry {
                object_id: schema_hash.to_object_id(),
                metadata: HashMap::from([(
                    MetadataKey::Type.to_string(),
                    ObjectType::CatalogueSchema.to_string(),
                )]),
                content: crate::schema_manager::encode_schema(schema),
            })
            .expect("persist schema");
        schema_hash
    }

    fn chain(row_id: ObjectId, schema: &Schema, len: usize) -> Vec<StoredRowBatch> {
        let mut out = Vec::new();
        let mut parent: Option<BatchId> = None;
        for level in 0..len {
            let data = encode_row(
                &schema[&crate::query_manager::types::TableName::new("users")].columns,
                &[Value::Uuid(row_id), Value::Text(format!("v{level}"))],
            )
            .expect("encode row");
            let row = StoredRowBatch::new(
                row_id,
                "main",
                parent.into_iter().collect::<Vec<_>>(),
                data,
                RowProvenance::for_insert("author", 1_000 + level as u64),
                HashMap::new(),
                RowState::VisibleDirect,
                None,
            );
            parent = Some(row.batch_id());
            out.push(row);
        }
        out
    }

    /// The review's decisive scenario: a batch authored under one schema grafted into a
    /// target whose row locator points at another. A byte-copy closes the SCAN and still
    /// fails the POINT LOOKUP — the check the apply path actually runs — because the
    /// per-batch exact locator is missing. The sanctioned verb writes it.
    #[test]
    fn scan_green_is_not_enough_the_point_lookup_must_close_too() {
        let row_id = ObjectId::new();

        let mut source = MemoryStorage::new();
        let v1 = persist_schema(&mut source, &users_schema_v1());
        source
            .put_row_locator(
                row_id,
                Some(&RowLocator {
                    table: "users".into(),
                    origin_schema_hash: Some(v1),
                }),
            )
            .unwrap();
        let rows = chain(row_id, &users_schema_v1(), 3);
        source.append_history_region_rows("users", &rows).unwrap();

        let mut target = MemoryStorage::new();
        persist_schema(&mut target, &users_schema_v1());
        let v2 = persist_schema(&mut target, &users_schema_v2());
        // The target believes the row originates under v2: grafted v1 batches land in a
        // raw table the origin-hash fast path will not find.
        target
            .put_row_locator(
                row_id,
                Some(&RowLocator {
                    table: "users".into(),
                    origin_schema_hash: Some(v2),
                }),
            )
            .unwrap();

        let report = graft_row_history(&mut target, &source, "users", row_id).unwrap();
        assert_eq!(report.batches_grafted, 3);

        // Scan closure — chain_doctor's oracle.
        assert_eq!(
            target
                .scan_history_row_batches("users", row_id)
                .unwrap()
                .len(),
            3
        );
        // Point-lookup closure — the APPLY path's oracle, for every batch.
        for row in &rows {
            assert!(
                target
                    .load_history_row_batch("users", "main", row_id, row.batch_id())
                    .unwrap()
                    .is_some(),
                "batch {:?} is scan-visible but unreachable by the point lookup the apply \
                 path uses — the graft omitted the exact table locator, and every \
                 descendant of this batch still dies with ParentNotFound",
                row.batch_id()
            );
        }

        // Idempotence.
        let again = graft_row_history(&mut target, &source, "users", row_id).unwrap();
        assert_eq!(again.batches_grafted, 0, "a second run must write nothing");
        assert_eq!(again.batches_already_present, 3);
    }

    /// The decisive backend for the decisive scenario: on the raw-table backends the point
    /// lookup resolves through the row locator's origin hash FIRST and the per-batch exact
    /// locator second. MemoryStorage overrides the lookup with a plain map read, so only
    /// this variant has teeth — a graft that omitted exact locators passes the memory
    /// variant and fails here.
    #[cfg(feature = "sqlite")]
    #[test]
    fn the_point_lookup_closes_on_a_raw_table_backend_too() {
        use crate::storage::SqliteStorage;

        let dir = tempfile::tempdir().expect("tempdir");
        let row_id = ObjectId::new();

        let mut source = SqliteStorage::open(dir.path().join("source.sqlite")).unwrap();
        let v1 = persist_schema(&mut source, &users_schema_v1());
        source
            .put_row_locator(
                row_id,
                Some(&RowLocator {
                    table: "users".into(),
                    origin_schema_hash: Some(v1),
                }),
            )
            .unwrap();
        let rows = chain(row_id, &users_schema_v1(), 3);
        source.append_history_region_rows("users", &rows).unwrap();

        let mut target = SqliteStorage::open(dir.path().join("target.sqlite")).unwrap();
        persist_schema(&mut target, &users_schema_v1());
        let v2 = persist_schema(&mut target, &users_schema_v2());
        target
            .put_row_locator(
                row_id,
                Some(&RowLocator {
                    table: "users".into(),
                    origin_schema_hash: Some(v2),
                }),
            )
            .unwrap();

        let report = graft_row_history(&mut target, &source, "users", row_id).unwrap();
        assert_eq!(report.batches_grafted, 3);

        for row in &rows {
            assert!(
                target
                    .load_history_row_batch("users", "main", row_id, row.batch_id())
                    .unwrap()
                    .is_some(),
                "batch {:?} is scan-visible but unreachable by the point lookup the apply \
                 path uses — the graft omitted the exact table locator, and every \
                 descendant of this batch still dies with ParentNotFound",
                row.batch_id()
            );
        }

        let again = graft_row_history(&mut target, &source, "users", row_id).unwrap();
        assert_eq!(again.batches_grafted, 0, "a second run must write nothing");
    }

    /// Same batch id with different content is divergence, and the graft must refuse it
    /// without writing anything.
    #[test]
    fn a_content_conflict_aborts_the_graft() {
        let row_id = ObjectId::new();
        let schema = users_schema_v1();

        let mut source = MemoryStorage::new();
        let v1 = persist_schema(&mut source, &schema);
        source
            .put_row_locator(
                row_id,
                Some(&RowLocator {
                    table: "users".into(),
                    origin_schema_hash: Some(v1),
                }),
            )
            .unwrap();
        let rows = chain(row_id, &schema, 2);
        source.append_history_region_rows("users", &rows).unwrap();

        let mut target = MemoryStorage::new();
        persist_schema(&mut target, &schema);
        target
            .put_row_locator(
                row_id,
                Some(&RowLocator {
                    table: "users".into(),
                    origin_schema_hash: Some(v1),
                }),
            )
            .unwrap();
        // The same batch id holds DIFFERENT content in the target.
        let mut forged = rows[0].clone();
        forged.data = encode_row(
            &schema[&crate::query_manager::types::TableName::new("users")].columns,
            &[Value::Uuid(row_id), Value::Text("forged".into())],
        )
        .unwrap()
        .into();
        target
            .append_history_region_rows("users", std::slice::from_ref(&forged))
            .unwrap();

        let error = graft_row_history(&mut target, &source, "users", row_id).unwrap_err();
        assert!(
            error.to_string().contains("divergence"),
            "expected a divergence refusal, got: {error}"
        );
    }
}
