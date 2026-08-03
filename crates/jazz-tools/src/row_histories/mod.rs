//! Row-history data types, codecs, and the algorithms that turn them into the
//! row a reader sees and the writes a sync target receives.
//!
//! Split into five submodules so each concern is independently navigable:
//! - [`types`]: data types (BatchId, RowState, QueryRowBatch, StoredRowBatch,
//!   RowMetadata, VisibleRowEntry, error types)
//! - [`codecs`]: descriptor builders and flat-row encode/decode
//! - [`resolution`]: pure visibility/merge math — frontier walk, latest common
//!   ancestor, per-column merge, delete-winner, computed visible preview. No
//!   storage access; called by both `mutations` and `types` (via
//!   `VisibleRowEntry::rebuild_*`).
//! - [`fastpath`]: pure O(1) serial-write construction of the next
//!   `VisibleRowEntry` (guards + tier-pointer carry-forward), its kill switch
//!   and telemetry counters. No storage access.
//! - [`mutations`]: the storage-mutating verbs (`apply_row_batch`,
//!   `patch_row_batch_state`) and their direct support — load history,
//!   recompute visibility via `resolution` (or `fastpath` when eligible),
//!   write through `Storage`, emit a `RowVisibilityChange`.

mod codecs;
mod fastpath;
mod mutations;
mod resolution;
mod types;

pub(crate) use codecs::{
    FlatRowCodecs, decode_flat_history_row_with_codecs, decode_flat_visible_row_entry_with_codecs,
    flat_row_codecs,
};
pub use codecs::{
    compute_row_digest, decode_flat_history_row, decode_flat_visible_row_entry,
    encode_flat_history_row, encode_flat_visible_row_entry, history_row_physical_descriptor,
    visible_row_physical_descriptor,
};
pub use fastpath::{
    HISTORY_FASTPATH_FALLBACKS, HISTORY_FASTPATH_HITS, PATCH_FASTPATH_FALLBACKS,
    PATCH_FASTPATH_HITS, history_fastpath_enabled,
};
#[cfg(any(test, feature = "test"))]
pub use fastpath::{HistoryFastpathMode, force_history_fastpath};
pub(crate) use mutations::{ApplyRowBatchWithContext, apply_row_batch_with_context};
pub use mutations::{apply_row_batch, patch_row_batch_state};
pub(crate) use resolution::visible_row_preview_from_history_rows;
pub use types::{
    ApplyRowBatchResult, BatchId, HistoryScan, QueryRowBatch, RowHistoryError, RowMetadata,
    RowState, RowVisibilityChange, StoredRowBatch, VisibleRowEntry,
};

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use uuid::Uuid;

    use super::fastpath::{try_in_place_tip_update_entry, try_serial_fastpath_entry};
    use super::*;
    use crate::metadata::{DeleteKind, MetadataKey, RowProvenance};
    use crate::object::{BranchName, ObjectId};
    use crate::query_manager::types::{
        ColumnDescriptor, ColumnMergeStrategy, ColumnType, RowDescriptor, Schema, TableName,
        TableSchema, Value,
    };
    use crate::row_format::{decode_row, encode_row};
    use crate::storage::{MemoryStorage, RowLocator, Storage};
    use crate::sync_manager::DurabilityTier;

    fn visible_row(updated_at: u64, confirmed_tier: Option<DurabilityTier>) -> StoredRowBatch {
        StoredRowBatch::new(
            ObjectId::new(),
            "main",
            Vec::new(),
            vec![updated_at as u8],
            RowProvenance::for_insert("alice".to_string(), updated_at),
            HashMap::new(),
            RowState::VisibleDirect,
            confirmed_tier,
        )
    }

    #[test]
    fn flat_visible_row_binary_roundtrips_retained_visible_columns() {
        let user_descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("title", ColumnType::Text),
            ColumnDescriptor::new("done", ColumnType::Boolean).nullable(),
        ]);
        let global = StoredRowBatch::new(
            ObjectId::from_uuid(Uuid::from_u128(21)),
            "main",
            Vec::new(),
            encode_row(
                &user_descriptor,
                &[Value::Text("ship it".into()), Value::Boolean(true)],
            )
            .expect("encode global row"),
            RowProvenance::for_insert("alice".to_string(), 10),
            HashMap::from([("source".to_string(), "global".to_string())]),
            RowState::VisibleDirect,
            Some(DurabilityTier::GlobalServer),
        );
        let current = StoredRowBatch::new(
            global.row_id,
            "main",
            vec![global.batch_id()],
            encode_row(
                &user_descriptor,
                &[Value::Text("ship it".into()), Value::Boolean(false)],
            )
            .expect("encode current row"),
            RowProvenance::for_update(&global.row_provenance(), "bob".to_string(), 30),
            HashMap::from([("source".to_string(), "local".to_string())]),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let entry = VisibleRowEntry {
            current_row: current,
            branch_frontier: vec![global.batch_id()],
            worker_batch_id: None,
            edge_batch_id: Some(global.batch_id()),
            global_batch_id: Some(global.batch_id()),
            winner_batch_pool: Vec::new(),
            current_winner_ordinals: None,
            worker_winner_ordinals: None,
            edge_winner_ordinals: None,
            global_winner_ordinals: None,
            merge_artifacts: Some(vec![1, 2, 3, 4]),
        };

        let encoded =
            encode_flat_visible_row_entry(&user_descriptor, &entry).expect("encode flat visible");
        let decoded = decode_flat_visible_row_entry(
            &user_descriptor,
            entry.current_row.row_id,
            entry.current_row.branch.as_str(),
            &encoded,
        )
        .expect("decode flat visible");

        assert_eq!(decoded.current_row.row_id, entry.current_row.row_id);
        assert_eq!(decoded.current_row.batch_id(), entry.current_row.batch_id());
        assert_eq!(decoded.current_row.branch, entry.current_row.branch);
        assert!(decoded.current_row.parents.is_empty());
        assert_eq!(decoded.current_row.updated_at, entry.current_row.updated_at);
        assert_eq!(decoded.current_row.created_by, entry.current_row.created_by);
        assert_eq!(decoded.current_row.created_at, entry.current_row.created_at);
        assert_eq!(decoded.current_row.updated_by, entry.current_row.updated_by);
        assert_eq!(decoded.current_row.state, entry.current_row.state);
        assert_eq!(
            decoded.current_row.confirmed_tier,
            entry.current_row.confirmed_tier
        );
        assert_eq!(
            decoded.current_row.delete_kind,
            entry.current_row.delete_kind
        );
        assert!(decoded.current_row.metadata.is_empty());
        assert_eq!(decoded.current_row.data, entry.current_row.data);
        assert_eq!(decoded.branch_frontier, entry.branch_frontier);
        assert_eq!(decoded.worker_batch_id, entry.worker_batch_id);
        assert_eq!(decoded.edge_batch_id, entry.edge_batch_id);
        assert_eq!(decoded.global_batch_id, entry.global_batch_id);
        assert_eq!(decoded.merge_artifacts, entry.merge_artifacts);
    }

    #[test]
    fn visible_row_entry_omits_tier_pointers_when_current_is_globally_confirmed() {
        let current = visible_row(30, Some(DurabilityTier::GlobalServer));
        let entry = VisibleRowEntry::rebuild(current.clone(), std::slice::from_ref(&current));

        assert_eq!(entry.branch_frontier, vec![current.batch_id()]);
        assert_eq!(entry.worker_batch_id, None);
        assert_eq!(entry.edge_batch_id, None);
        assert_eq!(entry.global_batch_id, None);
        assert_eq!(entry.merge_artifacts, None);
    }

    #[test]
    fn visible_row_entry_resolves_tier_fallback_chain() {
        let global = visible_row(10, Some(DurabilityTier::GlobalServer));
        let edge = StoredRowBatch::new(
            global.row_id,
            "main",
            vec![global.batch_id()],
            vec![2],
            RowProvenance::for_update(&global.row_provenance(), "alice".to_string(), 20),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::EdgeServer),
        );
        let current = StoredRowBatch::new(
            global.row_id,
            "main",
            vec![edge.batch_id()],
            vec![3],
            RowProvenance::for_update(&edge.row_provenance(), "alice".to_string(), 30),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let history = vec![global.clone(), edge.clone(), current.clone()];

        let entry = VisibleRowEntry::rebuild(current.clone(), &history);

        assert_eq!(entry.branch_frontier, vec![current.batch_id()]);
        assert_eq!(entry.worker_batch_id, None);
        assert_eq!(entry.edge_batch_id, Some(edge.batch_id()));
        assert_eq!(entry.global_batch_id, Some(global.batch_id()));
        assert_eq!(
            entry.batch_id_for_tier(DurabilityTier::Local),
            Some(current.batch_id())
        );
        assert_eq!(
            entry.batch_id_for_tier(DurabilityTier::EdgeServer),
            Some(edge.batch_id())
        );
        assert_eq!(
            entry.batch_id_for_tier(DurabilityTier::GlobalServer),
            Some(global.batch_id())
        );
    }

    #[test]
    fn visible_row_entry_returns_none_when_no_version_meets_required_tier() {
        let current = visible_row(30, Some(DurabilityTier::Local));
        let entry = VisibleRowEntry::rebuild(current.clone(), std::slice::from_ref(&current));

        assert_eq!(entry.branch_frontier, vec![current.batch_id()]);
        assert_eq!(entry.batch_id_for_tier(DurabilityTier::EdgeServer), None);
        assert_eq!(entry.batch_id_for_tier(DurabilityTier::GlobalServer), None);
    }

    #[test]
    fn visible_row_entry_preserves_multiple_branch_tips() {
        let base = visible_row(10, Some(DurabilityTier::Local));
        let left = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            vec![1],
            RowProvenance::for_update(&base.row_provenance(), "alice".to_string(), 20),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let right = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            vec![2],
            RowProvenance::for_update(&base.row_provenance(), "bob".to_string(), 21),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );

        let entry = VisibleRowEntry::rebuild(right.clone(), &[base, left.clone(), right.clone()]);

        assert_eq!(
            entry.branch_frontier,
            vec![left.batch_id(), right.batch_id()]
        );
    }

    #[test]
    fn visible_row_entry_merges_conflicting_field_updates() {
        let descriptor = user_descriptor();
        let base = StoredRowBatch::new(
            ObjectId::new(),
            "main",
            Vec::new(),
            encode_row(
                &descriptor,
                &[Value::Text("task".into()), Value::Boolean(false)],
            )
            .unwrap(),
            RowProvenance::for_insert("alice".to_string(), 10),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let left = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &descriptor,
                &[Value::Text("alice-title".into()), Value::Boolean(false)],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "alice".to_string(), 20),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let right = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &descriptor,
                &[Value::Text("task".into()), Value::Boolean(true)],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "bob".to_string(), 21),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );

        let entry = VisibleRowEntry::rebuild_with_descriptor(
            &descriptor,
            &[base, left.clone(), right.clone()],
        )
        .unwrap()
        .expect("merged visible entry");

        assert_eq!(
            decode_row(&descriptor, &entry.current_row.data).unwrap(),
            vec![Value::Text("alice-title".into()), Value::Boolean(true)]
        );
        assert_eq!(entry.current_row.batch_id(), right.batch_id());
        assert_eq!(entry.current_row.updated_by.as_str(), "bob");
        assert_eq!(
            entry.branch_frontier,
            vec![left.batch_id(), right.batch_id()]
        );
    }

    #[test]
    fn visible_row_entry_applies_counter_merge_strategy_per_column() {
        let descriptor = counter_descriptor();
        let base = StoredRowBatch::new(
            ObjectId::new(),
            "main",
            Vec::new(),
            encode_row(
                &descriptor,
                &[Value::Text("task".into()), Value::Integer(5)],
            )
            .unwrap(),
            RowProvenance::for_insert("alice".to_string(), 10),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let left = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &descriptor,
                &[Value::Text("alice-title".into()), Value::Integer(7)],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "alice".to_string(), 20),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let right = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &descriptor,
                &[Value::Text("task".into()), Value::Integer(4)],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "bob".to_string(), 21),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );

        let entry = VisibleRowEntry::rebuild_with_descriptor(
            &descriptor,
            &[base, left.clone(), right.clone()],
        )
        .unwrap()
        .expect("merged visible entry");

        assert_eq!(
            decode_row(&descriptor, &entry.current_row.data).unwrap(),
            vec![Value::Text("alice-title".into()), Value::Integer(6)]
        );
        assert_eq!(entry.current_row.batch_id(), right.batch_id());
        assert_eq!(entry.current_row.updated_by.as_str(), "bob");
        assert_eq!(
            entry.branch_frontier,
            vec![left.batch_id(), right.batch_id()]
        );
        assert_eq!(
            entry.winner_batch_pool,
            vec![left.batch_id(), right.batch_id()]
        );
        assert_eq!(entry.current_winner_ordinals, Some(vec![0, 1]));
    }

    #[test]
    fn visible_row_entry_uses_consumer_schema_merge_strategy() {
        let counter_descriptor = counter_descriptor();
        let lww_descriptor = lww_integer_descriptor();
        let base = StoredRowBatch::new(
            ObjectId::new(),
            "main",
            Vec::new(),
            encode_row(
                &counter_descriptor,
                &[Value::Text("task".into()), Value::Integer(5)],
            )
            .unwrap(),
            RowProvenance::for_insert("alice".to_string(), 10),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let left = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &counter_descriptor,
                &[Value::Text("alice-title".into()), Value::Integer(7)],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "alice".to_string(), 20),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let right = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &counter_descriptor,
                &[Value::Text("task".into()), Value::Integer(4)],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "bob".to_string(), 21),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let history = vec![base, left, right];

        let counter_entry = VisibleRowEntry::rebuild_with_descriptor(&counter_descriptor, &history)
            .unwrap()
            .expect("counter merged visible entry");
        let lww_entry = VisibleRowEntry::rebuild_with_descriptor(&lww_descriptor, &history)
            .unwrap()
            .expect("lww merged visible entry");

        assert_eq!(
            decode_row(&counter_descriptor, &counter_entry.current_row.data).unwrap(),
            vec![Value::Text("alice-title".into()), Value::Integer(6)]
        );
        assert_eq!(
            decode_row(&lww_descriptor, &lww_entry.current_row.data).unwrap(),
            vec![Value::Text("alice-title".into()), Value::Integer(4)]
        );
    }

    #[test]
    fn visible_row_entry_errors_when_counter_merge_overflows() {
        let descriptor = counter_only_descriptor();
        let base = StoredRowBatch::new(
            ObjectId::new(),
            "main",
            Vec::new(),
            encode_row(&descriptor, &[Value::Integer(i32::MAX - 1)]).unwrap(),
            RowProvenance::for_insert("alice".to_string(), 10),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let left = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(&descriptor, &[Value::Integer(i32::MAX)]).unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "alice".to_string(), 20),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let right = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(&descriptor, &[Value::Integer(i32::MAX)]).unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "bob".to_string(), 21),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );

        let error = VisibleRowEntry::rebuild_with_descriptor(&descriptor, &[base, left, right])
            .expect_err("counter overflow should fail");

        assert!(
            error.to_string().contains("overflow"),
            "expected overflow error, got {error}"
        );
    }

    #[test]
    fn visible_row_entry_merges_accepted_transactional_rows_but_ignores_staging_and_rejected_rows()
    {
        let descriptor = user_descriptor();
        let base = StoredRowBatch::new(
            ObjectId::new(),
            "main",
            Vec::new(),
            encode_row(
                &descriptor,
                &[Value::Text("task".into()), Value::Boolean(false)],
            )
            .unwrap(),
            RowProvenance::for_insert("alice".to_string(), 10),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let accepted_transaction = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &descriptor,
                &[Value::Text("txn-title".into()), Value::Boolean(false)],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "alice".to_string(), 20),
            HashMap::new(),
            RowState::VisibleTransactional,
            Some(DurabilityTier::Local),
        );
        let direct = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &descriptor,
                &[Value::Text("task".into()), Value::Boolean(true)],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "bob".to_string(), 21),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let staging = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &descriptor,
                &[
                    Value::Text("staging-should-not-win".into()),
                    Value::Boolean(false),
                ],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "mallory".to_string(), 30),
            HashMap::new(),
            RowState::StagingPending,
            None,
        );
        let rejected = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &descriptor,
                &[
                    Value::Text("rejected-should-not-win".into()),
                    Value::Boolean(false),
                ],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "mallory".to_string(), 31),
            HashMap::new(),
            RowState::Rejected,
            None,
        );

        let entry = VisibleRowEntry::rebuild_with_descriptor(
            &descriptor,
            &[
                base,
                accepted_transaction,
                direct.clone(),
                staging,
                rejected,
            ],
        )
        .unwrap()
        .expect("merged visible entry");

        assert_eq!(
            decode_row(&descriptor, &entry.current_row.data).unwrap(),
            vec![Value::Text("txn-title".into()), Value::Boolean(true)]
        );
        assert_eq!(entry.current_row.batch_id(), direct.batch_id());
        assert_eq!(entry.current_row.updated_by.as_str(), "bob");
    }

    #[test]
    fn visible_row_entry_roundtrips_current_winner_ordinals() {
        let descriptor = user_descriptor();
        let base = StoredRowBatch::new(
            ObjectId::new(),
            "main",
            Vec::new(),
            encode_row(
                &descriptor,
                &[Value::Text("task".into()), Value::Boolean(false)],
            )
            .unwrap(),
            RowProvenance::for_insert("alice".to_string(), 10),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let left = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &descriptor,
                &[Value::Text("alice-title".into()), Value::Boolean(false)],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "alice".to_string(), 20),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let right = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &descriptor,
                &[Value::Text("task".into()), Value::Boolean(true)],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "bob".to_string(), 21),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );

        let entry = VisibleRowEntry::rebuild_with_descriptor(
            &descriptor,
            &[base, left.clone(), right.clone()],
        )
        .unwrap()
        .expect("merged visible entry");
        assert_eq!(
            entry.winner_batch_pool,
            vec![left.batch_id(), right.batch_id()]
        );
        assert_eq!(entry.current_winner_ordinals, Some(vec![0, 1]));

        let encoded =
            encode_flat_visible_row_entry(&descriptor, &entry).expect("encode merged visible row");
        let decoded = decode_flat_visible_row_entry(
            &descriptor,
            entry.current_row.row_id,
            entry.current_row.branch.as_str(),
            &encoded,
        )
        .expect("decode merged visible row");

        assert_eq!(decoded.winner_batch_pool, entry.winner_batch_pool);
        assert_eq!(
            decoded.current_winner_ordinals,
            entry.current_winner_ordinals
        );
        assert_eq!(decoded.edge_winner_ordinals, None);
        assert_eq!(decoded.global_winner_ordinals, None);
    }

    #[test]
    fn visible_row_entry_materializes_tier_preview_when_batch_id_matches_current() {
        let descriptor = user_descriptor();
        let base = StoredRowBatch::new(
            ObjectId::new(),
            "main",
            Vec::new(),
            encode_row(
                &descriptor,
                &[Value::Text("task".into()), Value::Boolean(false)],
            )
            .unwrap(),
            RowProvenance::for_insert("alice".to_string(), 10),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::GlobalServer),
        );
        let worker_done = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &descriptor,
                &[Value::Text("task".into()), Value::Boolean(true)],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "bob".to_string(), 20),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let edge_title = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &descriptor,
                &[Value::Text("edge-title".into()), Value::Boolean(false)],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "alice".to_string(), 30),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::EdgeServer),
        );
        let history_rows = vec![base.clone(), worker_done.clone(), edge_title.clone()];
        let row_by_batch_id = history_rows
            .iter()
            .cloned()
            .map(|row| (row.batch_id(), row))
            .collect::<HashMap<_, _>>();

        let entry = VisibleRowEntry::rebuild_with_descriptor(&descriptor, &history_rows)
            .unwrap()
            .expect("visible entry");

        assert_eq!(entry.current_row.batch_id(), edge_title.batch_id());
        assert_eq!(entry.edge_batch_id, Some(edge_title.batch_id()));
        assert_eq!(entry.edge_winner_ordinals, None);

        let edge_preview = entry
            .materialize_preview_for_tier_from_loaded_rows(
                &descriptor,
                DurabilityTier::EdgeServer,
                &row_by_batch_id,
            )
            .unwrap()
            .expect("edge preview");
        assert_eq!(
            decode_row(&descriptor, &edge_preview.data).unwrap(),
            vec![Value::Text("edge-title".into()), Value::Boolean(false)]
        );
    }

    #[test]
    fn visible_row_entry_persists_merged_tier_override_ordinals() {
        let descriptor = user_descriptor();
        let base = StoredRowBatch::new(
            ObjectId::new(),
            "main",
            Vec::new(),
            encode_row(
                &descriptor,
                &[Value::Text("task".into()), Value::Boolean(false)],
            )
            .unwrap(),
            RowProvenance::for_insert("alice".to_string(), 10),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::GlobalServer),
        );
        let edge_title = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &descriptor,
                &[Value::Text("edge-title".into()), Value::Boolean(false)],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "alice".to_string(), 20),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::EdgeServer),
        );
        let edge_done = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &descriptor,
                &[Value::Text("task".into()), Value::Boolean(true)],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "bob".to_string(), 21),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::EdgeServer),
        );
        let worker_current = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![edge_title.batch_id(), edge_done.batch_id()],
            encode_row(
                &descriptor,
                &[Value::Text("edge-title".into()), Value::Boolean(true)],
            )
            .unwrap(),
            RowProvenance::for_update(&edge_done.row_provenance(), "charlie".to_string(), 30),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let history_rows = vec![
            base.clone(),
            edge_title.clone(),
            edge_done.clone(),
            worker_current.clone(),
        ];
        let row_by_batch_id = history_rows
            .iter()
            .cloned()
            .map(|row| (row.batch_id(), row))
            .collect::<HashMap<_, _>>();

        let entry = VisibleRowEntry::rebuild_with_descriptor(&descriptor, &history_rows)
            .unwrap()
            .expect("visible entry");

        assert_eq!(entry.current_row.batch_id(), worker_current.batch_id());
        assert_eq!(entry.current_winner_ordinals, None);
        assert_eq!(entry.edge_batch_id, Some(edge_done.batch_id()));
        assert_eq!(
            entry.winner_batch_pool,
            vec![edge_title.batch_id(), edge_done.batch_id()]
        );
        assert_eq!(entry.edge_winner_ordinals, Some(vec![0, 1]));

        let edge_preview = entry
            .materialize_preview_for_tier_from_loaded_rows(
                &descriptor,
                DurabilityTier::EdgeServer,
                &row_by_batch_id,
            )
            .unwrap()
            .expect("edge preview");
        assert_eq!(edge_preview.batch_id(), edge_done.batch_id());
        assert_eq!(
            decode_row(&descriptor, &edge_preview.data).unwrap(),
            vec![Value::Text("edge-title".into()), Value::Boolean(true)]
        );
    }

    fn user_descriptor() -> RowDescriptor {
        RowDescriptor::new(vec![
            ColumnDescriptor::new("title", ColumnType::Text),
            ColumnDescriptor::new("done", ColumnType::Boolean),
        ])
    }

    fn lww_integer_descriptor() -> RowDescriptor {
        RowDescriptor::new(vec![
            ColumnDescriptor::new("title", ColumnType::Text),
            ColumnDescriptor::new("count", ColumnType::Integer),
        ])
    }

    fn counter_descriptor() -> RowDescriptor {
        RowDescriptor::new(vec![
            ColumnDescriptor::new("title", ColumnType::Text),
            ColumnDescriptor::new("count", ColumnType::Integer)
                .merge_strategy(ColumnMergeStrategy::Counter),
        ])
    }

    fn counter_only_descriptor() -> RowDescriptor {
        RowDescriptor::new(vec![
            ColumnDescriptor::new("count", ColumnType::Integer)
                .merge_strategy(ColumnMergeStrategy::Counter),
        ])
    }

    #[test]
    fn history_row_physical_descriptor_appends_nullable_user_columns() {
        let descriptor = history_row_physical_descriptor(&user_descriptor());

        let title = descriptor
            .column("title")
            .expect("physical descriptor should contain title");
        assert!(title.nullable, "physical user columns should be nullable");

        let done = descriptor
            .column("done")
            .expect("physical descriptor should contain done");
        assert!(done.nullable, "physical user columns should be nullable");
    }

    #[test]
    fn history_row_physical_descriptor_omits_key_derived_and_marker_columns() {
        let descriptor = history_row_physical_descriptor(&user_descriptor());

        assert_eq!(
            descriptor
                .columns
                .iter()
                .filter(|column| column.name == "_jazz_batch_id")
                .count(),
            0,
            "flat history rows should not store batch identity from the key in the payload"
        );
        assert!(
            descriptor.column("_jazz_format_id").is_none(),
            "flat history rows should not need an in-payload format marker once decoding is key-aware"
        );
        assert!(
            descriptor.column("_jazz_row_id").is_none(),
            "flat history rows should not store row id from the key in the payload"
        );
        assert!(
            descriptor.column("_jazz_branch").is_none(),
            "flat history rows should not store branch from the key in the payload"
        );
    }

    #[test]
    fn visible_row_physical_descriptor_keeps_current_batch_id_but_omits_marker() {
        let descriptor = visible_row_physical_descriptor(&user_descriptor());

        assert!(
            descriptor.column("_jazz_format_id").is_none(),
            "visible rows should not need an in-payload format marker once keyed decoding is available"
        );
        assert_eq!(
            descriptor
                .columns
                .iter()
                .filter(|column| column.name == "_jazz_batch_id")
                .count(),
            1,
            "visible rows should keep the current visible batch id in the flat payload"
        );
        assert!(
            descriptor.column("_jazz_row_id").is_none(),
            "visible rows should derive row id from the storage key"
        );
        assert!(
            descriptor.column("_jazz_branch").is_none(),
            "visible rows should derive branch from the storage key"
        );
        assert!(
            descriptor.column("_jazz_parents").is_none(),
            "visible rows should not duplicate history parents in the hot visible payload"
        );
        assert!(
            descriptor.column("_jazz_metadata").is_none(),
            "visible rows should not duplicate history metadata in the hot visible payload"
        );
        assert!(
            descriptor.column("_jazz_is_deleted").is_none(),
            "visible rows should derive deletion state from delete_kind in the hot payload"
        );
    }

    #[test]
    fn flat_visible_row_common_case_omits_empty_arrays_and_metadata() {
        let descriptor = user_descriptor();
        let current = visible_row(10, Some(DurabilityTier::Local));
        let entry = VisibleRowEntry::rebuild(current.clone(), std::slice::from_ref(&current));

        let encoded =
            encode_flat_visible_row_entry(&descriptor, &entry).expect("encode visible row");
        let values = decode_row(&visible_row_physical_descriptor(&descriptor), &encoded)
            .expect("decode visible row");

        assert_eq!(
            values[8],
            Value::Null,
            "singleton frontier matching current batch should be implicit"
        );
    }

    #[test]
    fn flat_history_row_binary_roundtrips_user_and_system_columns() {
        let user_descriptor = user_descriptor();
        let user_values = vec![Value::Text("Write docs".into()), Value::Boolean(false)];
        let user_data = crate::row_format::encode_row(&user_descriptor, &user_values).unwrap();
        let row = StoredRowBatch::new(
            ObjectId::from_uuid(Uuid::from_u128(42)),
            "main",
            vec![BatchId([9; 16])],
            user_data.clone(),
            RowProvenance {
                created_by: "alice".to_string(),
                created_at: 100,
                updated_by: "bob".to_string(),
                updated_at: 123,
            },
            HashMap::from([("source".to_string(), "test".to_string())]),
            RowState::VisibleTransactional,
            Some(DurabilityTier::EdgeServer),
        );

        let encoded =
            encode_flat_history_row(&user_descriptor, &row).expect("encode flat history row");
        let decoded = decode_flat_history_row(
            &user_descriptor,
            row.row_id,
            row.branch.as_str(),
            row.batch_id(),
            &encoded,
        )
        .expect("decode flat history row");

        assert_eq!(decoded, row);

        let physical_descriptor = history_row_physical_descriptor(&user_descriptor);
        let physical_values = decode_row(&physical_descriptor, &encoded).expect("decode values");
        assert_eq!(
            physical_values[physical_descriptor.column_index("title").unwrap()],
            Value::Text("Write docs".into())
        );
        assert_eq!(
            physical_values[physical_descriptor.column_index("done").unwrap()],
            Value::Boolean(false)
        );
    }

    #[test]
    fn flat_history_row_binary_roundtrips_nonempty_metadata() {
        let user_descriptor = user_descriptor();
        let row = StoredRowBatch::new(
            ObjectId::from_uuid(Uuid::from_u128(44)),
            "main",
            vec![BatchId([3; 16])],
            encode_row(
                &user_descriptor,
                &[Value::Text("Ship".into()), Value::Boolean(true)],
            )
            .expect("encode user row"),
            RowProvenance::for_insert("alice".to_string(), 100),
            HashMap::from([
                ("source".to_string(), "local".to_string()),
                ("kind".to_string(), "task".to_string()),
            ]),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );

        let encoded =
            encode_flat_history_row(&user_descriptor, &row).expect("encode flat history row");
        let decoded = decode_flat_history_row(
            &user_descriptor,
            row.row_id,
            row.branch.as_str(),
            row.batch_id(),
            &encoded,
        )
        .expect("decode flat history row");

        assert_eq!(decoded.metadata, row.metadata);
    }

    #[test]
    fn flat_history_row_hard_delete_uses_null_user_columns() {
        let user_descriptor = user_descriptor();
        let deleted = StoredRowBatch::new(
            ObjectId::from_uuid(Uuid::from_u128(43)),
            "main",
            vec![BatchId([7; 16])],
            vec![],
            RowProvenance::for_insert("alice".to_string(), 100),
            HashMap::from([(
                crate::metadata::MetadataKey::Delete.to_string(),
                "hard".to_string(),
            )]),
            RowState::VisibleDirect,
            None,
        );

        let encoded =
            encode_flat_history_row(&user_descriptor, &deleted).expect("encode hard delete");
        let physical_descriptor = history_row_physical_descriptor(&user_descriptor);
        let physical_values = decode_row(&physical_descriptor, &encoded).expect("decode values");

        assert_eq!(
            physical_values[physical_descriptor.column_index("title").unwrap()],
            Value::Null
        );
        assert_eq!(
            physical_values[physical_descriptor.column_index("done").unwrap()],
            Value::Null
        );

        let decoded = decode_flat_history_row(
            &user_descriptor,
            deleted.row_id,
            deleted.branch.as_str(),
            deleted.batch_id(),
            &encoded,
        )
        .expect("decode hard delete");
        assert_eq!(decoded.data.as_ref(), &[] as &[u8]);
        assert!(decoded.is_hard_deleted());
    }

    #[test]
    fn flat_history_row_binary_compacts_hot_enums_to_single_bytes() {
        let user_descriptor = user_descriptor();
        let mut row = StoredRowBatch::new(
            ObjectId::from_uuid(Uuid::from_u128(45)),
            "main",
            vec![BatchId([4; 16])],
            encode_row(
                &user_descriptor,
                &[Value::Text("Compact".into()), Value::Boolean(false)],
            )
            .expect("encode user row"),
            RowProvenance::for_insert("alice".to_string(), 100),
            HashMap::new(),
            RowState::VisibleTransactional,
            Some(DurabilityTier::EdgeServer),
        );
        row.delete_kind = Some(DeleteKind::Hard);

        let encoded =
            encode_flat_history_row(&user_descriptor, &row).expect("encode flat history row");
        let descriptor = history_row_physical_descriptor(&user_descriptor);
        let layout = crate::row_format::compiled_row_layout(&descriptor);

        let state = crate::row_format::column_bytes_with_layout(
            &descriptor,
            layout.as_ref(),
            &encoded,
            descriptor.column_index("_jazz_state").unwrap(),
        )
        .expect("read state bytes")
        .expect("state should be present");
        let tier = crate::row_format::column_bytes_with_layout(
            &descriptor,
            layout.as_ref(),
            &encoded,
            descriptor.column_index("_jazz_confirmed_tier").unwrap(),
        )
        .expect("read tier bytes")
        .expect("tier should be present");
        let delete_kind = crate::row_format::column_bytes_with_layout(
            &descriptor,
            layout.as_ref(),
            &encoded,
            descriptor.column_index("_jazz_delete_kind").unwrap(),
        )
        .expect("read delete kind bytes")
        .expect("delete kind should be present");

        assert_eq!(state.len(), 1);
        assert_eq!(tier.len(), 1);
        assert_eq!(delete_kind.len(), 1);
    }

    // ─── serial-write fast path ─────────────────────────────────────────────
    //
    // Fixtures for `fastpath::try_serial_fastpath_entry`. The bar throughout:
    // a constructed fast entry must equal `rebuild_with_descriptor` over the
    // full history byte for byte, and every unprovable shape must decline.
    // (The randomized cross-check lives in `storage::conformance_differential`.)

    fn root_batch(
        descriptor: &RowDescriptor,
        values: &[Value],
        updated_at: u64,
        confirmed_tier: Option<DurabilityTier>,
    ) -> StoredRowBatch {
        StoredRowBatch::new(
            ObjectId::new(),
            "main",
            Vec::new(),
            encode_row(descriptor, values).expect("encode root row"),
            RowProvenance::for_insert("alice".to_string(), updated_at),
            HashMap::new(),
            RowState::VisibleDirect,
            confirmed_tier,
        )
    }

    fn serial_batch(
        prev: &StoredRowBatch,
        descriptor: &RowDescriptor,
        values: &[Value],
        updated_at: u64,
        confirmed_tier: Option<DurabilityTier>,
    ) -> StoredRowBatch {
        StoredRowBatch::new(
            prev.row_id,
            "main",
            vec![prev.batch_id()],
            encode_row(descriptor, values).expect("encode serial row"),
            RowProvenance::for_update(&prev.row_provenance(), "bob".to_string(), updated_at),
            HashMap::new(),
            RowState::VisibleDirect,
            confirmed_tier,
        )
    }

    fn rebuilt_entry(descriptor: &RowDescriptor, history: &[StoredRowBatch]) -> VisibleRowEntry {
        VisibleRowEntry::rebuild_with_descriptor(descriptor, history)
            .expect("rebuild visible entry")
            .expect("history has a visible row")
    }

    #[test]
    fn history_fastpath_matches_full_rebuild_on_tier_sparse_serial_chain() {
        let _mode = force_history_fastpath(true);
        let descriptor = user_descriptor();
        let a = root_batch(
            &descriptor,
            &[Value::Text("v1".into()), Value::Boolean(false)],
            10,
            Some(DurabilityTier::GlobalServer),
        );
        let b = serial_batch(
            &a,
            &descriptor,
            &[Value::Text("v2".into()), Value::Boolean(true)],
            20,
            Some(DurabilityTier::EdgeServer),
        );
        let previous = rebuilt_entry(&descriptor, &[a.clone(), b.clone()]);
        assert_eq!(previous.worker_batch_id, None);
        assert_eq!(previous.edge_batch_id, None);
        assert_eq!(previous.global_batch_id, Some(a.batch_id()));

        // Unconfirmed serial append: old tip becomes the worker/edge pointer,
        // the deeper global pointer is carried verbatim.
        let c = serial_batch(
            &b,
            &descriptor,
            &[Value::Text("v3".into()), Value::Boolean(false)],
            30,
            None,
        );
        let fast = try_serial_fastpath_entry(Some(&previous), &c)
            .expect("unconfirmed serial append takes the fast path");
        let mut history = vec![a.clone(), b.clone(), c.clone()];
        assert_eq!(fast, rebuilt_entry(&descriptor, &history));
        assert_eq!(fast.branch_frontier, vec![c.batch_id()]);
        assert_eq!(fast.worker_batch_id, Some(b.batch_id()));
        assert_eq!(fast.edge_batch_id, Some(b.batch_id()));
        assert_eq!(fast.global_batch_id, Some(a.batch_id()));

        // Extending past an unconfirmed tip carries every pointer verbatim.
        let d = serial_batch(
            &c,
            &descriptor,
            &[Value::Text("v4".into()), Value::Boolean(true)],
            40,
            None,
        );
        let fast = try_serial_fastpath_entry(Some(&fast), &d)
            .expect("second unconfirmed serial append takes the fast path");
        history.push(d.clone());
        assert_eq!(fast, rebuilt_entry(&descriptor, &history));
        assert_eq!(fast.worker_batch_id, Some(b.batch_id()));
        assert_eq!(fast.edge_batch_id, Some(b.batch_id()));
        assert_eq!(fast.global_batch_id, Some(a.batch_id()));
    }

    #[test]
    fn history_fastpath_clears_pointers_when_confirmed_row_tops_unconfirmed_chain() {
        let _mode = force_history_fastpath(true);
        let descriptor = user_descriptor();
        let a = root_batch(
            &descriptor,
            &[Value::Text("v1".into()), Value::Boolean(false)],
            10,
            None,
        );
        let b = serial_batch(
            &a,
            &descriptor,
            &[Value::Text("v2".into()), Value::Boolean(true)],
            20,
            None,
        );
        let previous = rebuilt_entry(&descriptor, &[a.clone(), b.clone()]);

        // No older batch is confirmed anywhere (provably empty tier sets), so
        // a globally confirmed new row satisfies every tier itself: all three
        // pointers must be None, matching the full rebuild.
        let c = serial_batch(
            &b,
            &descriptor,
            &[Value::Text("v3".into()), Value::Boolean(false)],
            30,
            Some(DurabilityTier::GlobalServer),
        );
        let fast = try_serial_fastpath_entry(Some(&previous), &c)
            .expect("confirmed row over an unconfirmed chain takes the fast path");
        assert_eq!(fast, rebuilt_entry(&descriptor, &[a, b, c]));
        assert_eq!(fast.worker_batch_id, None);
        assert_eq!(fast.edge_batch_id, None);
        assert_eq!(fast.global_batch_id, None);
    }

    #[test]
    fn history_fastpath_declines_tier_confirmed_row_over_tier_hole() {
        let _mode = force_history_fastpath(true);
        let descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("title", ColumnType::Text),
            ColumnDescriptor::new("count", ColumnType::Integer)
                .merge_strategy(ColumnMergeStrategy::Counter),
            ColumnDescriptor::new(
                "tags",
                ColumnType::Array {
                    element: Box::new(ColumnType::Text),
                },
            )
            .merge_strategy(ColumnMergeStrategy::GSet),
        ]);
        // r2 is a tier hole: it never got confirmed, so r1 stays a concurrent
        // tip of every tier-filtered frontier even though the chain is linear.
        // The previous entry is still "clean" (no pointers, no ordinals)
        // because the r1/r3 tier merge coincidentally equals r3 (zero counter
        // delta, tag subset) — which is exactly why cleanliness of the entry
        // cannot prove hole-freedom.
        let r1 = root_batch(
            &descriptor,
            &[
                Value::Text("r1".into()),
                Value::Integer(0),
                Value::Array(vec![Value::Text("red".into())]),
            ],
            10,
            Some(DurabilityTier::GlobalServer),
        );
        let r2 = serial_batch(
            &r1,
            &descriptor,
            &[
                Value::Text("r2".into()),
                Value::Integer(0),
                Value::Array(vec![Value::Text("red".into())]),
            ],
            20,
            None,
        );
        let r3 = serial_batch(
            &r2,
            &descriptor,
            &[
                Value::Text("r3".into()),
                Value::Integer(5),
                Value::Array(vec![Value::Text("blue".into()), Value::Text("red".into())]),
            ],
            30,
            Some(DurabilityTier::GlobalServer),
        );
        let previous = rebuilt_entry(&descriptor, &[r1.clone(), r2.clone(), r3.clone()]);
        assert_eq!(previous.worker_batch_id, None);
        assert_eq!(previous.global_batch_id, None);
        assert!(previous.winner_batch_pool.is_empty());
        assert_eq!(previous.global_winner_ordinals, None);

        // A new globally confirmed row over a tier that already had satisfying
        // rows must decline: the rebuild resurfaces r1 as a concurrent tier
        // tip and stores a merged tier preview (populated pool/ordinals) that
        // is not derivable from the previous entry. A design-literal
        // "row satisfies tier ⇒ pointer None" fast entry would diverge here.
        let c = serial_batch(
            &r3,
            &descriptor,
            &[
                Value::Text("c".into()),
                Value::Integer(7),
                Value::Array(vec![Value::Text("green".into())]),
            ],
            40,
            Some(DurabilityTier::GlobalServer),
        );
        assert_eq!(try_serial_fastpath_entry(Some(&previous), &c), None);

        let rebuilt = rebuilt_entry(&descriptor, &[r1, r2, r3, c.clone()]);
        assert_eq!(rebuilt.global_batch_id, Some(c.batch_id()));
        assert!(rebuilt.global_winner_ordinals.is_some());
        assert!(!rebuilt.winner_batch_pool.is_empty());
    }

    #[test]
    fn history_fastpath_requires_exact_frontier_coverage() {
        let _mode = force_history_fastpath(true);
        let descriptor = user_descriptor();
        let base = root_batch(
            &descriptor,
            &[Value::Text("task".into()), Value::Boolean(false)],
            10,
            Some(DurabilityTier::Local),
        );
        let left = serial_batch(
            &base,
            &descriptor,
            &[Value::Text("left-title".into()), Value::Boolean(false)],
            20,
            Some(DurabilityTier::Local),
        );
        let right = serial_batch(
            &base,
            &descriptor,
            &[Value::Text("task".into()), Value::Boolean(true)],
            21,
            Some(DurabilityTier::Local),
        );
        let previous = rebuilt_entry(&descriptor, &[base.clone(), left.clone(), right.clone()]);
        assert_eq!(
            previous.branch_frontier,
            vec![left.batch_id(), right.batch_id()]
        );
        assert!(previous.current_winner_ordinals.is_some());

        let extend = |parents: Vec<BatchId>| {
            StoredRowBatch::new(
                base.row_id,
                "main",
                parents,
                encode_row(
                    &descriptor,
                    &[Value::Text("next".into()), Value::Boolean(true)],
                )
                .expect("encode extension row"),
                RowProvenance::for_update(&base.row_provenance(), "carol".to_string(), 30),
                HashMap::new(),
                RowState::VisibleDirect,
                None,
            )
        };

        // Single-parent append on a two-tip row: parents ⊂ frontier, decline.
        assert_eq!(
            try_serial_fastpath_entry(Some(&previous), &extend(vec![right.batch_id()])),
            None
        );
        // Duplicate parents never count as covering the frontier.
        assert_eq!(
            try_serial_fastpath_entry(
                Some(&previous),
                &extend(vec![right.batch_id(), right.batch_id()])
            ),
            None
        );
        // An explicit merge-commit naming every tip passes the set-equality
        // guard, but a live two-tip row carries merged-preview pool/ordinals
        // from its concurrent state, so the never-forked guard declines — in
        // practice merge-commits take the full path.
        assert_eq!(
            try_serial_fastpath_entry(
                Some(&previous),
                &extend(vec![left.batch_id(), right.batch_id()])
            ),
            None
        );
    }

    #[test]
    fn history_fastpath_declines_staging_delete_and_cross_branch_writes() {
        let _mode = force_history_fastpath(true);
        let descriptor = user_descriptor();
        let a = root_batch(
            &descriptor,
            &[Value::Text("v1".into()), Value::Boolean(false)],
            10,
            None,
        );
        let b = serial_batch(
            &a,
            &descriptor,
            &[Value::Text("v2".into()), Value::Boolean(true)],
            20,
            None,
        );
        let previous = rebuilt_entry(&descriptor, &[a.clone(), b.clone()]);

        // StagingPending must never become `current_row` through a shortcut:
        // it contributes nothing to the frontier, and shortcutting it would
        // make a staged batch publicly visible (supersede logic also stays on
        // the full path by construction).
        let mut staged = serial_batch(
            &b,
            &descriptor,
            &[Value::Text("staged".into()), Value::Boolean(false)],
            30,
            None,
        );
        staged.state = RowState::StagingPending;
        assert_eq!(try_serial_fastpath_entry(Some(&previous), &staged), None);

        let mut rejected = staged.clone();
        rejected.state = RowState::Rejected;
        assert_eq!(try_serial_fastpath_entry(Some(&previous), &rejected), None);

        // Deletes interact with the delete-winner preview overlay: full path.
        let soft_delete = StoredRowBatch::new(
            a.row_id,
            "main",
            vec![b.batch_id()],
            encode_row(
                &descriptor,
                &[Value::Text("gone".into()), Value::Boolean(false)],
            )
            .expect("encode delete row"),
            RowProvenance::for_update(&a.row_provenance(), "carol".to_string(), 31),
            HashMap::from([(MetadataKey::Delete.to_string(), "soft".to_string())]),
            RowState::VisibleDirect,
            None,
        );
        assert_eq!(
            try_serial_fastpath_entry(Some(&previous), &soft_delete),
            None
        );

        // Writes on another branch never consult this branch's entry.
        let mut cross_branch = serial_batch(
            &b,
            &descriptor,
            &[Value::Text("branched".into()), Value::Boolean(true)],
            32,
            None,
        );
        cross_branch.branch =
            crate::query_manager::types::SharedString::from("feature".to_string());
        assert_eq!(
            try_serial_fastpath_entry(Some(&previous), &cross_branch),
            None
        );

        // Without a previous entry there is nothing to carry forward.
        let fresh = serial_batch(
            &b,
            &descriptor,
            &[Value::Text("fresh".into()), Value::Boolean(true)],
            33,
            None,
        );
        assert_eq!(try_serial_fastpath_entry(None, &fresh), None);
    }

    #[test]
    fn history_fastpath_declines_linear_extension_after_resolved_fork_with_live_preview() {
        let _mode = force_history_fastpath(true);
        let descriptor = counter_descriptor();
        let base = root_batch(
            &descriptor,
            &[Value::Text("task".into()), Value::Integer(5)],
            10,
            Some(DurabilityTier::EdgeServer),
        );
        let left = serial_batch(
            &base,
            &descriptor,
            &[Value::Text("task".into()), Value::Integer(7)],
            20,
            Some(DurabilityTier::EdgeServer),
        );
        let right = serial_batch(
            &base,
            &descriptor,
            &[Value::Text("task".into()), Value::Integer(4)],
            21,
            Some(DurabilityTier::EdgeServer),
        );
        // The merge-commit resolves the fork on the unfiltered view, but the
        // edge-tier preview still merges {left, right} concurrently (the merge
        // commit is only Local-confirmed), keeping pool/ordinals live.
        let merge = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![left.batch_id(), right.batch_id()],
            encode_row(
                &descriptor,
                &[Value::Text("task".into()), Value::Integer(6)],
            )
            .expect("encode merge row"),
            RowProvenance::for_update(&base.row_provenance(), "carol".to_string(), 30),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let history = vec![base.clone(), left.clone(), right.clone(), merge.clone()];
        let previous = rebuilt_entry(&descriptor, &history);
        assert_eq!(previous.current_row.batch_id(), merge.batch_id());
        assert_eq!(previous.branch_frontier, vec![merge.batch_id()]);
        assert!(previous.edge_winner_ordinals.is_some());
        assert!(!previous.winner_batch_pool.is_empty());

        // Linear extension over the resolved fork: guards 1–5 pass, the
        // never-forked guard declines, and the full path keeps the live
        // merged tier preview intact.
        let c = serial_batch(
            &merge,
            &descriptor,
            &[Value::Text("task".into()), Value::Integer(6)],
            40,
            None,
        );
        assert_eq!(try_serial_fastpath_entry(Some(&previous), &c), None);

        let mut extended = history;
        extended.push(c.clone());
        let rebuilt = rebuilt_entry(&descriptor, &extended);
        assert_eq!(rebuilt.current_row.batch_id(), c.batch_id());
        assert_eq!(rebuilt.worker_batch_id, Some(merge.batch_id()));
        assert!(rebuilt.edge_winner_ordinals.is_some());
        assert_eq!(rebuilt.winner_batch_pool, previous.winner_batch_pool);
    }

    #[test]
    fn history_fastpath_serial_appends_through_storage_hit_and_match_rebuild() {
        let table = "fastpath_docs";
        let descriptor = user_descriptor();
        let schema: Schema = [(TableName::new(table), TableSchema::new(descriptor.clone()))]
            .into_iter()
            .collect();
        let mut storage = MemoryStorage::new();
        let schema_hash = crate::test_support::persist_test_schema(&mut storage, &schema);
        let branch = BranchName::new("main");

        let root = root_batch(
            &descriptor,
            &[Value::Text("v0".into()), Value::Boolean(false)],
            10,
            None,
        );
        let row_id = root.row_id;
        storage
            .put_row_locator(
                row_id,
                Some(&RowLocator {
                    table: table.into(),
                    origin_schema_hash: Some(schema_hash),
                }),
            )
            .expect("row locator should persist");

        let assert_stored_matches_rebuild = |storage: &MemoryStorage| {
            let history = storage
                .scan_history_region(table, "main", HistoryScan::Row { row_id })
                .expect("scan history");
            let expected = VisibleRowEntry::rebuild_with_descriptor(&descriptor, &history)
                .expect("rebuild entry")
                .expect("visible entry present");
            let stored = storage
                .load_visible_region_entry(table, "main", row_id)
                .expect("load stored entry")
                .expect("stored entry present");
            assert_eq!(stored, expected, "stored entry diverges from rebuild");
        };

        // Fast path forced ON: every serial append after the first must hit,
        // and the stored entry must stay byte-equal to the full rebuild.
        let mut prev = root.clone();
        {
            let _mode = force_history_fastpath(true);
            let hits_before = HISTORY_FASTPATH_HITS.load(std::sync::atomic::Ordering::Relaxed);
            apply_row_batch(&mut storage, row_id, &branch, root, &[]).expect("apply root");
            for i in 0..6u64 {
                let next = serial_batch(
                    &prev,
                    &descriptor,
                    &[
                        Value::Text(format!("v{}", i + 1)),
                        Value::Boolean(i % 2 == 0),
                    ],
                    20 + 10 * i,
                    None,
                );
                apply_row_batch(&mut storage, row_id, &branch, next.clone(), &[])
                    .expect("apply serial append");
                assert_stored_matches_rebuild(&storage);
                prev = next;
            }
            let hits_after = HISTORY_FASTPATH_HITS.load(std::sync::atomic::Ordering::Relaxed);
            assert!(
                hits_after >= hits_before + 6,
                "expected all 6 serial appends to take the fast path \
                 (hits before {hits_before}, after {hits_after})"
            );
        }

        // Kill switch (forced OFF): same writes, same stored bytes.
        {
            let _mode = force_history_fastpath(false);
            for i in 0..3u64 {
                let next = serial_batch(
                    &prev,
                    &descriptor,
                    &[Value::Text(format!("off{i}")), Value::Boolean(i % 2 == 1)],
                    100 + 10 * i,
                    None,
                );
                apply_row_batch(&mut storage, row_id, &branch, next.clone(), &[])
                    .expect("apply serial append with fast path off");
                assert_stored_matches_rebuild(&storage);
                prev = next;
            }
        }
    }

    // ─── in-place / patch fast paths ────────────────────────────────────────
    //
    // Fixtures for `fastpath::try_in_place_tip_update_entry` and the routing
    // in `patch_row_batch_state`. Same bar as the serial fixtures: a fast
    // entry must equal the full rebuild byte for byte, every unprovable shape
    // must decline, and `→ Rejected` must always take the full path.

    #[test]
    fn in_place_fastpath_tier_confirmation_matches_full_rebuild() {
        let _mode = force_history_fastpath(true);
        let descriptor = user_descriptor();
        let a = root_batch(
            &descriptor,
            &[Value::Text("v1".into()), Value::Boolean(false)],
            10,
            None,
        );
        let b = serial_batch(
            &a,
            &descriptor,
            &[Value::Text("v2".into()), Value::Boolean(true)],
            20,
            None,
        );
        let previous = rebuilt_entry(&descriptor, &[a.clone(), b.clone()]);
        assert_eq!(previous.worker_batch_id, None);
        assert_eq!(previous.edge_batch_id, None);
        assert_eq!(previous.global_batch_id, None);

        // Tier confirmation of the tip over an all-unconfirmed chain: every
        // tier set was provably empty (pointer None, old version
        // unsatisfying), so the confirmed tip satisfies each tier itself.
        let confirmed = b.accepted_transaction_output(DurabilityTier::GlobalServer);
        let fast = try_in_place_tip_update_entry(Some(&previous), &b, &confirmed)
            .expect("tier confirmation of the sole tip takes the in-place fast path");
        assert_eq!(
            fast,
            rebuilt_entry(&descriptor, &[a.clone(), confirmed.clone()])
        );
        assert_eq!(fast.branch_frontier, vec![b.batch_id()]);
        assert_eq!(fast.worker_batch_id, None);
        assert_eq!(fast.edge_batch_id, None);
        assert_eq!(fast.global_batch_id, None);

        // Next serial append re-arms the pointers (Fix A carry), and the
        // following confirmation must DECLINE: a `Some` pointer cannot prove
        // the tier set holds no hole-hidden concurrent tips, even though the
        // full rebuild happens to resolve back to None here.
        let c = serial_batch(
            &confirmed,
            &descriptor,
            &[Value::Text("v3".into()), Value::Boolean(false)],
            30,
            None,
        );
        let previous = rebuilt_entry(&descriptor, &[a.clone(), confirmed.clone(), c.clone()]);
        assert_eq!(previous.worker_batch_id, Some(b.batch_id()));
        assert_eq!(previous.global_batch_id, Some(b.batch_id()));

        let confirmed_c = c.accepted_transaction_output(DurabilityTier::GlobalServer);
        assert_eq!(
            try_in_place_tip_update_entry(Some(&previous), &c, &confirmed_c),
            None
        );
        let full = rebuilt_entry(&descriptor, &[a, confirmed, confirmed_c.clone()]);
        assert_eq!(full.current_row, confirmed_c);
        assert_eq!(full.global_batch_id, None);
    }

    #[test]
    fn in_place_fastpath_carries_pointers_when_tier_membership_unchanged() {
        let _mode = force_history_fastpath(true);
        let descriptor = user_descriptor();
        let a = root_batch(
            &descriptor,
            &[Value::Text("v1".into()), Value::Boolean(false)],
            10,
            Some(DurabilityTier::GlobalServer),
        );
        let b = serial_batch(
            &a,
            &descriptor,
            &[Value::Text("v2".into()), Value::Boolean(true)],
            20,
            Some(DurabilityTier::Local),
        );
        let previous = rebuilt_entry(&descriptor, &[a.clone(), b.clone()]);
        assert_eq!(previous.worker_batch_id, None);
        assert_eq!(previous.edge_batch_id, Some(a.batch_id()));
        assert_eq!(previous.global_batch_id, Some(a.batch_id()));

        // Same-tier re-confirmation (state VisibleDirect → VisibleTransactional,
        // tier unchanged): membership is unchanged for every tier, so all
        // pointers carry verbatim — including the deep `Some(a)` ones.
        let flipped = b.accepted_transaction_output(DurabilityTier::Local);
        assert!(flipped.state != b.state, "state must actually change");
        let fast = try_in_place_tip_update_entry(Some(&previous), &b, &flipped)
            .expect("membership-preserving state flip takes the in-place fast path");
        assert_eq!(fast, rebuilt_entry(&descriptor, &[a.clone(), flipped]));
        assert_eq!(fast.worker_batch_id, None);
        assert_eq!(fast.edge_batch_id, Some(a.batch_id()));
        assert_eq!(fast.global_batch_id, Some(a.batch_id()));
    }

    #[test]
    fn in_place_fastpath_declines_tier_confirmation_over_tier_hole() {
        let _mode = force_history_fastpath(true);
        let descriptor = counter_descriptor();
        // r2 is a tier hole: confirming r3 makes the global-filtered frontier
        // {r1, r3} (r3's parent r2 is not in the tier set, so r1 resurfaces),
        // and the counter delta forces a REAL merged preview with
        // pool/ordinals — the concrete divergence an in-place claim (pointer
        // None or carried Some(r1)) would produce.
        let r1 = root_batch(
            &descriptor,
            &[Value::Text("task".into()), Value::Integer(2)],
            10,
            Some(DurabilityTier::GlobalServer),
        );
        let r2 = serial_batch(
            &r1,
            &descriptor,
            &[Value::Text("task".into()), Value::Integer(5)],
            20,
            None,
        );
        let r3 = serial_batch(
            &r2,
            &descriptor,
            &[Value::Text("task".into()), Value::Integer(7)],
            30,
            None,
        );
        let previous = rebuilt_entry(&descriptor, &[r1.clone(), r2.clone(), r3.clone()]);
        assert_eq!(previous.global_batch_id, Some(r1.batch_id()));
        assert_eq!(previous.global_winner_ordinals, None);
        assert!(previous.winner_batch_pool.is_empty());

        let confirmed = r3.accepted_transaction_output(DurabilityTier::GlobalServer);
        assert_eq!(
            try_in_place_tip_update_entry(Some(&previous), &r3, &confirmed),
            None
        );

        let full = rebuilt_entry(&descriptor, &[r1, r2, confirmed]);
        assert!(
            full.global_winner_ordinals.is_some(),
            "the rebuild needs a merged global preview: {full:?}"
        );
        assert!(!full.winner_batch_pool.is_empty());
    }

    #[test]
    fn in_place_fastpath_declines_non_tip_forked_lowered_and_content_shapes() {
        let _mode = force_history_fastpath(true);
        let descriptor = user_descriptor();
        let a = root_batch(
            &descriptor,
            &[Value::Text("v1".into()), Value::Boolean(false)],
            10,
            None,
        );
        let b = serial_batch(
            &a,
            &descriptor,
            &[Value::Text("v2".into()), Value::Boolean(true)],
            20,
            None,
        );
        let previous = rebuilt_entry(&descriptor, &[a.clone(), b.clone()]);

        // Confirming a NON-tip batch: the frontier guard declines.
        let confirmed_a = a.accepted_transaction_output(DurabilityTier::GlobalServer);
        assert_eq!(
            try_in_place_tip_update_entry(Some(&previous), &a, &confirmed_a),
            None
        );

        // Content changed beyond state/tier: declines even on the tip.
        let mut retitled = b.accepted_transaction_output(DurabilityTier::GlobalServer);
        retitled.data = encode_row(
            &descriptor,
            &[Value::Text("rewritten".into()), Value::Boolean(true)],
        )
        .expect("encode replacement row")
        .into();
        assert_eq!(
            try_in_place_tip_update_entry(Some(&previous), &b, &retitled),
            None
        );

        // Visible → invisible and invisible → visible are other paths'
        // business (full path and publish fast path respectively).
        let mut rejected = b.clone();
        rejected.state = RowState::Rejected;
        assert_eq!(
            try_in_place_tip_update_entry(Some(&previous), &b, &rejected),
            None
        );
        let mut staged_existing = b.clone();
        staged_existing.state = RowState::StagingPending;
        assert_eq!(
            try_in_place_tip_update_entry(Some(&previous), &staged_existing, &b),
            None
        );

        // Tier LOWERING (leaves the tier set): a removal event — declines.
        let mut b_global = b.clone();
        b_global.confirmed_tier = Some(DurabilityTier::GlobalServer);
        let mut b_edge = b.clone();
        b_edge.confirmed_tier = Some(DurabilityTier::EdgeServer);
        let previous_global = rebuilt_entry(&descriptor, &[a.clone(), b_global.clone()]);
        assert_eq!(
            try_in_place_tip_update_entry(Some(&previous_global), &b_global, &b_edge),
            None
        );

        // Without a previous entry there is nothing to update in place.
        let confirmed_b = b.accepted_transaction_output(DurabilityTier::GlobalServer);
        assert_eq!(try_in_place_tip_update_entry(None, &b, &confirmed_b), None);

        // A forked (two-tip) entry declines via the frontier guard.
        let left = serial_batch(
            &a,
            &descriptor,
            &[Value::Text("left".into()), Value::Boolean(false)],
            21,
            None,
        );
        let forked = rebuilt_entry(&descriptor, &[a, b.clone(), left]);
        assert_eq!(forked.branch_frontier.len(), 2);
        assert_eq!(
            try_in_place_tip_update_entry(Some(&forked), &b, &confirmed_b),
            None
        );
    }

    #[test]
    fn patch_fastpath_staging_publish_and_tier_bump_hit_through_storage() {
        let _mode = force_history_fastpath(true);
        let table = "patch_fastpath_docs";
        let descriptor = user_descriptor();
        let schema: Schema = [(TableName::new(table), TableSchema::new(descriptor.clone()))]
            .into_iter()
            .collect();
        let mut storage = MemoryStorage::new();
        let schema_hash = crate::test_support::persist_test_schema(&mut storage, &schema);
        let branch = BranchName::new("main");

        let root = root_batch(
            &descriptor,
            &[Value::Text("v0".into()), Value::Boolean(false)],
            10,
            None,
        );
        let row_id = root.row_id;
        storage
            .put_row_locator(
                row_id,
                Some(&RowLocator {
                    table: table.into(),
                    origin_schema_hash: Some(schema_hash),
                }),
            )
            .expect("row locator should persist");
        apply_row_batch(&mut storage, row_id, &branch, root.clone(), &[]).expect("apply root");

        let assert_stored_matches_rebuild = |storage: &MemoryStorage| {
            let history = storage
                .scan_history_region(table, "main", HistoryScan::Row { row_id })
                .expect("scan history");
            let expected = VisibleRowEntry::rebuild_with_descriptor(&descriptor, &history)
                .expect("rebuild entry")
                .expect("visible entry present");
            let stored = storage
                .load_visible_region_entry(table, "main", row_id)
                .expect("load stored entry")
                .expect("stored entry present");
            assert_eq!(stored, expected, "stored entry diverges from rebuild");
            expected
        };

        let mut staged = serial_batch(
            &root,
            &descriptor,
            &[Value::Text("staged".into()), Value::Boolean(true)],
            20,
            None,
        );
        staged.state = RowState::StagingPending;
        apply_row_batch(&mut storage, row_id, &branch, staged.clone(), &[]).expect("apply staged");

        use std::sync::atomic::Ordering;
        let hits_before = PATCH_FASTPATH_HITS.load(Ordering::Relaxed);
        let fallbacks_before = PATCH_FASTPATH_FALLBACKS.load(Ordering::Relaxed);

        // Staging publish (`runtime_core/writes.rs` shape): parents cover the
        // frontier, never-forked ⇒ the publish rides the domination fast path.
        let change = patch_row_batch_state(
            &mut storage,
            row_id,
            &branch,
            staged.batch_id(),
            Some(RowState::VisibleDirect),
            None,
        )
        .expect("publish staged batch")
        .expect("publish changes visibility");
        assert_eq!(change.row.batch_id(), staged.batch_id());
        let entry = assert_stored_matches_rebuild(&storage);
        assert_eq!(entry.current_row.batch_id(), staged.batch_id());
        assert_eq!(entry.branch_frontier, vec![staged.batch_id()]);

        // Pure tier bump on the (now published) tip: the in-place fast path.
        patch_row_batch_state(
            &mut storage,
            row_id,
            &branch,
            staged.batch_id(),
            None,
            Some(DurabilityTier::GlobalServer),
        )
        .expect("tier bump on tip");
        let entry = assert_stored_matches_rebuild(&storage);
        assert_eq!(
            entry.current_row.confirmed_tier,
            Some(DurabilityTier::GlobalServer)
        );
        assert_eq!(entry.worker_batch_id, None);
        assert_eq!(entry.global_batch_id, None);

        assert_eq!(
            PATCH_FASTPATH_HITS.load(Ordering::Relaxed),
            hits_before + 2,
            "publish and tier bump should both hit the patch fast path"
        );
        assert_eq!(
            PATCH_FASTPATH_FALLBACKS.load(Ordering::Relaxed),
            fallbacks_before
        );

        // Publish with a concurrent visible sibling: the flip IS a fork
        // event — the fast path declines and the full path merges both tips.
        let mut staged_two = serial_batch(
            &staged,
            &descriptor,
            &[Value::Text("staged-two".into()), Value::Boolean(false)],
            30,
            None,
        );
        staged_two.state = RowState::StagingPending;
        apply_row_batch(&mut storage, row_id, &branch, staged_two.clone(), &[])
            .expect("apply second staged batch");
        let sibling = serial_batch(
            &staged,
            &descriptor,
            &[Value::Text("sibling".into()), Value::Boolean(true)],
            31,
            None,
        );
        apply_row_batch(&mut storage, row_id, &branch, sibling.clone(), &[])
            .expect("apply visible sibling");

        patch_row_batch_state(
            &mut storage,
            row_id,
            &branch,
            staged_two.batch_id(),
            Some(RowState::VisibleDirect),
            None,
        )
        .expect("publish staged batch with concurrent sibling");
        let entry = assert_stored_matches_rebuild(&storage);
        assert_eq!(entry.branch_frontier.len(), 2, "both tips must survive");
        assert_eq!(
            PATCH_FASTPATH_FALLBACKS.load(Ordering::Relaxed),
            fallbacks_before + 1,
            "the forked publish must fall back to the full path"
        );
    }

    #[test]
    fn patch_fastpath_rejection_of_sole_tip_reexposes_ancestor() {
        let _mode = force_history_fastpath(true);
        let table = "patch_reject_docs";
        let descriptor = user_descriptor();
        let schema: Schema = [(TableName::new(table), TableSchema::new(descriptor.clone()))]
            .into_iter()
            .collect();
        let mut storage = MemoryStorage::new();
        let schema_hash = crate::test_support::persist_test_schema(&mut storage, &schema);
        let branch = BranchName::new("main");

        let a = root_batch(
            &descriptor,
            &[Value::Text("v1".into()), Value::Boolean(false)],
            10,
            Some(DurabilityTier::GlobalServer),
        );
        let row_id = a.row_id;
        storage
            .put_row_locator(
                row_id,
                Some(&RowLocator {
                    table: table.into(),
                    origin_schema_hash: Some(schema_hash),
                }),
            )
            .expect("row locator should persist");
        apply_row_batch(&mut storage, row_id, &branch, a.clone(), &[]).expect("apply a");
        let b = serial_batch(
            &a,
            &descriptor,
            &[Value::Text("v2".into()), Value::Boolean(true)],
            20,
            None,
        );
        apply_row_batch(&mut storage, row_id, &branch, b.clone(), &[]).expect("apply b");

        use std::sync::atomic::Ordering;
        let hits_before = PATCH_FASTPATH_HITS.load(Ordering::Relaxed);
        let fallbacks_before = PATCH_FASTPATH_FALLBACKS.load(Ordering::Relaxed);

        // Rejecting the sole frontier tip must take the full path and
        // re-expose the superseded ancestor as the visible row.
        let change = patch_row_batch_state(
            &mut storage,
            row_id,
            &branch,
            b.batch_id(),
            Some(RowState::Rejected),
            None,
        )
        .expect("reject tip")
        .expect("rejection changes visibility");
        assert_eq!(change.row.batch_id(), a.batch_id());
        assert_eq!(
            change.previous_row.as_ref().map(|row| row.batch_id()),
            Some(b.batch_id())
        );

        let history = storage
            .scan_history_region(table, "main", HistoryScan::Row { row_id })
            .expect("scan history");
        let expected = VisibleRowEntry::rebuild_with_descriptor(&descriptor, &history)
            .expect("rebuild entry")
            .expect("visible entry present");
        let stored = storage
            .load_visible_region_entry(table, "main", row_id)
            .expect("load stored entry")
            .expect("stored entry present");
        assert_eq!(stored, expected);
        assert_eq!(stored.current_row.batch_id(), a.batch_id());
        assert_eq!(stored.branch_frontier, vec![a.batch_id()]);

        // Rejections are full-path by design: not part of the fast-path
        // population, so neither counter moves.
        assert_eq!(PATCH_FASTPATH_HITS.load(Ordering::Relaxed), hits_before);
        assert_eq!(
            PATCH_FASTPATH_FALLBACKS.load(Ordering::Relaxed),
            fallbacks_before
        );

        // Rejecting the re-exposed root empties the visible set entirely.
        let change = patch_row_batch_state(
            &mut storage,
            row_id,
            &branch,
            a.batch_id(),
            Some(RowState::Rejected),
            None,
        )
        .expect("reject root");
        assert_eq!(change, None);
        assert_eq!(
            storage
                .load_visible_region_entry(table, "main", row_id)
                .expect("load stored entry"),
            None
        );
    }

    #[test]
    fn direct_row_writes_use_batch_identity() {
        let provenance = RowProvenance::for_insert("alice".to_string(), 100);
        let first = StoredRowBatch::new(
            ObjectId::from_uuid(Uuid::from_u128(101)),
            "main",
            Vec::new(),
            vec![1, 2, 3],
            provenance.clone(),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );

        assert_eq!(
            first.batch_id(),
            first.batch_id,
            "direct visible rows should publish under their batch identity"
        );
    }
}
