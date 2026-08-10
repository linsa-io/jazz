use super::*;
use crate::batch_fate::{CapturedFrontierMember, SealedBatchMember, SealedBatchSubmission};
use crate::query_manager::policy::PolicyExpr;
use crate::query_manager::query::QueryBuilder;
use crate::query_manager::session::WriteContext;
use crate::query_manager::types::{
    ColumnType, SchemaBuilder, SchemaHash, TableName, TablePolicies, TableSchema,
};
use crate::row_format::encode_row;
use crate::row_histories::BatchId;
use crate::schema_manager::AppId;
use crate::storage::{
    MemoryStorage, RawTableKeys, RawTableRows, RowLocator, Storage, StorageError,
};
use crate::sync_manager::{
    ClientId, ClientRole, Destination, DurabilityTier, InboxEntry, OutboxEntry, ServerId, Source,
    SyncError, SyncManager, SyncPayload,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

type TestCore = RuntimeCore<MemoryStorage, NoopScheduler>;
type BoxedStorageTestCore = RuntimeCore<Box<dyn Storage>, NoopScheduler>;

fn new_test_core<S: Storage, Sch: Scheduler>(
    schema_manager: SchemaManager,
    storage: S,
    scheduler: Sch,
) -> RuntimeCore<S, Sch> {
    let mut core = RuntimeCore::new(schema_manager, storage, scheduler);
    core.set_sync_sender(Box::new(VecSyncSender::new()));
    core
}

struct RowRegionReadFailingStorage {
    inner: MemoryStorage,
    fail_visible_row_reads: bool,
    fail_row_locator_scans: bool,
    fail_sealed_submission_upserts: Arc<Mutex<bool>>,
    fail_prepared_row_mutations: Arc<Mutex<bool>>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct LegacyStorageCallCounts;

struct LegacyPersistenceObservingStorage {
    inner: MemoryStorage,
    _calls: Arc<Mutex<LegacyStorageCallCounts>>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct RowMutationCallCounts {
    row_mutation_calls: usize,
    separate_index_mutation_calls: usize,
    flush_wal_calls: usize,
    local_batch_record_get_calls: usize,
}

struct RowMutationObservingStorage {
    inner: MemoryStorage,
    calls: Arc<Mutex<RowMutationCallCounts>>,
}

#[derive(Clone, Default)]
struct CountingScheduler {
    schedule_calls: Arc<Mutex<usize>>,
}

impl RowRegionReadFailingStorage {
    fn new() -> Self {
        Self {
            inner: MemoryStorage::new(),
            fail_visible_row_reads: true,
            fail_row_locator_scans: false,
            fail_sealed_submission_upserts: Arc::new(Mutex::new(false)),
            fail_prepared_row_mutations: Arc::new(Mutex::new(false)),
        }
    }

    fn with_row_locator_scan_failure() -> Self {
        Self {
            inner: MemoryStorage::new(),
            fail_visible_row_reads: false,
            fail_row_locator_scans: true,
            fail_sealed_submission_upserts: Arc::new(Mutex::new(false)),
            fail_prepared_row_mutations: Arc::new(Mutex::new(false)),
        }
    }

    fn with_sealed_submission_upsert_failure(
        fail_sealed_submission_upserts: Arc<Mutex<bool>>,
    ) -> Self {
        Self {
            inner: MemoryStorage::new(),
            fail_visible_row_reads: false,
            fail_row_locator_scans: false,
            fail_sealed_submission_upserts,
            fail_prepared_row_mutations: Arc::new(Mutex::new(false)),
        }
    }

    fn with_prepared_row_mutation_failure(fail_prepared_row_mutations: Arc<Mutex<bool>>) -> Self {
        Self {
            inner: MemoryStorage::new(),
            fail_visible_row_reads: false,
            fail_row_locator_scans: false,
            fail_sealed_submission_upserts: Arc::new(Mutex::new(false)),
            fail_prepared_row_mutations,
        }
    }
}

impl LegacyPersistenceObservingStorage {
    fn new(calls: Arc<Mutex<LegacyStorageCallCounts>>) -> Self {
        Self {
            inner: MemoryStorage::new(),
            _calls: calls,
        }
    }
}

impl RowMutationObservingStorage {
    fn new(calls: Arc<Mutex<RowMutationCallCounts>>) -> Self {
        Self {
            inner: MemoryStorage::new(),
            calls,
        }
    }
}

impl CountingScheduler {
    fn schedule_count(&self) -> usize {
        *self.schedule_calls.lock().unwrap()
    }
}

impl Scheduler for CountingScheduler {
    fn schedule_batched_tick(&self) {
        *self.schedule_calls.lock().unwrap() += 1;
    }
}

impl Storage for RowRegionReadFailingStorage {
    fn apply_encoded_row_mutation(
        &mut self,
        table: &str,
        history_rows: &[crate::storage::OwnedHistoryRowBytes],
        visible_rows: &[crate::storage::OwnedVisibleRowBytes],
        index_mutations: &[crate::storage::IndexMutation<'_>],
    ) -> Result<(), StorageError> {
        self.inner
            .apply_encoded_row_mutation(table, history_rows, visible_rows, index_mutations)
    }

    fn apply_prepared_row_mutation(
        &mut self,
        table: &str,
        history_rows: &[crate::row_histories::StoredRowBatch],
        visible_entries: &[crate::row_histories::VisibleRowEntry],
        encoded_history_rows: &[crate::storage::OwnedHistoryRowBytes],
        encoded_visible_rows: &[crate::storage::OwnedVisibleRowBytes],
        index_mutations: &[crate::storage::IndexMutation<'_>],
    ) -> Result<(), StorageError> {
        if *self.fail_prepared_row_mutations.lock().unwrap() {
            return Err(StorageError::IoError(
                "prepared row mutations deliberately disabled in this test".to_string(),
            ));
        }
        self.inner.apply_prepared_row_mutation(
            table,
            history_rows,
            visible_entries,
            encoded_history_rows,
            encoded_visible_rows,
            index_mutations,
        )
    }

    fn scan_row_locators(&self) -> Result<crate::storage::RowLocatorRows, StorageError> {
        if self.fail_row_locator_scans {
            return Err(StorageError::IoError(
                "row-locator scans deliberately disabled in this test".to_string(),
            ));
        }
        self.inner.scan_row_locators()
    }

    fn load_row_locator(
        &self,
        id: ObjectId,
    ) -> Result<Option<crate::storage::RowLocator>, StorageError> {
        self.inner.load_row_locator(id)
    }

    fn put_row_locator(
        &mut self,
        id: ObjectId,
        locator: Option<&crate::storage::RowLocator>,
    ) -> Result<(), StorageError> {
        self.inner.put_row_locator(id, locator)
    }

    fn upsert_sealed_batch_submission(
        &mut self,
        submission: &SealedBatchSubmission,
    ) -> Result<(), StorageError> {
        if *self.fail_sealed_submission_upserts.lock().unwrap() {
            return Err(StorageError::IoError(
                "sealed submission upserts deliberately disabled in this test".to_string(),
            ));
        }
        self.inner.upsert_sealed_batch_submission(submission)
    }

    fn raw_table_put(&mut self, table: &str, key: &str, value: &[u8]) -> Result<(), StorageError> {
        self.inner.raw_table_put(table, key, value)
    }

    fn raw_table_delete(&mut self, table: &str, key: &str) -> Result<(), StorageError> {
        self.inner.raw_table_delete(table, key)
    }

    fn raw_table_get(&self, table: &str, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        self.inner.raw_table_get(table, key)
    }

    fn raw_table_scan_prefix(
        &self,
        table: &str,
        prefix: &str,
    ) -> Result<RawTableRows, StorageError> {
        self.inner.raw_table_scan_prefix(table, prefix)
    }

    fn raw_table_scan_prefix_keys(
        &self,
        table: &str,
        prefix: &str,
    ) -> Result<RawTableKeys, StorageError> {
        self.inner.raw_table_scan_prefix_keys(table, prefix)
    }

    fn raw_table_scan_range(
        &self,
        table: &str,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<RawTableRows, StorageError> {
        self.inner.raw_table_scan_range(table, start, end)
    }

    fn raw_table_scan_range_keys(
        &self,
        table: &str,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<RawTableKeys, StorageError> {
        self.inner.raw_table_scan_range_keys(table, start, end)
    }

    fn append_history_region_rows(
        &mut self,
        table: &str,
        rows: &[crate::row_histories::StoredRowBatch],
    ) -> Result<(), StorageError> {
        self.inner.append_history_region_rows(table, rows)
    }

    fn append_history_region_row_bytes(
        &mut self,
        table: &str,
        rows: &[crate::storage::HistoryRowBytes<'_>],
    ) -> Result<(), StorageError> {
        self.inner.append_history_region_row_bytes(table, rows)
    }

    fn upsert_visible_region_rows(
        &mut self,
        table: &str,
        entries: &[crate::row_histories::VisibleRowEntry],
    ) -> Result<(), StorageError> {
        self.inner.upsert_visible_region_rows(table, entries)
    }

    fn delete_visible_region_row(
        &mut self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        self.inner.delete_visible_region_row(table, branch, row_id)
    }

    fn patch_row_region_rows_by_batch(
        &mut self,
        table: &str,
        batch_id: crate::row_histories::BatchId,
        state: Option<crate::row_histories::RowState>,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Result<(), StorageError> {
        self.inner
            .patch_row_region_rows_by_batch(table, batch_id, state, confirmed_tier)
    }

    fn patch_exact_row_batch(
        &mut self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
        state: Option<crate::row_histories::RowState>,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Result<bool, StorageError> {
        self.inner
            .patch_exact_row_batch(table, branch, row_id, batch_id, state, confirmed_tier)
    }

    fn patch_exact_row_batch_for_schema_hash(
        &mut self,
        table: &str,
        schema_hash: crate::query_manager::types::SchemaHash,
        branch: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
        state: Option<crate::row_histories::RowState>,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Result<bool, StorageError> {
        self.inner.patch_exact_row_batch_for_schema_hash(
            table,
            schema_hash,
            branch,
            row_id,
            batch_id,
            state,
            confirmed_tier,
        )
    }

    fn scan_visible_region(
        &self,
        table: &str,
        branch: &str,
    ) -> Result<Vec<crate::row_histories::StoredRowBatch>, StorageError> {
        self.inner.scan_visible_region(table, branch)
    }

    fn load_visible_region_row(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<crate::row_histories::StoredRowBatch>, StorageError> {
        if self.fail_visible_row_reads {
            return Err(StorageError::IoError(
                "row-history reads deliberately disabled in this test".to_string(),
            ));
        }
        self.inner.load_visible_region_row(table, branch, row_id)
    }

    fn load_visible_region_frontier(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<Vec<crate::row_histories::BatchId>>, StorageError> {
        self.inner
            .load_visible_region_frontier(table, branch, row_id)
    }

    fn capture_family_visible_frontier(
        &self,
        target_branch_name: crate::object::BranchName,
    ) -> Result<Vec<crate::batch_fate::CapturedFrontierMember>, StorageError> {
        self.inner
            .capture_family_visible_frontier(target_branch_name)
    }

    fn scan_visible_region_row_batches(
        &self,
        table: &str,
        row_id: ObjectId,
    ) -> Result<Vec<crate::row_histories::StoredRowBatch>, StorageError> {
        self.inner.scan_visible_region_row_batches(table, row_id)
    }

    fn scan_history_row_batches(
        &self,
        table: &str,
        row_id: ObjectId,
    ) -> Result<Vec<crate::row_histories::StoredRowBatch>, StorageError> {
        self.inner.scan_history_row_batches(table, row_id)
    }

    fn load_history_row_batch(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
    ) -> Result<Option<crate::row_histories::StoredRowBatch>, StorageError> {
        self.inner
            .load_history_row_batch(table, branch, row_id, batch_id)
    }

    fn load_history_query_row_batch(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
    ) -> Result<Option<crate::row_histories::QueryRowBatch>, StorageError> {
        self.inner
            .load_history_query_row_batch(table, branch, row_id, batch_id)
    }

    fn load_history_row_batch_for_schema_hash(
        &self,
        table: &str,
        schema_hash: crate::query_manager::types::SchemaHash,
        branch: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
    ) -> Result<Option<crate::row_histories::StoredRowBatch>, StorageError> {
        self.inner.load_history_row_batch_for_schema_hash(
            table,
            schema_hash,
            branch,
            row_id,
            batch_id,
        )
    }

    fn load_history_row_batch_any_branch(
        &self,
        table: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
    ) -> Result<Option<crate::row_histories::StoredRowBatch>, StorageError> {
        self.inner
            .load_history_row_batch_any_branch(table, row_id, batch_id)
    }

    fn load_history_query_row_batch_any_branch(
        &self,
        table: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
    ) -> Result<Option<crate::row_histories::QueryRowBatch>, StorageError> {
        self.inner
            .load_history_query_row_batch_any_branch(table, row_id, batch_id)
    }

    fn row_batch_exists(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
    ) -> Result<bool, StorageError> {
        self.inner.row_batch_exists(table, branch, row_id, batch_id)
    }

    fn scan_row_branch_tip_ids(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Vec<crate::row_histories::BatchId>, StorageError> {
        self.inner.scan_row_branch_tip_ids(table, branch, row_id)
    }

    fn load_history_row_batch_bytes(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        self.inner
            .load_history_row_batch_bytes(table, branch, row_id, batch_id)
    }

    fn scan_history_region_bytes(
        &self,
        table: &str,
        scan: crate::row_histories::HistoryScan,
    ) -> Result<Vec<Vec<u8>>, StorageError> {
        self.inner.scan_history_region_bytes(table, scan)
    }

    fn scan_history_region(
        &self,
        table: &str,
        branch: &str,
        scan: crate::row_histories::HistoryScan,
    ) -> Result<Vec<crate::row_histories::StoredRowBatch>, StorageError> {
        self.inner.scan_history_region(table, branch, scan)
    }

    fn index_insert(
        &mut self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        self.inner
            .index_insert(table, column, branch, value, row_id)
    }

    fn index_remove(
        &mut self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        self.inner
            .index_remove(table, column, branch, value, row_id)
    }

    fn index_lookup(
        &self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
    ) -> Vec<ObjectId> {
        self.inner.index_lookup(table, column, branch, value)
    }

    fn index_range(
        &self,
        table: &str,
        column: &str,
        branch: &str,
        start: std::ops::Bound<&Value>,
        end: std::ops::Bound<&Value>,
    ) -> Vec<ObjectId> {
        self.inner.index_range(table, column, branch, start, end)
    }

    fn index_scan_all(&self, table: &str, column: &str, branch: &str) -> Vec<ObjectId> {
        self.inner.index_scan_all(table, column, branch)
    }

    fn flush(&self) -> Result<(), StorageError> {
        self.inner.flush()
    }

    fn flush_wal(&self) -> Result<(), StorageError> {
        self.inner.flush_wal()
    }

    fn close(&self) -> Result<(), StorageError> {
        self.inner.close()
    }
}

impl Storage for LegacyPersistenceObservingStorage {
    fn apply_encoded_row_mutation(
        &mut self,
        table: &str,
        history_rows: &[crate::storage::OwnedHistoryRowBytes],
        visible_rows: &[crate::storage::OwnedVisibleRowBytes],
        index_mutations: &[crate::storage::IndexMutation<'_>],
    ) -> Result<(), StorageError> {
        self.inner
            .apply_encoded_row_mutation(table, history_rows, visible_rows, index_mutations)
    }

    fn apply_prepared_row_mutation(
        &mut self,
        table: &str,
        history_rows: &[crate::row_histories::StoredRowBatch],
        visible_entries: &[crate::row_histories::VisibleRowEntry],
        encoded_history_rows: &[crate::storage::OwnedHistoryRowBytes],
        encoded_visible_rows: &[crate::storage::OwnedVisibleRowBytes],
        index_mutations: &[crate::storage::IndexMutation<'_>],
    ) -> Result<(), StorageError> {
        self.inner.apply_prepared_row_mutation(
            table,
            history_rows,
            visible_entries,
            encoded_history_rows,
            encoded_visible_rows,
            index_mutations,
        )
    }

    fn scan_row_locators(&self) -> Result<crate::storage::RowLocatorRows, StorageError> {
        self.inner.scan_row_locators()
    }

    fn load_row_locator(
        &self,
        id: ObjectId,
    ) -> Result<Option<crate::storage::RowLocator>, StorageError> {
        self.inner.load_row_locator(id)
    }

    fn put_row_locator(
        &mut self,
        id: ObjectId,
        locator: Option<&crate::storage::RowLocator>,
    ) -> Result<(), StorageError> {
        self.inner.put_row_locator(id, locator)
    }

    fn raw_table_put(&mut self, table: &str, key: &str, value: &[u8]) -> Result<(), StorageError> {
        self.inner.raw_table_put(table, key, value)
    }

    fn raw_table_delete(&mut self, table: &str, key: &str) -> Result<(), StorageError> {
        self.inner.raw_table_delete(table, key)
    }

    fn raw_table_get(&self, table: &str, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        self.inner.raw_table_get(table, key)
    }

    fn raw_table_scan_prefix(
        &self,
        table: &str,
        prefix: &str,
    ) -> Result<RawTableRows, StorageError> {
        self.inner.raw_table_scan_prefix(table, prefix)
    }

    fn raw_table_scan_prefix_keys(
        &self,
        table: &str,
        prefix: &str,
    ) -> Result<RawTableKeys, StorageError> {
        self.inner.raw_table_scan_prefix_keys(table, prefix)
    }

    fn raw_table_scan_range(
        &self,
        table: &str,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<RawTableRows, StorageError> {
        self.inner.raw_table_scan_range(table, start, end)
    }

    fn raw_table_scan_range_keys(
        &self,
        table: &str,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<RawTableKeys, StorageError> {
        self.inner.raw_table_scan_range_keys(table, start, end)
    }

    fn append_history_region_rows(
        &mut self,
        table: &str,
        rows: &[crate::row_histories::StoredRowBatch],
    ) -> Result<(), StorageError> {
        self.inner.append_history_region_rows(table, rows)
    }

    fn append_history_region_row_bytes(
        &mut self,
        table: &str,
        rows: &[crate::storage::HistoryRowBytes<'_>],
    ) -> Result<(), StorageError> {
        self.inner.append_history_region_row_bytes(table, rows)
    }

    fn upsert_visible_region_rows(
        &mut self,
        table: &str,
        entries: &[crate::row_histories::VisibleRowEntry],
    ) -> Result<(), StorageError> {
        self.inner.upsert_visible_region_rows(table, entries)
    }

    fn delete_visible_region_row(
        &mut self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        self.inner.delete_visible_region_row(table, branch, row_id)
    }

    fn patch_row_region_rows_by_batch(
        &mut self,
        table: &str,
        batch_id: crate::row_histories::BatchId,
        state: Option<crate::row_histories::RowState>,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Result<(), StorageError> {
        self.inner
            .patch_row_region_rows_by_batch(table, batch_id, state, confirmed_tier)
    }

    fn patch_exact_row_batch(
        &mut self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
        state: Option<crate::row_histories::RowState>,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Result<bool, StorageError> {
        self.inner
            .patch_exact_row_batch(table, branch, row_id, batch_id, state, confirmed_tier)
    }

    fn patch_exact_row_batch_for_schema_hash(
        &mut self,
        table: &str,
        schema_hash: crate::query_manager::types::SchemaHash,
        branch: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
        state: Option<crate::row_histories::RowState>,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Result<bool, StorageError> {
        self.inner.patch_exact_row_batch_for_schema_hash(
            table,
            schema_hash,
            branch,
            row_id,
            batch_id,
            state,
            confirmed_tier,
        )
    }

    fn scan_visible_region(
        &self,
        table: &str,
        branch: &str,
    ) -> Result<Vec<crate::row_histories::StoredRowBatch>, StorageError> {
        self.inner.scan_visible_region(table, branch)
    }

    fn load_visible_region_row(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<crate::row_histories::StoredRowBatch>, StorageError> {
        self.inner.load_visible_region_row(table, branch, row_id)
    }

    fn load_visible_region_frontier(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<Vec<crate::row_histories::BatchId>>, StorageError> {
        self.inner
            .load_visible_region_frontier(table, branch, row_id)
    }

    fn capture_family_visible_frontier(
        &self,
        target_branch_name: crate::object::BranchName,
    ) -> Result<Vec<crate::batch_fate::CapturedFrontierMember>, StorageError> {
        self.inner
            .capture_family_visible_frontier(target_branch_name)
    }

    fn scan_visible_region_row_batches(
        &self,
        table: &str,
        row_id: ObjectId,
    ) -> Result<Vec<crate::row_histories::StoredRowBatch>, StorageError> {
        self.inner.scan_visible_region_row_batches(table, row_id)
    }

    fn scan_history_row_batches(
        &self,
        table: &str,
        row_id: ObjectId,
    ) -> Result<Vec<crate::row_histories::StoredRowBatch>, StorageError> {
        self.inner.scan_history_row_batches(table, row_id)
    }

    fn load_history_row_batch(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
    ) -> Result<Option<crate::row_histories::StoredRowBatch>, StorageError> {
        self.inner
            .load_history_row_batch(table, branch, row_id, batch_id)
    }

    fn load_history_query_row_batch(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
    ) -> Result<Option<crate::row_histories::QueryRowBatch>, StorageError> {
        self.inner
            .load_history_query_row_batch(table, branch, row_id, batch_id)
    }

    fn load_history_row_batch_for_schema_hash(
        &self,
        table: &str,
        schema_hash: crate::query_manager::types::SchemaHash,
        branch: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
    ) -> Result<Option<crate::row_histories::StoredRowBatch>, StorageError> {
        self.inner.load_history_row_batch_for_schema_hash(
            table,
            schema_hash,
            branch,
            row_id,
            batch_id,
        )
    }

    fn load_history_row_batch_any_branch(
        &self,
        table: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
    ) -> Result<Option<crate::row_histories::StoredRowBatch>, StorageError> {
        self.inner
            .load_history_row_batch_any_branch(table, row_id, batch_id)
    }

    fn load_history_query_row_batch_any_branch(
        &self,
        table: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
    ) -> Result<Option<crate::row_histories::QueryRowBatch>, StorageError> {
        self.inner
            .load_history_query_row_batch_any_branch(table, row_id, batch_id)
    }

    fn row_batch_exists(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
    ) -> Result<bool, StorageError> {
        self.inner.row_batch_exists(table, branch, row_id, batch_id)
    }

    fn scan_row_branch_tip_ids(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Vec<crate::row_histories::BatchId>, StorageError> {
        self.inner.scan_row_branch_tip_ids(table, branch, row_id)
    }

    fn load_history_row_batch_bytes(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        self.inner
            .load_history_row_batch_bytes(table, branch, row_id, batch_id)
    }

    fn scan_history_region_bytes(
        &self,
        table: &str,
        scan: crate::row_histories::HistoryScan,
    ) -> Result<Vec<Vec<u8>>, StorageError> {
        self.inner.scan_history_region_bytes(table, scan)
    }

    fn scan_history_region(
        &self,
        table: &str,
        branch: &str,
        scan: crate::row_histories::HistoryScan,
    ) -> Result<Vec<crate::row_histories::StoredRowBatch>, StorageError> {
        self.inner.scan_history_region(table, branch, scan)
    }

    fn index_insert(
        &mut self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        self.inner
            .index_insert(table, column, branch, value, row_id)
    }

    fn index_remove(
        &mut self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        self.inner
            .index_remove(table, column, branch, value, row_id)
    }

    fn index_lookup(
        &self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
    ) -> Vec<ObjectId> {
        self.inner.index_lookup(table, column, branch, value)
    }

    fn index_range(
        &self,
        table: &str,
        column: &str,
        branch: &str,
        start: std::ops::Bound<&Value>,
        end: std::ops::Bound<&Value>,
    ) -> Vec<ObjectId> {
        self.inner.index_range(table, column, branch, start, end)
    }

    fn index_scan_all(&self, table: &str, column: &str, branch: &str) -> Vec<ObjectId> {
        self.inner.index_scan_all(table, column, branch)
    }

    fn flush(&self) -> Result<(), StorageError> {
        self.inner.flush()
    }

    fn flush_wal(&self) -> Result<(), StorageError> {
        self.inner.flush_wal()
    }

    fn close(&self) -> Result<(), StorageError> {
        self.inner.close()
    }
}

impl Storage for RowMutationObservingStorage {
    fn apply_encoded_row_mutation(
        &mut self,
        table: &str,
        history_rows: &[crate::storage::OwnedHistoryRowBytes],
        visible_rows: &[crate::storage::OwnedVisibleRowBytes],
        index_mutations: &[crate::storage::IndexMutation<'_>],
    ) -> Result<(), StorageError> {
        self.calls.lock().unwrap().row_mutation_calls += 1;
        self.inner
            .apply_encoded_row_mutation(table, history_rows, visible_rows, index_mutations)
    }

    fn apply_prepared_row_mutation(
        &mut self,
        table: &str,
        history_rows: &[crate::row_histories::StoredRowBatch],
        visible_entries: &[crate::row_histories::VisibleRowEntry],
        encoded_history_rows: &[crate::storage::OwnedHistoryRowBytes],
        encoded_visible_rows: &[crate::storage::OwnedVisibleRowBytes],
        index_mutations: &[crate::storage::IndexMutation<'_>],
    ) -> Result<(), StorageError> {
        self.calls.lock().unwrap().row_mutation_calls += 1;
        self.inner.apply_prepared_row_mutation(
            table,
            history_rows,
            visible_entries,
            encoded_history_rows,
            encoded_visible_rows,
            index_mutations,
        )
    }

    fn scan_row_locators(&self) -> Result<crate::storage::RowLocatorRows, StorageError> {
        self.inner.scan_row_locators()
    }

    fn load_row_locator(
        &self,
        id: ObjectId,
    ) -> Result<Option<crate::storage::RowLocator>, StorageError> {
        self.inner.load_row_locator(id)
    }

    fn put_row_locator(
        &mut self,
        id: ObjectId,
        locator: Option<&crate::storage::RowLocator>,
    ) -> Result<(), StorageError> {
        self.inner.put_row_locator(id, locator)
    }

    fn raw_table_put(&mut self, table: &str, key: &str, value: &[u8]) -> Result<(), StorageError> {
        self.inner.raw_table_put(table, key, value)
    }

    fn raw_table_delete(&mut self, table: &str, key: &str) -> Result<(), StorageError> {
        self.inner.raw_table_delete(table, key)
    }

    fn raw_table_get(&self, table: &str, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        if table == "__local_batch_record" && key.starts_with("batch:") {
            self.calls.lock().unwrap().local_batch_record_get_calls += 1;
        }
        self.inner.raw_table_get(table, key)
    }

    fn raw_table_scan_prefix(
        &self,
        table: &str,
        prefix: &str,
    ) -> Result<RawTableRows, StorageError> {
        self.inner.raw_table_scan_prefix(table, prefix)
    }

    fn raw_table_scan_prefix_keys(
        &self,
        table: &str,
        prefix: &str,
    ) -> Result<RawTableKeys, StorageError> {
        self.inner.raw_table_scan_prefix_keys(table, prefix)
    }

    fn raw_table_scan_range(
        &self,
        table: &str,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<RawTableRows, StorageError> {
        self.inner.raw_table_scan_range(table, start, end)
    }

    fn raw_table_scan_range_keys(
        &self,
        table: &str,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<RawTableKeys, StorageError> {
        self.inner.raw_table_scan_range_keys(table, start, end)
    }

    fn append_history_region_rows(
        &mut self,
        table: &str,
        rows: &[crate::row_histories::StoredRowBatch],
    ) -> Result<(), StorageError> {
        self.inner.append_history_region_rows(table, rows)
    }

    fn append_history_region_row_bytes(
        &mut self,
        table: &str,
        rows: &[crate::storage::HistoryRowBytes<'_>],
    ) -> Result<(), StorageError> {
        self.inner.append_history_region_row_bytes(table, rows)
    }

    fn upsert_visible_region_rows(
        &mut self,
        table: &str,
        entries: &[crate::row_histories::VisibleRowEntry],
    ) -> Result<(), StorageError> {
        self.inner.upsert_visible_region_rows(table, entries)
    }

    fn delete_visible_region_row(
        &mut self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        self.inner.delete_visible_region_row(table, branch, row_id)
    }

    fn apply_row_mutation(
        &mut self,
        table: &str,
        history_rows: &[crate::row_histories::StoredRowBatch],
        visible_entries: &[crate::row_histories::VisibleRowEntry],
        index_mutations: &[crate::storage::IndexMutation<'_>],
    ) -> Result<(), StorageError> {
        self.calls.lock().unwrap().row_mutation_calls += 1;
        self.inner
            .apply_row_mutation(table, history_rows, visible_entries, index_mutations)
    }

    fn patch_row_region_rows_by_batch(
        &mut self,
        table: &str,
        batch_id: crate::row_histories::BatchId,
        state: Option<crate::row_histories::RowState>,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Result<(), StorageError> {
        self.inner
            .patch_row_region_rows_by_batch(table, batch_id, state, confirmed_tier)
    }

    fn patch_exact_row_batch(
        &mut self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
        state: Option<crate::row_histories::RowState>,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Result<bool, StorageError> {
        self.inner
            .patch_exact_row_batch(table, branch, row_id, batch_id, state, confirmed_tier)
    }

    fn patch_exact_row_batch_for_schema_hash(
        &mut self,
        table: &str,
        schema_hash: crate::query_manager::types::SchemaHash,
        branch: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
        state: Option<crate::row_histories::RowState>,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Result<bool, StorageError> {
        self.inner.patch_exact_row_batch_for_schema_hash(
            table,
            schema_hash,
            branch,
            row_id,
            batch_id,
            state,
            confirmed_tier,
        )
    }

    fn scan_visible_region(
        &self,
        table: &str,
        branch: &str,
    ) -> Result<Vec<crate::row_histories::StoredRowBatch>, StorageError> {
        self.inner.scan_visible_region(table, branch)
    }

    fn load_visible_region_row(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<crate::row_histories::StoredRowBatch>, StorageError> {
        self.inner.load_visible_region_row(table, branch, row_id)
    }

    fn load_visible_region_frontier(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<Vec<crate::row_histories::BatchId>>, StorageError> {
        self.inner
            .load_visible_region_frontier(table, branch, row_id)
    }

    fn capture_family_visible_frontier(
        &self,
        target_branch_name: crate::object::BranchName,
    ) -> Result<Vec<crate::batch_fate::CapturedFrontierMember>, StorageError> {
        self.inner
            .capture_family_visible_frontier(target_branch_name)
    }

    fn scan_visible_region_row_batches(
        &self,
        table: &str,
        row_id: ObjectId,
    ) -> Result<Vec<crate::row_histories::StoredRowBatch>, StorageError> {
        self.inner.scan_visible_region_row_batches(table, row_id)
    }

    fn scan_history_row_batches(
        &self,
        table: &str,
        row_id: ObjectId,
    ) -> Result<Vec<crate::row_histories::StoredRowBatch>, StorageError> {
        self.inner.scan_history_row_batches(table, row_id)
    }

    fn load_history_row_batch(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
    ) -> Result<Option<crate::row_histories::StoredRowBatch>, StorageError> {
        self.inner
            .load_history_row_batch(table, branch, row_id, batch_id)
    }

    fn load_history_query_row_batch(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
    ) -> Result<Option<crate::row_histories::QueryRowBatch>, StorageError> {
        self.inner
            .load_history_query_row_batch(table, branch, row_id, batch_id)
    }

    fn load_history_row_batch_for_schema_hash(
        &self,
        table: &str,
        schema_hash: crate::query_manager::types::SchemaHash,
        branch: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
    ) -> Result<Option<crate::row_histories::StoredRowBatch>, StorageError> {
        self.inner.load_history_row_batch_for_schema_hash(
            table,
            schema_hash,
            branch,
            row_id,
            batch_id,
        )
    }

    fn load_history_row_batch_any_branch(
        &self,
        table: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
    ) -> Result<Option<crate::row_histories::StoredRowBatch>, StorageError> {
        self.inner
            .load_history_row_batch_any_branch(table, row_id, batch_id)
    }

    fn load_history_query_row_batch_any_branch(
        &self,
        table: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
    ) -> Result<Option<crate::row_histories::QueryRowBatch>, StorageError> {
        self.inner
            .load_history_query_row_batch_any_branch(table, row_id, batch_id)
    }

    fn row_batch_exists(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
    ) -> Result<bool, StorageError> {
        self.inner.row_batch_exists(table, branch, row_id, batch_id)
    }

    fn scan_row_branch_tip_ids(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Vec<crate::row_histories::BatchId>, StorageError> {
        self.inner.scan_row_branch_tip_ids(table, branch, row_id)
    }

    fn load_history_row_batch_bytes(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        self.inner
            .load_history_row_batch_bytes(table, branch, row_id, batch_id)
    }

    fn scan_history_region_bytes(
        &self,
        table: &str,
        scan: crate::row_histories::HistoryScan,
    ) -> Result<Vec<Vec<u8>>, StorageError> {
        self.inner.scan_history_region_bytes(table, scan)
    }

    fn scan_history_region(
        &self,
        table: &str,
        branch: &str,
        scan: crate::row_histories::HistoryScan,
    ) -> Result<Vec<crate::row_histories::StoredRowBatch>, StorageError> {
        self.inner.scan_history_region(table, branch, scan)
    }

    fn index_insert(
        &mut self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        self.inner
            .index_insert(table, column, branch, value, row_id)
    }

    fn index_remove(
        &mut self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        self.inner
            .index_remove(table, column, branch, value, row_id)
    }

    fn apply_index_mutations(
        &mut self,
        mutations: &[crate::storage::IndexMutation<'_>],
    ) -> Result<(), StorageError> {
        self.calls.lock().unwrap().separate_index_mutation_calls += 1;
        self.inner.apply_index_mutations(mutations)
    }

    fn index_lookup(
        &self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
    ) -> Vec<ObjectId> {
        self.inner.index_lookup(table, column, branch, value)
    }

    fn index_range(
        &self,
        table: &str,
        column: &str,
        branch: &str,
        start: std::ops::Bound<&Value>,
        end: std::ops::Bound<&Value>,
    ) -> Vec<ObjectId> {
        self.inner.index_range(table, column, branch, start, end)
    }

    fn index_scan_all(&self, table: &str, column: &str, branch: &str) -> Vec<ObjectId> {
        self.inner.index_scan_all(table, column, branch)
    }

    fn flush(&self) -> Result<(), StorageError> {
        self.inner.flush()
    }

    fn flush_wal(&self) -> Result<(), StorageError> {
        self.calls.lock().unwrap().flush_wal_calls += 1;
        self.inner.flush_wal()
    }

    fn close(&self) -> Result<(), StorageError> {
        self.inner.close()
    }
}

fn test_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text),
        )
        .build()
}

fn schema_evolution_v1() -> Schema {
    test_schema()
}

fn schema_evolution_v2() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .column("email", ColumnType::Text),
        )
        .build()
}

fn protected_documents_schema() -> Schema {
    let policies = TablePolicies::new()
        .with_select(PolicyExpr::eq_session("owner_id", vec!["user_id".into()]))
        .with_insert(PolicyExpr::eq_session("owner_id", vec!["user_id".into()]));

    SchemaBuilder::new()
        .table(
            TableSchema::builder("documents")
                .column("owner_id", ColumnType::Text)
                .column("title", ColumnType::Text)
                .policies(policies),
        )
        .build()
}

fn session_exists_rel_teams_schema() -> Schema {
    use crate::query_manager::relation_ir::{
        ColumnRef, JoinCondition, JoinKind, PredicateCmpOp, PredicateExpr, RelExpr, RowIdRef,
        ValueRef,
    };

    let team_select_policy = PolicyExpr::ExistsRel {
        rel: RelExpr::Filter {
            input: Box::new(RelExpr::Join {
                left: Box::new(RelExpr::TableScan {
                    table: TableName::new("user_team_edges"),
                }),
                right: Box::new(RelExpr::TableScan {
                    table: TableName::new("teams"),
                }),
                on: vec![JoinCondition {
                    left: ColumnRef::scoped("user_team_edges", "team_id"),
                    right: ColumnRef::scoped("__join_0", "id"),
                }],
                join_kind: JoinKind::Inner,
            }),
            predicate: PredicateExpr::And(vec![
                PredicateExpr::Cmp {
                    left: ColumnRef::scoped("user_team_edges", "user_id"),
                    op: PredicateCmpOp::Eq,
                    right: ValueRef::SessionRef(vec!["user_id".into()]),
                },
                PredicateExpr::Cmp {
                    left: ColumnRef::scoped("__join_0", "id"),
                    op: PredicateCmpOp::Eq,
                    right: ValueRef::RowId(RowIdRef::Outer),
                },
            ]),
        },
    };

    SchemaBuilder::new()
        .table(
            TableSchema::builder("teams")
                .column("name", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(team_select_policy)
                        .with_insert(PolicyExpr::True),
                ),
        )
        .table(
            TableSchema::builder("user_team_edges")
                .column("user_id", ColumnType::Text)
                .column("team_id", ColumnType::Uuid)
                .policies(TablePolicies::new().with_insert(PolicyExpr::True)),
        )
        .build()
}

fn structural_session_exists_rel_teams_schema() -> Schema {
    SchemaBuilder::new()
        .table(TableSchema::builder("teams").column("name", ColumnType::Text))
        .table(
            TableSchema::builder("user_team_edges")
                .column("user_id", ColumnType::Text)
                .column("team_id", ColumnType::Uuid),
        )
        .build()
}

fn users_insert_denied_authorization_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .policies(TablePolicies::new().with_insert(PolicyExpr::False)),
        )
        .build()
}

fn defaulted_todos_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("todos")
                .column("title", ColumnType::Text)
                .column_with_default("done", ColumnType::Boolean, Value::Boolean(false)),
        )
        .build()
}

fn user_row_values(id: ObjectId, name: &str) -> Vec<Value> {
    vec![Value::Uuid(id), Value::Text(name.to_string())]
}

fn user_insert_values(id: ObjectId, name: &str) -> HashMap<String, Value> {
    HashMap::from([
        ("id".to_string(), Value::Uuid(id)),
        ("name".to_string(), Value::Text(name.to_string())),
    ])
}

fn insert_and_wait_for_batch<S: Storage, Sch: Scheduler>(
    core: &mut RuntimeCore<S, Sch>,
    table: &str,
    values: HashMap<String, Value>,
    write_context: Option<&WriteContext>,
    tier: DurabilityTier,
) -> std::result::Result<
    (
        InsertedRow,
        futures::channel::oneshot::Receiver<PersistedWriteAck>,
    ),
    RuntimeError,
> {
    let (row, batch_id) = core.insert(table, values, write_context)?;
    let receiver = core.wait_for_batch(batch_id, tier)?;
    Ok((row, receiver))
}

fn delete_and_wait_for_batch<S: Storage, Sch: Scheduler>(
    core: &mut RuntimeCore<S, Sch>,
    object_id: ObjectId,
    write_context: Option<&WriteContext>,
    tier: DurabilityTier,
) -> std::result::Result<futures::channel::oneshot::Receiver<PersistedWriteAck>, RuntimeError> {
    let batch_id = core.delete(object_id, write_context)?;
    core.wait_for_batch(batch_id, tier)
}

fn staged_user_row(
    row_id: ObjectId,
    batch_id: BatchId,
    updated_at: u64,
    name: &str,
) -> crate::row_histories::StoredRowBatch {
    crate::row_histories::StoredRowBatch::new_with_batch_id(
        batch_id,
        row_id,
        "main",
        Vec::<BatchId>::new(),
        encode_row(
            &test_schema()[&TableName::new("users")].columns,
            &user_row_values(row_id, name),
        )
        .expect("user test row should encode"),
        crate::metadata::RowProvenance::for_insert(row_id.to_string(), updated_at),
        HashMap::new(),
        crate::row_histories::RowState::StagingPending,
        None,
    )
}

fn document_insert_values(owner_id: &str, title: &str) -> HashMap<String, Value> {
    HashMap::from([
        ("owner_id".to_string(), Value::Text(owner_id.to_string())),
        ("title".to_string(), Value::Text(title.to_string())),
    ])
}

fn project_insert_values(name: &str, owner_id: &str) -> HashMap<String, Value> {
    HashMap::from([
        ("name".to_string(), Value::Text(name.to_string())),
        ("owner_id".to_string(), Value::Text(owner_id.to_string())),
    ])
}

fn todo_insert_values(
    title: &str,
    done: bool,
    description: Value,
    owner_id: &str,
    project: Value,
) -> HashMap<String, Value> {
    HashMap::from([
        ("title".to_string(), Value::Text(title.to_string())),
        ("done".to_string(), Value::Boolean(done)),
        ("description".to_string(), description),
        ("owner_id".to_string(), Value::Text(owner_id.to_string())),
        ("project".to_string(), project),
    ])
}

fn create_runtime_with_schema_and_sync_manager(
    schema: Schema,
    app_name: &str,
    sync_manager: SyncManager,
) -> TestCore {
    let app_id = AppId::from_name(app_name);
    let schema_manager = SchemaManager::new(sync_manager, schema, app_id, "dev", "main").unwrap();
    let mut core = new_test_core(schema_manager, MemoryStorage::new(), NoopScheduler);
    core.immediate_tick();
    core
}

fn create_runtime_with_schema(schema: Schema, app_name: &str) -> TestCore {
    create_runtime_with_schema_and_sync_manager(schema, app_name, SyncManager::new())
}

fn create_runtime_with_storage(schema: Schema, app_name: &str, storage: MemoryStorage) -> TestCore {
    create_runtime_with_storage_and_sync_manager(schema, app_name, storage, SyncManager::new())
}

fn create_runtime_with_storage_and_sync_manager(
    schema: Schema,
    app_name: &str,
    storage: MemoryStorage,
    sync_manager: SyncManager,
) -> TestCore {
    let app_id = AppId::from_name(app_name);
    let schema_manager = SchemaManager::new(sync_manager, schema, app_id, "dev", "main").unwrap();
    let mut core = new_test_core(schema_manager, storage, NoopScheduler);
    core.immediate_tick();
    core
}

fn create_runtime_with_boxed_storage(
    schema: Schema,
    app_name: &str,
    storage: Box<dyn Storage>,
) -> BoxedStorageTestCore {
    let app_id = AppId::from_name(app_name);
    let schema_manager =
        SchemaManager::new(SyncManager::new(), schema, app_id, "dev", "main").unwrap();
    let mut core = new_test_core(schema_manager, storage, NoopScheduler);
    core.immediate_tick();
    core
}

fn create_test_runtime() -> TestCore {
    create_runtime_with_schema(test_schema(), "test-app")
}

fn documents_query_by_title(title: &str) -> Query {
    QueryBuilder::new("documents")
        .filter_eq("title", Value::Text(title.into()))
        .build()
}

fn column_index(schema: &Schema, table: &str, column: &str) -> usize {
    schema
        .get(&TableName::new(table))
        .unwrap_or_else(|| panic!("table '{table}' should exist"))
        .columns
        .column_index(column)
        .unwrap_or_else(|| panic!("column '{column}' should exist on table '{table}'"))
}

/// Helper to execute a query synchronously via subscribe/tick/unsubscribe.
fn execute_query(core: &mut TestCore, query: Query) -> Vec<(ObjectId, Vec<Value>)> {
    let sub_id = core
        .schema_manager_mut()
        .query_manager_mut()
        .subscribe(query)
        .unwrap();
    core.immediate_tick();
    let results = core
        .schema_manager_mut()
        .query_manager_mut()
        .get_subscription_results(sub_id);
    core.schema_manager_mut()
        .query_manager_mut()
        .unsubscribe_with_sync(sub_id);
    results
}

fn execute_runtime_query(
    core: &mut TestCore,
    query: Query,
    session: Option<Session>,
) -> Vec<(ObjectId, Vec<Value>)> {
    execute_runtime_query_with_propagation(
        core,
        query,
        session,
        crate::sync_manager::QueryPropagation::Full,
    )
}

fn execute_local_runtime_query(
    core: &mut TestCore,
    query: Query,
    session: Option<Session>,
) -> Vec<(ObjectId, Vec<Value>)> {
    execute_runtime_query_with_propagation(
        core,
        query,
        session,
        crate::sync_manager::QueryPropagation::LocalOnly,
    )
}

fn execute_runtime_query_with_propagation(
    core: &mut TestCore,
    query: Query,
    session: Option<Session>,
    propagation: crate::sync_manager::QueryPropagation,
) -> Vec<(ObjectId, Vec<Value>)> {
    execute_runtime_query_with_durability_and_propagation(
        core,
        query,
        session,
        ReadDurabilityOptions::default(),
        propagation,
    )
}

fn execute_runtime_query_with_durability_and_propagation(
    core: &mut TestCore,
    query: Query,
    session: Option<Session>,
    durability: ReadDurabilityOptions,
    propagation: crate::sync_manager::QueryPropagation,
) -> Vec<(ObjectId, Vec<Value>)> {
    let waker = noop_waker();
    let mut cx = std::task::Context::from_waker(&waker);

    let mut future = core.query_with_propagation(query, session, durability, propagation);

    match Pin::new(&mut future).poll(&mut cx) {
        Poll::Ready(Ok(results)) => results,
        Poll::Ready(Err(err)) => panic!("query should succeed: {err:?}"),
        Poll::Pending => panic!("query should resolve immediately"),
    }
}

fn execute_runtime_query_with_local_overlay(
    core: &mut TestCore,
    query: Query,
    session: Option<Session>,
    durability: ReadDurabilityOptions,
    propagation: crate::sync_manager::QueryPropagation,
    overlay: QueryLocalOverlay,
) -> Vec<(ObjectId, Vec<Value>)> {
    let waker = noop_waker();
    let mut cx = std::task::Context::from_waker(&waker);

    let mut future =
        core.query_with_local_overlay(query, session, durability, propagation, overlay);

    match Pin::new(&mut future).poll(&mut cx) {
        Poll::Ready(Ok(results)) => results,
        Poll::Ready(Err(err)) => panic!("query should succeed: {err:?}"),
        Poll::Pending => panic!("query should resolve immediately"),
    }
}

fn decode_added_rows(delta: &SubscriptionDelta) -> Vec<(ObjectId, Vec<Value>)> {
    delta
        .ordered_delta
        .added
        .iter()
        .map(|row| {
            let values = decode_row(&delta.descriptor, &row.row.data).unwrap_or_else(|err| {
                panic!(
                    "subscription row {:?} should decode successfully: {err:?}",
                    row.row.id
                )
            });
            (row.row.id, values)
        })
        .collect()
}

fn pump_client_messages_to_server(
    client: &mut TestCore,
    server: &mut TestCore,
    server_id: ServerId,
    client_id: ClientId,
) -> bool {
    let mut any_messages = false;

    client.batched_tick();
    for entry in client.sync_sender().take() {
        if entry.destination == Destination::Server(server_id) {
            any_messages = true;
            server.park_sync_message(InboxEntry {
                source: Source::Client(client_id),
                payload: entry.payload,
            });
        }
    }
    server.batched_tick();
    server.immediate_tick();

    any_messages
}

struct ClientForServer<'a> {
    core: &'a mut TestCore,
    server_id: ServerId,
    client_id: ClientId,
}

fn pump_server_messages_to_clients(
    server: &mut TestCore,
    clients: &mut [ClientForServer<'_>],
    server_outputs: &mut Vec<OutboxEntry>,
) -> bool {
    let mut any_messages = false;

    server.batched_tick();

    let server_out = server.sync_sender().take();
    server_outputs.extend(server_out.iter().cloned());
    for entry in server_out {
        let Destination::Client(destination_client_id) = entry.destination else {
            continue;
        };

        if let Some(client) = clients
            .iter_mut()
            .find(|client| client.client_id == destination_client_id)
        {
            any_messages = true;
            client.core.park_sync_message(InboxEntry {
                source: Source::Server(client.server_id),
                payload: entry.payload,
            });
        }
    }

    any_messages
}

fn sync_server_with_clients(
    server: &mut TestCore,
    clients: &mut [ClientForServer<'_>],
) -> Vec<OutboxEntry> {
    let mut server_outputs = Vec::new();

    for _ in 0..10 {
        let mut any_messages = false;

        for client in clients.iter_mut() {
            any_messages |= pump_client_messages_to_server(
                client.core,
                server,
                client.server_id,
                client.client_id,
            );
        }

        any_messages |= pump_server_messages_to_clients(server, clients, &mut server_outputs);

        for client in clients.iter_mut() {
            client.core.batched_tick();
            client.core.immediate_tick();
        }

        if !any_messages {
            break;
        }
    }

    server_outputs
}

fn outbox_has_object_update_for_client(
    entries: &[OutboxEntry],
    client_id: ClientId,
    object_id: ObjectId,
) -> bool {
    entries.iter().any(|entry| {
        matches!(
            &entry.destination,
            Destination::Client(dest_client_id) if *dest_client_id == client_id
        ) && match &entry.payload {
            SyncPayload::RowBatchNeeded { row, .. } | SyncPayload::RowBatchCreated { row, .. } => {
                row.row_id == object_id
            }
            _ => false,
        }
    })
}

/// Three-tier RuntimeCore setup for durability tests.
struct ThreeTierRC {
    a: TestCore,
    b: TestCore,
    c: TestCore,
    a_client_of_b: ClientId,
    b_server_for_a: ServerId,
    b_client_of_c: ClientId,
    c_server_for_b: ServerId,
}

fn create_3tier_rc() -> ThreeTierRC {
    let schema = test_schema();
    create_3tier_rc_with_schema(schema)
}

fn create_3tier_rc_with_schema(schema: Schema) -> ThreeTierRC {
    let app_id = AppId::from_name("durability-test");

    // A = client (no tier)
    let sm_a = SyncManager::new();
    let mgr_a = SchemaManager::new(sm_a, schema.clone(), app_id, "dev", "main").unwrap();
    let mut a = new_test_core(mgr_a, MemoryStorage::new(), NoopScheduler);

    // B = Worker server
    let sm_b = SyncManager::new().with_durability_tier(DurabilityTier::Local);
    let mgr_b = SchemaManager::new(sm_b, schema.clone(), app_id, "dev", "main").unwrap();
    let mut b = new_test_core(mgr_b, MemoryStorage::new(), NoopScheduler);

    // C = EdgeServer
    let sm_c = SyncManager::new().with_durability_tier(DurabilityTier::EdgeServer);
    let mgr_c = SchemaManager::new(sm_c, schema, app_id, "dev", "main").unwrap();
    let mut c = new_test_core(mgr_c, MemoryStorage::new(), NoopScheduler);

    let a_client_of_b = ClientId::new();
    let b_server_for_a = ServerId::new();
    let b_client_of_c = ClientId::new();
    let c_server_for_b = ServerId::new();

    // Topology: A ↔ B ↔ C
    {
        b.add_client(a_client_of_b, None);
        b.schema_manager_mut()
            .query_manager_mut()
            .sync_manager_mut()
            .set_client_role(a_client_of_b, ClientRole::Peer);
    }
    a.add_server(b_server_for_a);

    {
        c.add_client(b_client_of_c, None);
        c.schema_manager_mut()
            .query_manager_mut()
            .sync_manager_mut()
            .set_client_role(b_client_of_c, ClientRole::Peer);
    }
    b.add_server(c_server_for_b);

    // Initial tick + clear initial sync messages
    a.immediate_tick();
    b.immediate_tick();
    c.immediate_tick();
    a.batched_tick();
    b.batched_tick();
    c.batched_tick();
    a.sync_sender().take();
    b.sync_sender().take();
    c.sync_sender().take();

    ThreeTierRC {
        a,
        b,
        c,
        a_client_of_b,
        b_server_for_a,
        b_client_of_c,
        c_server_for_b,
    }
}

/// Pump all messages between 3 RuntimeCore nodes until quiescent.
fn pump_3tier(s: &mut ThreeTierRC) {
    for _ in 0..10 {
        let mut any_messages = false;

        // A outbox → B
        s.a.batched_tick();
        let a_out = s.a.sync_sender().take();
        for entry in a_out {
            if entry.destination == Destination::Server(s.b_server_for_a) {
                any_messages = true;
                s.b.park_sync_message(InboxEntry {
                    source: Source::Client(s.a_client_of_b),
                    payload: entry.payload,
                });
            }
        }

        // B process, then route outbox to A or C
        s.b.batched_tick();
        s.b.immediate_tick();
        s.b.batched_tick();
        let b_out = s.b.sync_sender().take();
        for entry in b_out {
            match &entry.destination {
                Destination::Client(cid) if *cid == s.a_client_of_b => {
                    any_messages = true;
                    s.a.park_sync_message(InboxEntry {
                        source: Source::Server(s.b_server_for_a),
                        payload: entry.payload,
                    });
                }
                Destination::Server(sid) if *sid == s.c_server_for_b => {
                    any_messages = true;
                    s.c.park_sync_message(InboxEntry {
                        source: Source::Client(s.b_client_of_c),
                        payload: entry.payload,
                    });
                }
                _ => {}
            }
        }

        // C process, then route outbox to B
        s.c.batched_tick();
        s.c.immediate_tick();
        s.c.batched_tick();
        let c_out = s.c.sync_sender().take();
        for entry in c_out {
            if entry.destination == Destination::Client(s.b_client_of_c) {
                any_messages = true;
                s.b.park_sync_message(InboxEntry {
                    source: Source::Server(s.c_server_for_b),
                    payload: entry.payload,
                });
            }
        }

        // A processes incoming
        s.a.batched_tick();
        s.a.immediate_tick();

        if !any_messages {
            break;
        }
    }
}

/// Pump only A → B (one hop, no C).
fn pump_a_to_b(s: &mut ThreeTierRC) {
    s.a.batched_tick();
    let a_out = s.a.sync_sender().take();
    for entry in a_out {
        if entry.destination == Destination::Server(s.b_server_for_a) {
            s.b.park_sync_message(InboxEntry {
                source: Source::Client(s.a_client_of_b),
                payload: entry.payload,
            });
        }
    }
    s.b.batched_tick();
    s.b.immediate_tick();
}

/// Route B's outbox to both A and C as appropriate.
fn route_b_outbox(s: &mut ThreeTierRC) {
    s.b.batched_tick();
    let b_out = s.b.sync_sender().take();
    for entry in b_out {
        match &entry.destination {
            Destination::Client(cid) if *cid == s.a_client_of_b => {
                s.a.park_sync_message(InboxEntry {
                    source: Source::Server(s.b_server_for_a),
                    payload: entry.payload,
                });
            }
            Destination::Server(sid) if *sid == s.c_server_for_b => {
                s.c.park_sync_message(InboxEntry {
                    source: Source::Client(s.b_client_of_c),
                    payload: entry.payload,
                });
            }
            _ => {}
        }
    }
}

/// Pump B → A (acks back).
fn pump_b_to_a(s: &mut ThreeTierRC) {
    route_b_outbox(s);
    s.a.batched_tick();
    s.a.immediate_tick();
}

/// Pump B → C (forward to edge).
fn pump_b_to_c(s: &mut ThreeTierRC) {
    route_b_outbox(s);
    s.c.batched_tick();
    s.c.immediate_tick();
}

/// Pump C → B → A (edge ack relay).
fn pump_c_to_b_to_a(s: &mut ThreeTierRC) {
    // C → B
    s.c.batched_tick();
    let c_out = s.c.sync_sender().take();
    for entry in c_out {
        if entry.destination == Destination::Client(s.b_client_of_c) {
            s.b.park_sync_message(InboxEntry {
                source: Source::Server(s.c_server_for_b),
                payload: entry.payload,
            });
        }
    }
    s.b.batched_tick();
    s.b.immediate_tick();

    // B → A
    pump_b_to_a(s);
}

fn count_query_subscriptions_to_server(entries: &[OutboxEntry], server_id: ServerId) -> usize {
    entries
        .iter()
        .filter(|entry| {
            matches!(
                &entry.destination,
                Destination::Server(dest_server_id) if *dest_server_id == server_id
            ) && matches!(&entry.payload, SyncPayload::QuerySubscription { .. })
        })
        .count()
}

fn noop_waker() -> std::task::Waker {
    fn noop(_: *const ()) {}
    fn clone(_: *const ()) -> std::task::RawWaker {
        std::task::RawWaker::new(std::ptr::null(), &VTABLE)
    }
    static VTABLE: std::task::RawWakerVTable =
        std::task::RawWakerVTable::new(clone, noop, noop, noop);
    unsafe { std::task::Waker::from_raw(std::task::RawWaker::new(std::ptr::null(), &VTABLE)) }
}

mod accepted_batch_downgrade;
mod authz_cache_runtime;
mod basic;
mod batched_tick_parked_drain;
mod delivery_confirmation;
mod fk_remove_error;
mod incremental_scan;
mod install_transport_tests;
mod query_subscription;
mod schema_catalogue;
mod sealed_batch_cost;
mod sync_replay;
mod write_batch;

/// A wiped upstream (fresh store behind the same endpoint) RELEARNS settled
/// rows on reconnect: the reconnect replay re-offers the client's durable
/// batches (`RowBatchCreated`), and the amnesiac server accepts them and
/// forwards them to its own upstream. This is the recovery property the
/// "full wipe" story leans on — if it ever regresses, a wipe permanently
/// strands every deterministic-id singleton (Saved Messages, assistant chat).
///
/// Also documented here: the ENGINE has no column-based duplicate refusal —
/// re-inserting the same column values mints a fresh row ObjectId. The
/// "row already exists" behavior the app sees for deterministic ids lives in
/// the TS binding (which maps the caller-provided id onto the engine row id
/// and checks the local store), not in the core.
#[test]
fn a_wiped_upstream_relearns_settled_rows_on_reconnect() {
    let mut s = create_3tier_rc();

    let fixed_id = ObjectId::new();
    let deterministic_row = || {
        HashMap::from([
            ("id".to_string(), Value::Uuid(fixed_id)),
            (
                "name".to_string(),
                Value::Text("Saved Messages".to_string()),
            ),
        ])
    };

    // The app always holds live queries; reconnect replays exactly these.
    let _sub =
        s.a.subscribe(Query::new("users"), |_delta| {}, None)
            .unwrap();

    let ((created_row_id, _values), _ack) = insert_and_wait_for_batch(
        &mut s.a,
        "users",
        deterministic_row(),
        None,
        DurabilityTier::Local,
    )
    .expect("the first create must succeed");

    // Positive control at the emission level: the client offers the batch upstream.
    s.a.batched_tick();
    let first_out = s.a.sync_sender().take();
    assert!(
        first_out.iter().any(|e| matches!(
            &e.payload,
            SyncPayload::RowBatchCreated { row, .. } if row.row_id == created_row_id
        )),
        "the fresh insert must be offered to the server"
    );
    for e in first_out {
        if e.destination == Destination::Server(s.b_server_for_a) {
            s.b.park_sync_message(InboxEntry {
                source: Source::Client(s.a_client_of_b),
                payload: e.payload,
            });
        }
    }
    pump_3tier(&mut s);

    // The wipe: same endpoint from the client's point of view, brand-new server
    // state behind it (the client keeps its own store and settled state).
    let app_id = AppId::from_name("durability-test");
    let sm_b2 = SyncManager::new().with_durability_tier(DurabilityTier::Local);
    let mgr_b2 = SchemaManager::new(sm_b2, test_schema(), app_id, "dev", "main").unwrap();
    let mut b2 = new_test_core(mgr_b2, MemoryStorage::new(), NoopScheduler);
    b2.add_client(s.a_client_of_b, None);
    b2.schema_manager_mut()
        .query_manager_mut()
        .sync_manager_mut()
        .set_client_role(s.a_client_of_b, ClientRole::Peer);
    b2.add_server(s.c_server_for_b);
    b2.immediate_tick();
    b2.batched_tick();
    b2.sync_sender().take();
    s.b = b2;

    // Reconnect semantics: drop and re-add the upstream — this is what replays
    // active query subscriptions (rc_replays_active_queries_on_upstream_reconnect).
    s.a.remove_server(s.b_server_for_a);
    s.a.add_server(s.b_server_for_a);

    // Pump manually so every payload the client sends after the reconnect is
    // recorded before being delivered.
    let mut reoffered_fixed_row = false;
    let mut reoffer_kinds: Vec<&'static str> = Vec::new();
    let mut b2_forwarded_to_c = false;
    for _ in 0..10 {
        let mut any_messages = false;

        s.a.batched_tick();
        for entry in s.a.sync_sender().take() {
            if entry.destination == Destination::Server(s.b_server_for_a) {
                any_messages = true;
                match &entry.payload {
                    SyncPayload::RowBatchCreated { row, .. } if row.row_id == created_row_id => {
                        reoffered_fixed_row = true;
                        reoffer_kinds.push("RowBatchCreated");
                    }
                    SyncPayload::RowBatchNeeded { row, .. } if row.row_id == created_row_id => {
                        reoffered_fixed_row = true;
                        reoffer_kinds.push("RowBatchNeeded");
                    }
                    _ => {}
                }
                s.b.park_sync_message(InboxEntry {
                    source: Source::Client(s.a_client_of_b),
                    payload: entry.payload,
                });
            }
        }

        s.b.batched_tick();
        s.b.immediate_tick();
        s.b.batched_tick();
        for entry in s.b.sync_sender().take() {
            match &entry.destination {
                Destination::Client(cid) if *cid == s.a_client_of_b => {
                    any_messages = true;
                    s.a.park_sync_message(InboxEntry {
                        source: Source::Server(s.b_server_for_a),
                        payload: entry.payload,
                    });
                }
                Destination::Server(sid) if *sid == s.c_server_for_b => {
                    any_messages = true;
                    if matches!(
                        &entry.payload,
                        SyncPayload::RowBatchCreated { row, .. } if row.row_id == created_row_id
                    ) {
                        b2_forwarded_to_c = true;
                    }
                    s.c.park_sync_message(InboxEntry {
                        source: Source::Client(s.b_client_of_c),
                        payload: entry.payload,
                    });
                }
                _ => {}
            }
        }

        s.c.batched_tick();
        s.c.immediate_tick();
        for entry in s.c.sync_sender().take() {
            if entry.destination == Destination::Client(s.b_client_of_c) {
                any_messages = true;
                s.b.park_sync_message(InboxEntry {
                    source: Source::Server(s.c_server_for_b),
                    payload: entry.payload,
                });
            }
        }

        s.a.batched_tick();
        s.a.immediate_tick();

        if !any_messages {
            break;
        }
    }

    let duplicate = s.a.insert("users", deterministic_row(), None);

    assert!(
        reoffered_fixed_row && reoffer_kinds.contains(&"RowBatchCreated"),
        "reconnect must re-offer the settled row to the wiped upstream \
         (got kinds {reoffer_kinds:?}) — without this a wipe strands every \
         deterministic-id singleton"
    );
    assert!(
        b2_forwarded_to_c,
        "the wiped upstream must accept the re-offered row and forward it to its own upstream"
    );
    let ((second_row_id, _), _) =
        duplicate.expect("engine-level insert has no column-id dedupe; the TS binding owns that");
    assert_ne!(
        second_row_id, created_row_id,
        "engine identity is the row ObjectId, not the id column — a re-insert mints a new row"
    );
}

/// RED GATE (prod incident 2026-08-09, UPSTREAM-DEFECTS #11): a seal that
/// arrives for a batch whose ROWS never made it (they rode a dying connection)
/// must not become an eternal full-store-scan loop on the server. In the field
/// this pinned a core: every pass scanned the whole history region (~3.5s on
/// the production store), made no progress, and repeated on the next tick,
/// starving sync entirely.
///
/// The contract this gate encodes for the fix:
///   1. the full-store fallback runs AT MOST ONCE per stuck batch while no new
///      writes land (negative cache), and
///   2. the sealer is answered with `BatchFate::Missing` — whose client-side
///      handler already retransmits the batch rows — so the two-phase loop
///      closes instead of spinning.
#[test]
fn a_seal_without_rows_must_not_loop_full_store_scans() {
    // Two nodes, prod-shaped: the server is the AUTHORITY tier (EdgeServer,
    // like production jazz-sync). Client fates recorded here become
    // authoritative — the ingredient the 3-tier middle node lacks.
    let app_id = AppId::from_name("durability-test");
    let schema = test_schema();
    let sm_a = SyncManager::new();
    let mgr_a = SchemaManager::new(sm_a, schema.clone(), app_id, "dev", "main").unwrap();
    let mut a = new_test_core(mgr_a, MemoryStorage::new(), NoopScheduler);
    let sm_e = SyncManager::new().with_durability_tier(DurabilityTier::EdgeServer);
    let mgr_e = SchemaManager::new(sm_e, schema, app_id, "dev", "main").unwrap();
    let mut e = new_test_core(mgr_e, MemoryStorage::new(), NoopScheduler);
    let a_client_of_e = ClientId::new();
    let e_server_for_a = ServerId::new();
    e.add_client(a_client_of_e, None);
    e.schema_manager_mut()
        .query_manager_mut()
        .sync_manager_mut()
        .set_client_role(a_client_of_e, ClientRole::Peer);
    a.add_server(e_server_for_a);
    a.immediate_tick();
    e.immediate_tick();
    a.batched_tick();
    e.batched_tick();
    a.sync_sender().take();
    e.sync_sender().take();
    struct Pair {
        a: TestCore,
        b: TestCore,
        a_client_of_b: ClientId,
        b_server_for_a: ServerId,
    }
    let mut s = Pair {
        a,
        b: e,
        a_client_of_b: a_client_of_e,
        b_server_for_a: e_server_for_a,
    };

    let ((row_id, _values), _ack) = insert_and_wait_for_batch(
        &mut s.a,
        "users",
        HashMap::from([
            ("id".to_string(), Value::Uuid(ObjectId::new())),
            ("name".to_string(), Value::Text("lost rows".to_string())),
        ]),
        None,
        DurabilityTier::EdgeServer,
    )
    .expect("client-side insert");

    // Capture the client's upload and deliver everything EXCEPT the row
    // payload itself: the rows died with the first connection, the seal
    // arrived on the second.
    s.a.batched_tick();
    let mut dropped_row_payloads = 0usize;
    let mut retried_payloads = Vec::new();
    for entry in s.a.sync_sender().take() {
        if entry.destination != Destination::Server(s.b_server_for_a) {
            continue;
        }
        match &entry.payload {
            SyncPayload::RowBatchCreated { row, .. } if row.row_id == row_id => {
                dropped_row_payloads += 1;
            }
            _ => retried_payloads.push(entry.payload),
        }
    }
    assert!(
        dropped_row_payloads > 0,
        "the scenario requires actually dropping the row payload"
    );
    assert!(
        !retried_payloads.is_empty(),
        "the seal/durability payloads must survive to be retried"
    );
    // Deliver the seal once and check what the server persisted — production
    // stores an orphan sealed submission for every such batch (609 of them in
    // the incident store). If this harness delivery does not persist one,
    // seed it exactly as production holds it: from the client's own payload.
    for payload in retried_payloads.clone() {
        s.b.park_sync_message(InboxEntry {
            source: Source::Client(s.a_client_of_b),
            payload,
        });
    }
    s.b.batched_tick();
    s.b.immediate_tick();
    {
        use crate::storage::Storage as _;
        let batch_id = retried_payloads
            .iter()
            .find_map(|p| match p {
                SyncPayload::SealBatch { submission } => Some(submission.batch_id),
                _ => None,
            })
            .expect("a SealBatch payload");
        let persisted =
            s.b.storage()
                .load_sealed_batch_submission(batch_id)
                .unwrap();
        eprintln!(
            "server persisted orphan submission after seal: {}",
            persisted.is_some()
        );
        if persisted.is_none() {
            if let Some(SyncPayload::SealBatch { submission }) = retried_payloads
                .iter()
                .find(|p| matches!(p, SyncPayload::SealBatch { .. }))
            {
                s.b.storage_mut()
                    .upsert_sealed_batch_submission(submission)
                    .expect("seed the production-shaped orphan submission");
                eprintln!("seeded orphan submission (production shape)");
            }
        }
    }

    // Let the server process the orphan seal across several ticks, re-parking
    // whatever the client keeps retrying (the field client retried its seal on
    // a live connection for 38+ minutes).
    let scans_before =
        crate::runtime_core::LOCAL_BATCH_FULL_SCANS.load(std::sync::atomic::Ordering::Relaxed);
    let _orphan_batch_id = retried_payloads
        .iter()
        .find_map(|p| match p {
            SyncPayload::SealBatch { submission } => Some(submission.batch_id),
            _ => None,
        })
        .expect("a SealBatch payload");
    let mut missing_answered = false;
    for round in 0..5 {
        // The field driver, named by the production stack dump: a DIVERGED
        // client keeps uploading rows whose parents this store never had
        // (ParentNotFound). Each one the server rejects walks
        // apply_received_batch_fate -> mark_local_batch_rows_rejected ->
        // local_batch_rows -> full-store scan.
        let diverged_parent = crate::row_histories::BatchId(*ObjectId::new().uuid().as_bytes());
        let diverged_batch_id = crate::row_histories::BatchId(*ObjectId::new().uuid().as_bytes());
        let diverged_row_id = ObjectId::new();
        let diverged_row = crate::row_histories::StoredRowBatch::new_with_batch_id(
            diverged_batch_id,
            diverged_row_id,
            "main",
            vec![diverged_parent],
            encode_row(
                &test_schema()[&TableName::new("users")].columns,
                &user_row_values(diverged_row_id, &format!("diverged-{round}")),
            )
            .expect("diverged row encodes"),
            crate::metadata::RowProvenance::for_insert(diverged_row_id.to_string(), 1),
            HashMap::new(),
            crate::row_histories::RowState::StagingPending,
            None,
        );
        s.b.park_sync_message(InboxEntry {
            source: Source::Client(s.a_client_of_b),
            payload: SyncPayload::RowBatchCreated {
                metadata: None,
                row: diverged_row,
            },
        });
        // Plus the seal retries the same client kept sending for 38 minutes.
        for payload in retried_payloads.clone() {
            s.b.park_sync_message(InboxEntry {
                source: Source::Client(s.a_client_of_b),
                payload,
            });
        }
        // Production is never quiet: unrelated writes land continuously
        // (presence heartbeats, tokens). Feed one per round so any
        // "re-scan on new input" behavior surfaces.
        let (_, _bg_ack) = insert_and_wait_for_batch(
            &mut s.a,
            "users",
            HashMap::from([
                ("id".to_string(), Value::Uuid(ObjectId::new())),
                (
                    "name".to_string(),
                    Value::Text(format!("background-{round}")),
                ),
            ]),
            None,
            DurabilityTier::EdgeServer,
        )
        .expect("background write");
        s.a.batched_tick();
        for entry in s.a.sync_sender().take() {
            if entry.destination == Destination::Server(s.b_server_for_a) {
                s.b.park_sync_message(InboxEntry {
                    source: Source::Client(s.a_client_of_b),
                    payload: entry.payload,
                });
            }
        }
        s.b.batched_tick();
        s.b.immediate_tick();
        for entry in s.b.sync_sender().take() {
            if let Destination::Client(cid) = &entry.destination {
                if *cid == s.a_client_of_b
                    && matches!(
                        &entry.payload,
                        SyncPayload::BatchFate {
                            fate: crate::batch_fate::BatchFate::Missing { .. },
                            ..
                        }
                    )
                {
                    missing_answered = true;
                }
            }
        }
    }
    let scans = crate::runtime_core::LOCAL_BATCH_FULL_SCANS
        .load(std::sync::atomic::Ordering::Relaxed)
        - scans_before;

    eprintln!("gate observation: scans={scans} missing_answered={missing_answered}");
    // The cost contract, driver-agnostic: ANY path deriving the pending set
    // over the persisted orphan pays local_batch_rows; with the orphan seeded,
    // repeated derivations must answer from the first scan's result.
    let scans_p0 = s.b.local_batch_full_scan_count();
    let pending_first = s.b.pending_batch_ids_needing_reconciliation_for_test();
    let pending_second = s.b.pending_batch_ids_needing_reconciliation_for_test();
    let derivation_scans = crate::runtime_core::LOCAL_BATCH_FULL_SCANS
        .load(std::sync::atomic::Ordering::Relaxed)
        - scans_p0;
    eprintln!(
        "pending derivations: first={} second={} scans={derivation_scans}",
        pending_first.len(),
        pending_second.len()
    );

    assert!(
        scans <= 1,
        "a stuck seal must cost at most one full-store scan while nothing changes; \
         got {scans} scans across 5 ticks — the production CPU-pin loop"
    );
    assert!(
        derivation_scans <= 1,
        "two pending-set derivations over one persisted orphan cost {derivation_scans} \
         full-store scans — repeated derivations must reuse the first scan's answer"
    );
    assert!(
        missing_answered,
        "the sealer must be told the batch is Missing so it retransmits the rows"
    );
}

/// RED GATE #2 (prod incident 2026-08-09, the "conveyor" half): settled
/// history must not cost full-store scans when the pending set is derived.
///
/// Production holds tens of thousands of settled batches (heartbeats, every
/// row ever written) whose batchId->rows index was cleared at settlement while
/// their fates/records persist. Deriving the pending-reconciliation set walks
/// those and, for every one whose fate still reads as unsettled, pays the
/// full-store fallback: one ~0.45s scan per batch, hours of pinned CPU after
/// every server restart, with the tick loop blocked the whole time.
#[test]
fn settled_history_must_not_cost_full_store_scans_on_reconciliation() {
    let app_id = AppId::from_name("durability-test");
    let schema = test_schema();
    let sm_a = SyncManager::new();
    let mgr_a = SchemaManager::new(sm_a, schema.clone(), app_id, "dev", "main").unwrap();
    let mut a = new_test_core(mgr_a, MemoryStorage::new(), NoopScheduler);
    let sm_e = SyncManager::new().with_durability_tier(DurabilityTier::EdgeServer);
    let mgr_e = SchemaManager::new(sm_e, schema, app_id, "dev", "main").unwrap();
    let mut e = new_test_core(mgr_e, MemoryStorage::new(), NoopScheduler);
    let a_client_of_e = ClientId::new();
    let e_server_for_a = ServerId::new();
    e.add_client(a_client_of_e, None);
    e.schema_manager_mut()
        .query_manager_mut()
        .sync_manager_mut()
        .set_client_role(a_client_of_e, ClientRole::Backend);
    a.add_server(e_server_for_a);
    a.immediate_tick();
    e.immediate_tick();
    a.batched_tick();
    e.batched_tick();
    a.sync_sender().take();
    e.sync_sender().take();

    // The "heartbeat backlog": a run of ordinary writes, fully settled.
    for i in 0..8 {
        let (_, _ack) = insert_and_wait_for_batch(
            &mut a,
            "users",
            HashMap::from([
                ("id".to_string(), Value::Uuid(ObjectId::new())),
                ("name".to_string(), Value::Text(format!("heartbeat-{i}"))),
            ]),
            None,
            DurabilityTier::EdgeServer,
        )
        .expect("insert");
        // Full roundtrip: client -> server, server acks -> client.
        for _ in 0..4 {
            a.batched_tick();
            for entry in a.sync_sender().take() {
                if entry.destination == Destination::Server(e_server_for_a) {
                    e.park_sync_message(InboxEntry {
                        source: Source::Client(a_client_of_e),
                        payload: entry.payload,
                    });
                }
            }
            e.batched_tick();
            e.immediate_tick();
            for entry in e.sync_sender().take() {
                if let Destination::Client(cid) = &entry.destination {
                    if *cid == a_client_of_e {
                        a.park_sync_message(InboxEntry {
                            source: Source::Server(e_server_for_a),
                            payload: entry.payload,
                        });
                    }
                }
            }
            a.batched_tick();
            a.immediate_tick();
        }
    }

    // The server-side leftovers a restart sweep will walk.
    {
        use crate::storage::Storage as _;
        let fates = e.storage().scan_authoritative_batch_fates().unwrap();
        let submissions = e.storage().scan_sealed_batch_submissions().unwrap();
        eprintln!(
            "server leftovers: fates={} submissions={}",
            fates.len(),
            submissions.len()
        );
    }

    let scans_before =
        crate::runtime_core::LOCAL_BATCH_FULL_SCANS.load(std::sync::atomic::Ordering::Relaxed);
    let pending = e.pending_batch_ids_needing_reconciliation_for_test();
    let scans = crate::runtime_core::LOCAL_BATCH_FULL_SCANS
        .load(std::sync::atomic::Ordering::Relaxed)
        - scans_before;
    eprintln!(
        "pending={} full_scans={} (settled history must answer from records, not scans)",
        pending.len(),
        scans
    );
    assert_eq!(
        scans, 0,
        "deriving the pending set over settled history cost {scans} full-store scans — \
         the production restart conveyor"
    );
}

/// RED GATE (prod 2026-08-10, the v16.1 follow-up): a POLICY-REJECTED write
/// must not cost a full-store history scan.
///
/// Named by the production store's own fates, not by inference: 934
/// `Insert denied by policy on table users` and 114 `Update denied by USING
/// policy on table users - no old content`. A diverged client — one whose
/// chains predate this store — writes presence heartbeats onto a `users` row
/// whose old content is absent here, so policy denies every one. Each denial
/// records a Rejected fate, and `apply_received_batch_fate` then calls
/// `mark_local_batch_rows_rejected`, which walks `local_batch_rows`: all four
/// point-lookup member sources miss (the rejected row never landed), and the
/// "last-resort" full-store history scan runs. Marking rows rejected is
/// best-effort bookkeeping over rows we already track; with no bookkeeping
/// there is nothing to mark, and rediscovering that by scanning every table's
/// history is unbounded work bought with peer input.
///
/// Measured in production: 264 distinct such batches in 26 minutes, the core
/// pinned at 100%. v16.1's negative cache capped REPEATS only (680 warns over
/// those 264 ids) and could not help the first scan of each new id.
#[test]
fn a_policy_rejected_write_costs_no_full_store_scan() {
    let schema = protected_documents_schema();
    let mut client = create_runtime_with_schema(schema.clone(), "policy-reject-scan-test");
    let mut server = create_runtime_with_schema(schema, "policy-reject-scan-test");

    let client_id = ClientId::new();
    let server_id = ServerId::new();
    // The server knows this connection as mallory; the client writes alice's
    // rows. Locally the write satisfies alice's own policy, and on the server
    // it is denied — the shape the field client hits, where its writes pass at
    // home and are refused here.
    server.add_client(client_id, Some(Session::new("mallory")));
    client.add_server(server_id);
    let alice_session = Session::new("alice");

    // Ordinary history on the server, so a full scan has something to walk.
    for index in 0..12 {
        server
            .insert(
                "documents",
                document_insert_values("resident", &format!("doc-{index}")),
                None,
            )
            .expect("seed history");
    }
    client.batched_tick();
    server.batched_tick();
    server.immediate_tick();
    client.sync_sender().take();
    server.sync_sender().take();

    // Per-runtime scans, measured as a DELTA between two identical halves: the
    // absolute count is not the contract (an unrelated caller may legitimately
    // scan once), but denials themselves must buy none — before the fix this
    // delta was one scan per denial.
    let mut scans_before = server.local_batch_full_scan_count();
    let mut first_half_scans = 0u64;
    let mut rejected_fates = 0usize;

    // Twelve denied writes in two identical halves — the cadence of a client
    // that keeps coming back.
    for round in 0..12 {
        if round == 6 {
            first_half_scans = server.local_batch_full_scan_count() - scans_before;
            scans_before = server.local_batch_full_scan_count();
        }
        client
            .insert(
                "documents",
                document_insert_values("alice", &format!("denied-{round}")),
                Some(&WriteContext::from_session(alice_session.clone())),
            )
            .expect("the write satisfies the client's own policy");
        pump_client_messages_to_server(&mut client, &mut server, server_id, client_id);
        server.batched_tick();
        server.immediate_tick();
        server.batched_tick();
        for entry in server.sync_sender().take() {
            if let SyncPayload::BatchFate { fate } = &entry.payload
                && matches!(fate, crate::batch_fate::BatchFate::Rejected { .. })
            {
                rejected_fates += 1;
            }
        }
    }

    let second_half_scans = server.local_batch_full_scan_count() - scans_before;
    eprintln!(
        "policy-denied writes: rejected_fates={rejected_fates} \
         scans_first_half={first_half_scans} scans_second_half={second_half_scans}"
    );
    assert!(
        rejected_fates >= 12,
        "the scenario must produce a rejected fate per write, else it gates nothing"
    );
    assert_eq!(
        second_half_scans, 0,
        "six more policy-denied writes cost {second_half_scans} full-store history scans — \
         a denial must not buy O(store) work"
    );
}

/// A parentless write must not read the row's whole history.
///
/// `pre_batch_visible_row` prepares the pre-batch content every incoming write
/// is policy-checked against. A batch with ONE parent takes a point-lookup
/// fast path there. A PARENTLESS batch does not: it falls through to
/// `scan_history_row_batches`, which reads every version the row has.
///
/// That is the shape a diverged client sends — the server classifies a write
/// with no parents and no visible old content as an insert — and production
/// 2026-08-10 collected 1442 `Insert denied by policy on table users` from it.
/// The row those landed on had grown to 2541 versions on presence heartbeats,
/// so each attempt read all 2541, with the runtime mutex held; a stack dump
/// caught the server there repeatedly.
///
/// A parentless batch has no ancestry to resolve. The read only decides
/// whether every visible version sits on the incoming branch, and when it does
/// — the single-branch case, which is what production is — the answer is
/// always `None`, so the whole read was spent proving that.
#[test]
fn a_parentless_write_does_not_read_the_whole_row_history() {
    let schema = test_schema();
    let mut client = create_runtime_with_schema(schema.clone(), "parentless-history-cost");
    let mut server = create_runtime_with_schema(schema, "parentless-history-cost");

    let client_id = ClientId::new();
    let server_id = ServerId::new();
    server.add_client(client_id, Some(Session::new("writer")));
    client.add_server(server_id);

    // One row, many versions — a presence row's shape.
    let row_id = ObjectId::new();
    let ((server_row_id, _), _) = insert_and_wait_for_batch(
        &mut server,
        "users",
        HashMap::from([
            ("id".to_string(), Value::Uuid(row_id)),
            ("name".to_string(), Value::Text("v0".to_string())),
        ]),
        None,
        DurabilityTier::Local,
    )
    .expect("seed the row");
    for version in 1..40 {
        server
            .update(
                server_row_id,
                vec![("name".to_string(), Value::Text(format!("v{version}")))],
                None,
            )
            .expect("grow the history");
    }
    server.batched_tick();
    server.immediate_tick();
    client.batched_tick();
    client.sync_sender().take();
    server.sync_sender().take();

    // The diverged shape: a write for that row carrying no parents, on the
    // branch rows actually live on (env + scope + user branch, composed).
    let live_branch = crate::storage::sole_branch_name(server.storage())
        .expect("branch registry readable")
        .expect("the seeded rows registered a branch");
    let parentless = crate::row_histories::StoredRowBatch::new(
        server_row_id,
        live_branch.as_str(),
        Vec::<crate::row_histories::BatchId>::new(),
        encode_row(
            &test_schema()[&TableName::new("users")].columns,
            &user_row_values(server_row_id, "from a diverged client"),
        )
        .expect("row encodes"),
        crate::metadata::RowProvenance::for_insert(server_row_id.to_string(), 9_999),
        HashMap::new(),
        crate::row_histories::RowState::VisibleDirect,
        None,
    );

    server.storage().reset_history_scans();
    server.park_sync_message(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::RowBatchCreated {
            metadata: None,
            row: parentless,
        },
    });
    server.batched_tick();
    server.immediate_tick();
    let history_reads = server.storage().history_scans();

    eprintln!("whole-history reads for one parentless write: {history_reads}");
    assert_eq!(
        history_reads, 0,
        "one parentless write cost {history_reads} reads of the row's entire history — a \
         batch with no ancestry has nothing to resolve, and a client can send these as fast \
         as it likes"
    );
}

/// An exact replay must be recognised before the work it does not need.
///
/// A peer that retransmits rows it already sent — what every reconnect and
/// every `Missing` answer produces — sends batches the authority holds byte
/// for byte. `process_from_client` recognises that
/// (`matches_replayed_row_batch`) and short-circuits, but the preparation for
/// the policy check that will never run used to happen FIRST: another history
/// row read and decode, or a walk of the whole history for a parentless row.
/// The decision needs only the row the first read already fetched.
///
/// Production 2026-08-10: a core pinned with an empty log, the runtime
/// absorbing a peer's replays — silently, because the short-circuit logs
/// nothing.
#[test]
fn an_exact_replay_costs_one_history_read() {
    let schema = test_schema();
    let mut server = create_runtime_with_schema(schema, "replay-cost");
    let client_id = ClientId::new();
    server.add_client(client_id, Some(Session::new("writer")));

    // A row with history, so a replay has a parent that could be looked up.
    let row_id = ObjectId::new();
    let ((server_row_id, _), _) = insert_and_wait_for_batch(
        &mut server,
        "users",
        HashMap::from([
            ("id".to_string(), Value::Uuid(row_id)),
            ("name".to_string(), Value::Text("v0".to_string())),
        ]),
        None,
        DurabilityTier::Local,
    )
    .expect("seed the row");
    server.batched_tick();
    server.immediate_tick();
    server.sync_sender().take();

    let live_branch = crate::storage::sole_branch_name(server.storage())
        .expect("branch registry readable")
        .expect("the seeded row registered a branch");
    let history = server
        .storage()
        .scan_history_row_batches("users", server_row_id)
        .expect("history readable");
    let root = history.first().expect("the row has a version").clone();

    // What a peer sends: a child of what it holds.
    let child = crate::row_histories::StoredRowBatch::new(
        server_row_id,
        live_branch.as_str(),
        vec![root.batch_id],
        encode_row(
            &test_schema()[&TableName::new("users")].columns,
            &user_row_values(server_row_id, "from the peer"),
        )
        .expect("row encodes"),
        crate::metadata::RowProvenance::for_insert(server_row_id.to_string(), 5_000),
        HashMap::new(),
        crate::row_histories::RowState::VisibleDirect,
        None,
    );
    let deliver = |server: &mut TestCore, row: crate::row_histories::StoredRowBatch| {
        server.park_sync_message(InboxEntry {
            source: Source::Client(client_id),
            payload: SyncPayload::RowBatchCreated {
                metadata: None,
                row,
            },
        });
        server.batched_tick();
        server.immediate_tick();
    };
    deliver(&mut server, child.clone());

    let stored = server
        .storage()
        .load_history_row_batch("users", live_branch.as_str(), server_row_id, child.batch_id)
        .expect("history lookup");
    assert!(
        stored.is_some(),
        "the first send must land, else the second is not a replay and this gates nothing"
    );

    // The same batch again, byte for byte.
    server.storage().reset_history_row_lookups();
    server.storage().reset_history_scans();
    deliver(&mut server, child);
    let lookups = server.storage().history_row_lookups();
    let scans = server.storage().history_scans();

    eprintln!("exact replay cost: history_row_lookups={lookups} history_scans={scans}");
    assert_eq!(
        scans, 0,
        "an exact replay walked the row's whole history {scans} time(s)"
    );
    assert!(
        lookups <= 1,
        "an exact replay cost {lookups} history row reads; one fetches the row the \
         short-circuit is decided from, and everything past that decision prepares a \
         policy check that never runs"
    );
}
