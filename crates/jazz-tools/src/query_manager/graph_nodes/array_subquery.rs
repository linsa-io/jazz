//! ArraySubqueryNode for correlated subqueries that produce array columns.
//!
//! This node implements the "dynamic graph instances" approach where each
//! unique outer row gets its own subgraph evaluation. This is intentionally
//! chosen over shared hash indices to explore subgraph patterns and collect
//! learnings for future optimizations.

use ahash::{AHashMap, AHashSet};

use crate::object::ObjectId;
use crate::query_manager::encoding::{decode_row, encode_row};
use crate::query_manager::precise_dirty::precise_dirty_enabled;
use crate::query_manager::query::ArraySubqueryRequirement;
use std::sync::Arc;

use crate::query_manager::types::{
    ColumnDescriptor, ColumnType, LoadedRow, RowDescriptor, Schema, TableName, Tuple,
    TupleBatchProvenance, TupleDelta, TupleDescriptor, TupleElement, TupleProvenance, Value,
};

use crate::storage::Storage;

use super::RowNode;
use super::subgraph::{SubgraphInstance, SubgraphTemplate};

/// Node that evaluates a correlated subquery for each outer row,
/// producing an array column with the results.
///
/// ## Architecture
///
/// ```text
/// OuterScan → Materialize → ArraySubqueryNode
///                               ↓
///                     For each outer tuple:
///                       - Bind correlation values
///                       - Evaluate subgraph
///                       - Collect results into array
///                               ↓
///                     outer tuple + array column
/// ```
///
/// ## Learnings to collect (for future sub-graph sharing optimization):
/// - Which parts of SubgraphInstances could be shared (index state? settled results?)
/// - Memory overhead per instance
/// - Update cost distribution (how many instances need re-settling on inner change?)
/// - Common subgraph patterns that could benefit from memoization
///
/// Hard ceiling on live subgraph instances kept per node.
///
/// The cache is normally bounded by the number of live outer rows, because entries are
/// dropped with the row they belong to. This is the safety valve for a result set large
/// enough that holding a compiled graph per row would cost more memory than the compiles
/// it saves. Above it we evict the least recently used entry, which degrades toward the
/// old recompile-per-row behaviour for the overflow rather than growing without bound.
const MAX_CACHED_SUBGRAPHS: usize = 2048;

/// Ceiling on ids held in [`PendingInnerDirt`] before it is flushed eagerly.
///
/// Sized like `MAX_PENDING_CHANGED_ROWS` in `index_scan.rs`, and for the same
/// reason: past it the bookkeeping costs more than the work it defers. It is
/// also what stops a node whose instances are never re-evaluated from pinning
/// changed-id sets without bound — see [`Self::flush_pending_inner_dirt`] for
/// the overflow policy.
const MAX_PENDING_INNER_ROWS: usize = 4096;

/// Inner-table dirt recorded at WRITE time and threaded into a cached subgraph
/// instance when that instance is next evaluated (v13-3).
///
/// WHY: v13-2 threaded every mark into every cached instance eagerly, inside
/// `mark_table_dependents_dirty` and the `forward_rows_*` siblings. That made
/// one write cost O(live outer rows) on the write path — for every table the
/// subscription touches, not just the include's own, because the content and
/// removal channels were table-blind. Measured on the live server it doubled
/// idle CPU and turned a ten-second presence heartbeat into a burst that
/// scaled with the subscribed result set (`tests/include_instance_flatness.rs`
/// pins both axes). Buffering here makes the write path O(1) in the instance
/// count; the marks are applied per instance in `evaluate_subgraph_for_single`,
/// which the settle already visits, so no instance sees a different bitmap
/// than it would have under eager marking — only later.
///
/// The buffer is a SNAPSHOT, not a log: an instance behind `generation` gets
/// the whole current payload applied. That is a superset of the marks it
/// missed, and every mark is a "re-check this" instruction, so over-applying
/// costs work, never correctness. `generation` plus the per-instance
/// `applied_generation` is what makes application exactly-once: an instance
/// compiled after a push starts all-dirty AND up to date, so it neither misses
/// the change nor re-applies it.
#[derive(Debug, Default)]
struct PendingInnerDirt {
    /// Bumped on every payload change. An instance is up to date exactly when
    /// its `applied_generation` equals this.
    generation: u64,
    /// Cached instances still behind `generation`. The payload is non-empty
    /// only while this is non-zero — reaching zero drops it.
    stale_instances: usize,
    /// Row-precise membership marks, per inner table.
    rows: AHashMap<TableName, AHashSet<ObjectId>>,
    /// Inner tables marked with no row information: a full rescan on apply.
    /// Dominates `rows` for the same table, exactly as `IndexScanNode`'s
    /// `needs_full` dominates its pending set.
    full_tables: AHashSet<TableName>,
    /// Content re-load marks, per inner table.
    updated: AHashMap<TableName, AHashSet<ObjectId>>,
    /// Removal marks, per inner table.
    deleted: AHashMap<TableName, AHashSet<ObjectId>>,
}

/// Which table-keyed content buffer a graph-level row mark lands in.
#[derive(Debug, Clone, Copy)]
enum ContentMark {
    Updated,
    Deleted,
}

impl PendingInnerDirt {
    /// Ids currently buffered, across every channel and inner table.
    fn buffered_ids(&self) -> usize {
        fn total(per_table: &AHashMap<TableName, AHashSet<ObjectId>>) -> usize {
            per_table.values().map(|ids| ids.len()).sum()
        }
        total(&self.rows) + total(&self.updated) + total(&self.deleted)
    }

    /// Thread the whole payload into one instance's graph, in the order the
    /// eager path delivered it: table-level marks (which force a full rescan)
    /// before row-precise ones, membership before content before removals —
    /// the order `apply_batched_subscription_visibility_effects` marks in.
    fn apply_to(&self, graph: &mut crate::query_manager::graph::QueryGraph) {
        for table in &self.full_tables {
            graph.mark_dirty_for_table(table.as_str());
        }
        for (table, ids) in &self.rows {
            graph.mark_rows_changed_for_table(table.as_str(), ids);
        }
        for (table, ids) in &self.updated {
            graph.mark_rows_updated(table.as_str(), ids);
        }
        for (table, ids) in &self.deleted {
            graph.mark_rows_deleted(table.as_str(), ids);
        }
    }

    fn clear_payload(&mut self) {
        self.rows.clear();
        self.full_tables.clear();
        self.updated.clear();
        self.deleted.clear();
        self.stale_instances = 0;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Correlate {
    /// Correlate using a column value from the outer row.
    Col(usize),
    /// Correlate using the outer tuple's object id.
    Id,
}

#[derive(Debug)]
pub struct ArraySubqueryNode {
    /// Descriptor for outer tuples.
    outer_descriptor: TupleDescriptor,
    /// Output descriptor (outer columns + array column).
    output_descriptor: RowDescriptor,
    /// Output tuple descriptor.
    output_tuple_descriptor: TupleDescriptor,

    /// Template for the inner subgraph.
    subgraph_template: SubgraphTemplate,
    /// Schema for compiling subgraphs.
    schema: Arc<Schema>,

    /// Source of the correlation value from the outer tuple.
    outer_correlation: Correlate,
    /// Requirement for whether the correlated result must exist.
    requirement: ArraySubqueryRequirement,

    /// Per-outer-row state: outer_id → latest outer tuple + evaluated array.
    instances: AHashMap<ObjectId, ArrayInstanceState>,

    /// Current output tuples.
    current_tuples: AHashSet<Tuple>,

    dirty: bool,
    /// True if the inner table changed (need to reevaluate all instances).
    inner_dirty: bool,

    /// Live subgraph instances, keyed by (outer row, element index).
    ///
    /// WHY: `instantiate` compiles a FRESH QueryGraph per outer row, and
    /// `reevaluate_all` re-evaluates every instance whenever the inner table
    /// changes. Measured on a real store, one incoming row update produced ~147
    /// full `try_compile_with_schema_context` calls, i.e. ~177/s on an otherwise
    /// idle server. The compiled shape is identical across re-evaluations — only
    /// the correlation binding differs — so keeping the settled instance and
    /// re-settling it removes the compile entirely. Results are read from
    /// `current_output_tuples()` (full state, not a delta), so reuse is sound.
    subgraph_cache: AHashMap<(ObjectId, usize), CachedSubgraph>,
    /// Monotonic tick used to order cache entries by last use.
    subgraph_cache_clock: u64,
    /// Inner-table dirt buffered at write time, applied per instance on use.
    ///
    /// Boxed: the buffer's four collections are ~200 bytes of mostly-empty
    /// headers, and `ArraySubqueryNode` is the largest `GraphNode` variant, so
    /// inline they would widen every node slot in every compiled graph
    /// (`clippy::large_enum_variant`). One pointer here, one allocation per
    /// compiled node.
    pending_inner_dirt: Box<PendingInnerDirt>,
}

#[derive(Debug)]
struct CachedSubgraph {
    correlation_value: Value,
    instance: SubgraphInstance,
    last_used: u64,
    /// [`PendingInnerDirt::generation`] this instance's graph has already been
    /// marked with. Behind it means the buffer still owes this instance.
    applied_generation: u64,
}

#[derive(Debug, Clone)]
struct ArrayInstanceState {
    outer_tuple: Tuple,
    correlation_value: Value,
    array_result: Value,
    provenance: TupleProvenance,
    batch_provenance: TupleBatchProvenance,
}

impl ArraySubqueryNode {
    /// Create a new ArraySubqueryNode.
    ///
    /// # Arguments
    /// * `outer_descriptor` - Descriptor for incoming outer tuples
    /// * `subgraph_template` - Template for creating inner subgraph instances
    /// * `outer_correlation` - Source for correlation value from outer tuple.
    /// * `array_column_name` - Name for the output array column
    /// * `schema` - Schema for compiling subgraphs
    pub fn new(
        outer_descriptor: TupleDescriptor,
        subgraph_template: SubgraphTemplate,
        outer_correlation: Correlate,
        requirement: ArraySubqueryRequirement,
        array_column_name: String,
        schema: Arc<Schema>,
    ) -> Self {
        // Build output descriptor: outer columns + array column
        let outer_row_descriptor = outer_descriptor.combined_descriptor();
        let mut output_columns = outer_row_descriptor.columns.to_vec();

        // Array column type: Array<Row> with the subgraph's output columns.
        // The row id is carried in Value::Row { id: Some(...), .. } rather than
        // prepended as a column.
        let row_columns = subgraph_template.output_descriptor().columns.clone();
        let element_type = ColumnType::Array {
            element: Box::new(ColumnType::Row {
                columns: Box::new(RowDescriptor::new(row_columns)),
            }),
        };

        output_columns.push(ColumnDescriptor {
            name: array_column_name.clone().into(),
            column_type: element_type,
            nullable: false,
            references: None,
            default: None,
            merge_strategy: None,
        });

        let output_descriptor = RowDescriptor::new(output_columns);
        let output_tuple_descriptor =
            TupleDescriptor::single_with_materialization("", output_descriptor.clone(), true);

        Self {
            outer_descriptor,
            output_descriptor,
            output_tuple_descriptor,
            subgraph_template,
            schema,
            outer_correlation,
            requirement,
            instances: AHashMap::new(),
            current_tuples: AHashSet::new(),
            dirty: true,
            inner_dirty: false,
            subgraph_cache: AHashMap::new(),
            subgraph_cache_clock: 0,
            pending_inner_dirt: Box::default(),
        }
    }

    /// Drop every cached subgraph belonging to an outer row.
    ///
    /// Called when the row leaves the result set, so the cache tracks live rows rather
    /// than every row ever seen. Without this the cache is what grows without bound —
    /// the capacity ceiling is only a backstop.
    fn forget_cached_subgraphs(&mut self, outer_id: ObjectId) {
        let generation = self.pending_inner_dirt.generation;
        let mut dropped_stale = 0usize;
        self.subgraph_cache.retain(|(id, _), cached| {
            let keep = *id != outer_id;
            if !keep && cached.applied_generation != generation {
                dropped_stale += 1;
            }
            keep
        });
        self.release_stale_instances(dropped_stale);
    }

    /// Make room for one new entry, evicting the least recently used one if needed.
    fn evict_subgraphs_over_capacity(&mut self, incoming: &(ObjectId, usize)) {
        if self.subgraph_cache.len() < MAX_CACHED_SUBGRAPHS
            || self.subgraph_cache.contains_key(incoming)
        {
            return;
        }
        // Linear scan is fine: it only runs at the ceiling, over a bounded map.
        if let Some(victim) = self
            .subgraph_cache
            .iter()
            .min_by_key(|(_, cached)| cached.last_used)
            .map(|(key, _)| *key)
        {
            self.drop_cached_subgraph(&victim);
        }
    }

    /// Drop one cache entry, keeping the pending-dirt debt count in step.
    fn drop_cached_subgraph(&mut self, key: &(ObjectId, usize)) {
        if let Some(cached) = self.subgraph_cache.remove(key)
            && cached.applied_generation != self.pending_inner_dirt.generation
        {
            self.release_stale_instances(1);
        }
    }

    /// Record that `count` instances no longer owe the buffer an application.
    /// The payload is dropped the moment nobody is behind it any more — that is
    /// what keeps a settled node from carrying changed-id sets between ticks.
    fn release_stale_instances(&mut self, count: usize) {
        let pending = &mut self.pending_inner_dirt;
        pending.stale_instances = pending.stale_instances.saturating_sub(count);
        if pending.stale_instances == 0 {
            pending.clear_payload();
        }
    }

    /// Open a new buffer generation: every cached instance now owes it an
    /// application. Called after any payload change.
    fn bump_pending_generation(&mut self) {
        self.pending_inner_dirt.generation += 1;
        self.pending_inner_dirt.stale_instances = self.subgraph_cache.len();
        if self.pending_inner_dirt.stale_instances == 0 {
            // Nothing to owe it to — a node with no live instances buffers
            // nothing, so an idle include costs nothing to keep marked.
            self.pending_inner_dirt.clear_payload();
        } else if self.pending_inner_dirt.buffered_ids() > MAX_PENDING_INNER_ROWS {
            self.flush_pending_inner_dirt();
        }
    }

    /// Bounded degradation past [`MAX_PENDING_INNER_ROWS`]: thread the payload
    /// into every cached instance NOW — the v13-2 eager walk — and reset the
    /// buffer.
    ///
    /// Deliberately not "mark everything on next use": `mark_all_dirty` is the
    /// legacy coarse mark, and the legacy mark is exactly what fails to re-load
    /// content for rows an instance already holds (the staleness family in
    /// `manager_tests/subscription_output_oracle.rs`). Flushing eagerly keeps
    /// the marks row-precise, so the overflow path costs one O(instances) walk
    /// per `MAX_PENDING_INNER_ROWS` buffered ids — amortised O(instances/4096)
    /// per id — and never trades correctness for the bound.
    fn flush_pending_inner_dirt(&mut self) {
        let generation = self.pending_inner_dirt.generation;
        for cached in self.subgraph_cache.values_mut() {
            self.pending_inner_dirt.apply_to(&mut cached.instance.graph);
            cached.applied_generation = generation;
        }
        self.pending_inner_dirt.clear_payload();
    }

    /// Table-level sibling of [`Self::note_inner_rows_changed`] for dirt with
    /// no row information (F2): with precise dirtiness enabled the table mark
    /// is buffered for every cached subgraph instance, whose scans on that
    /// table then take one full rescan (restoring an exact incremental
    /// baseline) while every other node stays clean. Without this, a
    /// table-level channel (e.g. the manager's row-coverage fallback in
    /// `apply_batched_subscription_visibility_effects`) would set
    /// `inner_dirty` but leave instance graphs clean, and the precise
    /// re-evaluation skip would serve stale arrays.
    pub fn note_inner_table_dirty(&mut self, table: &str) {
        self.inner_dirty = true;
        if !precise_dirty_enabled() {
            return;
        }
        let table = TableName::new(table);
        // The full-rescan mark dominates everything row-precise for this table,
        // so drop what it subsumes rather than applying both.
        self.pending_inner_dirt.rows.remove(&table);
        self.pending_inner_dirt.full_tables.insert(table);
        self.bump_pending_generation();
    }

    /// Row-precise sibling of [`Self::note_inner_table_dirty`] (F2): `ids`
    /// changed in `table` (one of this include's inner tables — the direct
    /// one or a nested one, both registered against this node at compile
    /// time).
    ///
    /// With precise dirtiness enabled the ids are BUFFERED (see
    /// [`PendingInnerDirt`]) and threaded into a cached subgraph instance when
    /// that instance is next evaluated: the instance's index scans then learn
    /// exactly which rows changed (point-membership re-checks instead of full
    /// rescans), and the instance's own `mark_table_dependents_dirty` routes
    /// nested-table ids onward into nested ArraySubqueryNodes — the recursion
    /// that fixes nested-include staleness (FINDING manifestation 2 in
    /// `manager_tests/subscription_output_oracle.rs`). Content re-loads for
    /// held rows arrive separately via [`Self::forward_rows_updated`].
    ///
    /// With the kill switch off this degrades to the legacy coarse mark; the
    /// reused instances are then `mark_all_dirty`-ed at evaluation time (see
    /// `evaluate_subgraph_for_single`).
    pub fn note_inner_rows_changed(&mut self, table: &str, ids: &AHashSet<ObjectId>) {
        self.inner_dirty = true;
        if !precise_dirty_enabled() || ids.is_empty() {
            return;
        }
        let table = TableName::new(table);
        if self.pending_inner_dirt.full_tables.contains(&table) {
            // Already scheduled for a full rescan; row detail adds nothing.
            return;
        }
        self.pending_inner_dirt
            .rows
            .entry(table)
            .or_default()
            .extend(ids.iter().copied());
        self.bump_pending_generation();
    }

    /// Buffer a content-update mark so include-inner materializers re-load rows
    /// they already hold (FINDING manifestation 1) when their instance is next
    /// evaluated.
    ///
    /// `table` is one of this node's registered inner tables — the caller
    /// (`QueryGraph::mark_rows_updated`) only forwards to nodes that read it.
    /// That scoping is the other half of the v13-2 fix: the channel used to be
    /// table-blind, so a write to ANY table in the subscription walked every
    /// cached instance of every include, none of which could hold the row.
    ///
    /// Returns whether the node needs re-evaluation. Marks for a table this
    /// node reads always qualify: the membership channel for the same table
    /// runs first in the same tick (see
    /// `apply_batched_subscription_visibility_effects`, whose changed-row set
    /// is the union of the updated and deleted ids) and has already dirtied
    /// this node, so answering "yes" here adds no re-settles — it only removes
    /// the dependency on that ordering. Legacy mode buffers nothing,
    /// preserving the old behavior byte for byte.
    pub fn forward_rows_updated(&mut self, table: &str, ids: &AHashSet<ObjectId>) -> bool {
        self.buffer_content_marks(table, ids, ContentMark::Updated)
    }

    /// Removal-delta counterpart of [`Self::forward_rows_updated`].
    pub fn forward_rows_deleted(&mut self, table: &str, ids: &AHashSet<ObjectId>) -> bool {
        self.buffer_content_marks(table, ids, ContentMark::Deleted)
    }

    fn buffer_content_marks(
        &mut self,
        table: &str,
        ids: &AHashSet<ObjectId>,
        mark: ContentMark,
    ) -> bool {
        if !precise_dirty_enabled() || ids.is_empty() {
            return false;
        }
        let table = TableName::new(table);
        let sink = match mark {
            ContentMark::Updated => &mut self.pending_inner_dirt.updated,
            ContentMark::Deleted => &mut self.pending_inner_dirt.deleted,
        };
        sink.entry(table).or_default().extend(ids.iter().copied());
        self.inner_dirty = true;
        self.bump_pending_generation();
        true
    }

    /// How many compiled subgraphs this node is holding.
    ///
    /// Exposed so the memory cost of the cache can be attributed: RSS alone cannot say
    /// how much of it is ours, but RSS delta divided by this count can.
    pub fn cached_subgraph_count(&self) -> usize {
        self.subgraph_cache.len()
    }

    /// Check if the inner table changed (need to reevaluate all instances).
    pub fn is_inner_dirty(&self) -> bool {
        self.inner_dirty
    }

    /// Process outer deltas with access to Storage and object manager for subgraph settling.
    pub fn process_with_context<F>(
        &mut self,
        input: TupleDelta,
        io: &dyn Storage,
        mut row_loader: F,
    ) -> TupleDelta
    where
        F: FnMut(ObjectId, Option<TableName>) -> Option<LoadedRow>,
    {
        let mut result = TupleDelta::new();

        // Process removed tuples
        for tuple in input.removed {
            if let Some(outer_id) = tuple.first_id() {
                let state = self.instances.remove(&outer_id);
                self.forget_cached_subgraphs(outer_id);
                let old_array = state
                    .as_ref()
                    .map(|state| state.array_result.clone())
                    .unwrap_or_else(|| Value::Array(vec![]));
                let old_provenance = state
                    .as_ref()
                    .map(|state| state.provenance.clone())
                    .unwrap_or_default();
                let old_batch_provenance = state
                    .as_ref()
                    .map(|state| state.batch_provenance.clone())
                    .unwrap_or_default();
                let old_correlation = state
                    .as_ref()
                    .map(|state| state.correlation_value.clone())
                    .unwrap_or(Value::Null);
                if let Some(old_output) = self.build_output_tuple(
                    &tuple,
                    &old_correlation,
                    &old_array,
                    &old_provenance,
                    &old_batch_provenance,
                ) {
                    self.current_tuples.remove(&old_output);
                    result.removed.push(old_output);
                }
            }
        }

        // Process added tuples
        for tuple in input.added {
            if let Some(outer_id) = tuple.first_id() {
                // Get correlation value from outer tuple
                if let Some(correlation_value) = self.extract_correlation_value(&tuple) {
                    // Evaluate subgraph for this correlation value
                    let (array_result, provenance, batch_provenance) =
                        self.evaluate_subgraph(outer_id, &correlation_value, io, &mut row_loader);

                    // Store instance state
                    self.instances.insert(
                        outer_id,
                        ArrayInstanceState {
                            outer_tuple: tuple.clone(),
                            correlation_value: correlation_value.clone(),
                            array_result: array_result.clone(),
                            provenance: provenance.clone(),
                            batch_provenance: batch_provenance.clone(),
                        },
                    );

                    // Build output tuple with array column
                    if let Some(output_tuple) = self.build_output_tuple(
                        &tuple,
                        &correlation_value,
                        &array_result,
                        &provenance,
                        &batch_provenance,
                    ) {
                        self.current_tuples.insert(output_tuple.clone());
                        result.added.push(output_tuple);
                    }
                }
            }
        }

        // Process updated tuples
        for (old_tuple, new_tuple) in input.updated {
            let old_outer_id = old_tuple.first_id();
            let new_outer_id = new_tuple.first_id();
            let old_state = old_outer_id.and_then(|outer_id| self.instances.remove(&outer_id));
            if let Some(outer_id) = old_outer_id
                && Some(outer_id) != new_outer_id
            {
                self.forget_cached_subgraphs(outer_id);
            }

            let old_array = old_state
                .as_ref()
                .map(|state| state.array_result.clone())
                .unwrap_or_else(|| Value::Array(vec![]));
            let old_provenance = old_state
                .as_ref()
                .map(|state| state.provenance.clone())
                .unwrap_or_default();
            let old_batch_provenance = old_state
                .as_ref()
                .map(|state| state.batch_provenance.clone())
                .unwrap_or_default();

            let old_correlation = old_state
                .as_ref()
                .map(|state| state.correlation_value.clone())
                .or_else(|| self.extract_correlation_value(&old_tuple));
            let new_correlation = self.extract_correlation_value(&new_tuple);

            // Reusing the stored array is only sound when no inner change is
            // pending for this instance. When one is, evaluate HERE so this
            // settle emits a single coalesced pair for the row: emitting the
            // stale array now and letting `reevaluate_all` emit a correction
            // would put two chained update pairs for one row id into one
            // TupleDelta — and tuple identity is ID-based, so downstream
            // nodes (SortNode keeps a Vec ordered by ID-equality) double-book
            // the row and later serve a stale pre-image (FINDING
            // manifestation 3 in `manager_tests/subscription_output_oracle.rs`).
            let reused_array_is_fresh = if precise_dirty_enabled() {
                match (new_outer_id, new_correlation.as_ref()) {
                    (Some(outer_id), Some(correlation)) => {
                        self.subgraph_state_clean(outer_id, correlation)
                    }
                    _ => true,
                }
            } else {
                !self.inner_dirty
            };
            let (new_array, new_provenance, new_batch_provenance) =
                if old_correlation == new_correlation && reused_array_is_fresh {
                    (
                        old_state
                            .as_ref()
                            .map(|state| state.array_result.clone())
                            .unwrap_or_else(|| Value::Array(vec![])),
                        old_state
                            .as_ref()
                            .map(|state| state.provenance.clone())
                            .unwrap_or_default(),
                        old_state
                            .as_ref()
                            .map(|state| state.batch_provenance.clone())
                            .unwrap_or_default(),
                    )
                } else if let Some(ref new_corr) = new_correlation {
                    match new_outer_id.or(old_outer_id) {
                        Some(outer_id) => {
                            self.evaluate_subgraph(outer_id, new_corr, io, &mut row_loader)
                        }
                        None => (
                            Value::Array(vec![]),
                            TupleProvenance::default(),
                            TupleBatchProvenance::default(),
                        ),
                    }
                } else {
                    (
                        Value::Array(vec![]),
                        TupleProvenance::default(),
                        TupleBatchProvenance::default(),
                    )
                };

            if let (Some(outer_id), Some(correlation_value)) =
                (new_outer_id, new_correlation.clone())
            {
                self.instances.insert(
                    outer_id,
                    ArrayInstanceState {
                        outer_tuple: new_tuple.clone(),
                        correlation_value,
                        array_result: new_array.clone(),
                        provenance: new_provenance.clone(),
                        batch_provenance: new_batch_provenance.clone(),
                    },
                );
            }

            let old_output = old_correlation.as_ref().and_then(|correlation| {
                self.build_output_tuple(
                    &old_tuple,
                    correlation,
                    &old_array,
                    &old_provenance,
                    &old_batch_provenance,
                )
            });
            let new_output = new_correlation.as_ref().and_then(|correlation| {
                self.build_output_tuple(
                    &new_tuple,
                    correlation,
                    &new_array,
                    &new_provenance,
                    &new_batch_provenance,
                )
            });

            match (old_output, new_output) {
                (Some(old_output), Some(new_output)) => {
                    self.current_tuples.remove(&old_output);
                    self.current_tuples.insert(new_output.clone());
                    result.updated.push((old_output, new_output));
                }
                (Some(old_output), None) => {
                    self.current_tuples.remove(&old_output);
                    result.removed.push(old_output);
                }
                (None, Some(new_output)) => {
                    self.current_tuples.insert(new_output.clone());
                    result.added.push(new_output);
                }
                (None, None) => {}
            }
        }

        self.dirty = false;
        result
    }

    /// Extract correlation value from an outer tuple.
    fn extract_correlation_value(&self, tuple: &Tuple) -> Option<Value> {
        match self.outer_correlation {
            Correlate::Id => tuple.first_id().map(Value::Uuid),
            Correlate::Col(col_idx) => {
                let element = tuple.get(0)?;
                let content = element.content()?;
                let outer_row_desc = self.outer_descriptor.combined_descriptor();
                let values = decode_row(&outer_row_desc, content).ok()?;
                values.get(col_idx).cloned()
            }
        }
    }

    /// Evaluate the subgraph for a given correlation value.
    /// Uses trait object to avoid recursion limit with nested generics.
    fn evaluate_subgraph(
        &mut self,
        outer_id: ObjectId,
        correlation_value: &Value,
        io: &dyn Storage,
        row_loader: &mut dyn FnMut(ObjectId, Option<TableName>) -> Option<LoadedRow>,
    ) -> (Value, TupleProvenance, TupleBatchProvenance) {
        // UUID[] FK forward includes correlate an array of ids to scalar inner ids.
        // Evaluate each element independently so output preserves source order/duplicates.
        if let Value::Array(elements) = correlation_value {
            let mut materialized = Vec::new();
            let mut provenance = TupleProvenance::default();
            let mut batch_provenance = TupleBatchProvenance::default();
            for (idx, element) in elements.iter().enumerate() {
                let (nested_value, nested_provenance, nested_batch_provenance) =
                    self.evaluate_subgraph_for_single((outer_id, idx), element, io, row_loader);
                let Value::Array(mut nested) = nested_value else {
                    continue;
                };
                materialized.append(&mut nested);
                for scoped_object in nested_provenance {
                    provenance.insert(scoped_object);
                }
                for batch_id in nested_batch_provenance {
                    batch_provenance.insert(batch_id);
                }
            }
            return (Value::Array(materialized), provenance, batch_provenance);
        }

        self.evaluate_subgraph_for_single((outer_id, 0), correlation_value, io, row_loader)
    }

    fn evaluate_subgraph_for_single(
        &mut self,
        cache_key: (ObjectId, usize),
        correlation_value: &Value,
        io: &dyn Storage,
        row_loader: &mut dyn FnMut(ObjectId, Option<TableName>) -> Option<LoadedRow>,
    ) -> (Value, TupleProvenance, TupleBatchProvenance) {
        // Settle-cost accounting: this is the per-instance unit of include
        // work. `SUBQUERY_INSTANTIATIONS` (counted inside
        // `SubgraphTemplate::instantiate`) is the subset that also compiled.
        crate::query_manager::settle_cost::bump(
            &crate::query_manager::settle_cost::SUBQUERY_INSTANCE_EVALS,
        );
        let output_desc = self.subgraph_template.output_descriptor().clone();

        // Reuse the settled instance when the correlation binding is unchanged; only
        // a new binding needs a compile.
        let reusable = matches!(
            self.subgraph_cache.get(&cache_key),
            Some(cached) if &cached.correlation_value == correlation_value
        );
        if !reusable {
            match self
                .subgraph_template
                .instantiate(correlation_value.clone(), &self.schema)
            {
                Some(fresh) => {
                    self.evict_subgraphs_over_capacity(&cache_key);
                    self.subgraph_cache_clock += 1;
                    // A freshly compiled graph starts with every node dirty, so
                    // it already covers everything the buffer holds: it is born
                    // UP TO DATE with the current generation. That is what stops
                    // an instance created after a buffered change from either
                    // missing it or applying it a second time.
                    let replaced = self.subgraph_cache.insert(
                        cache_key,
                        CachedSubgraph {
                            correlation_value: correlation_value.clone(),
                            instance: fresh,
                            last_used: self.subgraph_cache_clock,
                            applied_generation: self.pending_inner_dirt.generation,
                        },
                    );
                    if let Some(replaced) = replaced
                        && replaced.applied_generation != self.pending_inner_dirt.generation
                    {
                        self.release_stale_instances(1);
                    }
                }
                None => {
                    self.drop_cached_subgraph(&cache_key);
                    return (
                        Value::Array(vec![]),
                        TupleProvenance::default(),
                        TupleBatchProvenance::default(),
                    );
                }
            }
        }
        let reused = reusable;
        self.subgraph_cache_clock += 1;
        let clock = self.subgraph_cache_clock;
        let Some(cached) = self.subgraph_cache.get_mut(&cache_key) else {
            return (
                Value::Array(vec![]),
                TupleProvenance::default(),
                TupleBatchProvenance::default(),
            );
        };
        cached.last_used = clock;

        // Apply whatever inner-table dirt was buffered since this instance was
        // last evaluated. This is the deferred half of the write-path marking:
        // the instance ends up with the bitmap the eager walk would have given
        // it (a superset when several ticks coalesced), just paid for here,
        // where the settle was going to visit it anyway.
        let generation = self.pending_inner_dirt.generation;
        let consumed_pending = cached.applied_generation != generation;
        if consumed_pending {
            self.pending_inner_dirt.apply_to(&mut cached.instance.graph);
            cached.applied_generation = generation;
        }
        let instance = &mut cached.instance;

        if reused && !precise_dirty_enabled() {
            // LEGACY PATH (JAZZ_PRECISE_DIRTY=0). A freshly compiled graph
            // starts with every node dirty. A reused one does not, and its
            // scan nodes have no idea the inner table moved — that is what
            // made three array_subquery tests return stale arrays. Marking all
            // dirty restores exactly the fresh-graph semantics; the saving is
            // the compile, not the scan.
            //
            // PRECISE PATH (default): reused instance graphs carry their own
            // row-precise dirt, delivered at write time through
            // `note_inner_rows_changed` (membership) and `forward_rows_*`
            // (content / removals), so the settle below re-evaluates exactly
            // the touched rows — O(|changed ids|) point reads per instance —
            // and a clean instance settles in zero evaluations. The one dirt
            // source that bypasses these channels is a schema republish, and
            // that never reuses instances: `recompile_stale_subscriptions`
            // (query_manager/manager.rs) replaces the whole subscription
            // graph, dropping this node together with its instance caches, so
            // a stale-shape plan cannot survive a schema change (verified
            // pre-condition, include-plan-sharing design §9).
            instance.graph.mark_all_dirty();
        }
        let _row_delta = instance
            .graph
            .settle(io, &mut |id, hint| row_loader(id, hint));
        let mut provenance = TupleProvenance::default();
        let mut batch_provenance = TupleBatchProvenance::default();
        let array_elements: Vec<Value> = instance
            .graph
            .current_output_tuples()
            .into_iter()
            .filter_map(|tuple| {
                let row = if tuple.len() == 1 {
                    tuple.to_single_row()
                } else {
                    tuple
                        .flatten_with_descriptors(
                            &instance.graph.table_descriptors,
                            &instance.graph.combined_descriptor,
                        )
                        .and_then(|flattened| flattened.to_single_row())
                }?;
                let values = decode_row(&output_desc, &row.data).ok()?;
                for scoped_object in tuple.provenance().iter().copied() {
                    provenance.insert(scoped_object);
                }
                for batch_id in tuple.batch_provenance().iter().copied() {
                    batch_provenance.insert(batch_id);
                }
                Some(Value::Row {
                    id: Some(row.id),
                    values,
                })
            })
            .collect();
        if consumed_pending {
            self.release_stale_instances(1);
        }
        (Value::Array(array_elements), provenance, batch_provenance)
    }

    /// Build output tuple from outer tuple + array result.
    fn build_output_tuple(
        &self,
        outer_tuple: &Tuple,
        correlation_value: &Value,
        array_result: &Value,
        inner_provenance: &TupleProvenance,
        inner_batch_provenance: &TupleBatchProvenance,
    ) -> Option<Tuple> {
        if !self.requirement_satisfied(correlation_value, array_result) {
            return None;
        }

        let element = outer_tuple.get(0)?;
        let outer_id = element.id();
        let outer_content = element.content()?;
        let batch_id = element.batch_id()?;
        let row_provenance = element.row_provenance()?.clone();

        // Decode outer values
        let outer_row_desc = self.outer_descriptor.combined_descriptor();
        let mut values = decode_row(&outer_row_desc, outer_content).ok()?;

        // Append array column
        values.push(array_result.clone());

        // Encode output
        let output_content = encode_row(&self.output_descriptor, &values).ok()?;

        let mut provenance = outer_tuple.provenance().clone();
        for scoped_object in inner_provenance.iter().copied() {
            provenance.insert(scoped_object);
        }
        let mut batch_provenance = outer_tuple.batch_provenance().clone();
        for batch_id in inner_batch_provenance.iter().copied() {
            batch_provenance.insert(batch_id);
        }

        Some(Tuple::new_with_shadow_state(
            vec![TupleElement::Row {
                id: outer_id,
                content: output_content.into(),
                batch_id,
                row_provenance,
            }],
            provenance,
            batch_provenance,
        ))
    }

    fn requirement_satisfied(&self, correlation_value: &Value, array_result: &Value) -> bool {
        let Value::Array(rows) = array_result else {
            return self.requirement == ArraySubqueryRequirement::Optional;
        };

        match self.requirement {
            ArraySubqueryRequirement::Optional => true,
            ArraySubqueryRequirement::AtLeastOne => !rows.is_empty(),
            ArraySubqueryRequirement::MatchCorrelationCardinality => match correlation_value {
                Value::Array(elements) => rows.len() == elements.len(),
                Value::Null => false,
                _ => rows.len() == 1,
            },
        }
    }

    /// Whether every cached subgraph serving this correlation value exists,
    /// is bound to the current correlation, owes the pending-dirt buffer
    /// nothing, and carries no dirty nodes — in which case re-evaluating it is
    /// provably a no-op (a settle over a clean bitmap evaluates zero nodes, so
    /// the output tuples cannot have moved).
    ///
    /// The generation check is what keeps deferral invisible: an instance with
    /// buffered dirt still owed to it is NOT clean, exactly as it would not
    /// have been under eager marking, so it is re-evaluated on the same tick.
    fn subgraph_state_clean(&self, outer_id: ObjectId, correlation_value: &Value) -> bool {
        let generation = self.pending_inner_dirt.generation;
        let element_clean = |index: usize, element: &Value| {
            matches!(
                self.subgraph_cache.get(&(outer_id, index)),
                Some(cached) if &cached.correlation_value == element
                    && cached.applied_generation == generation
                    && !cached.instance.graph.has_dirty_nodes()
            )
        };
        match correlation_value {
            Value::Array(elements) => elements
                .iter()
                .enumerate()
                .all(|(index, element)| element_clean(index, element)),
            single => element_clean(0, single),
        }
    }

    /// Re-evaluate instances when inner data changes.
    /// Returns deltas for any arrays that changed.
    ///
    /// With precise dirtiness enabled only instances whose subgraphs actually
    /// carry dirt (or need a fresh compile) are re-evaluated; the rest are
    /// skipped outright, which is what makes an uncorrelated write cost
    /// O(|changed ids|) per live instance instead of a full re-scan per
    /// instance per settle (the settle-spin fix, design §5). The legacy path
    /// re-evaluates every instance unconditionally.
    pub fn reevaluate_all<F>(&mut self, io: &dyn Storage, row_loader: &mut F) -> TupleDelta
    where
        F: FnMut(ObjectId, Option<TableName>) -> Option<LoadedRow>,
    {
        let mut result = TupleDelta::new();

        // Clear inner_dirty flag
        self.inner_dirty = false;

        // Snapshot the IDS to re-evaluate, never the state behind them.
        //
        // This used to clone the whole `ArrayInstanceState` per instance —
        // outer tuple, materialised array, provenance sets — purely to dodge
        // the borrow conflict with `&mut self` below. That deep copy ran on
        // every settle pass reaching this node, for every live instance, and
        // showed up on the live server as `Vec<TupleElement>::clone` under
        // `reevaluate_all`. Ids are `Copy`; the state is looked up again
        // inside the loop, where the immutable borrow ends before any mutation
        // needs one. Evaluation order, eviction and emitted deltas are
        // unchanged: the map is not mutated between the collect and the
        // lookups, so each instance sees exactly the state the snapshot held.
        let precise = precise_dirty_enabled();
        let outer_ids: Vec<ObjectId> = self
            .instances
            .iter()
            .filter(|(id, state)| {
                !precise || !self.subgraph_state_clean(**id, &state.correlation_value)
            })
            .map(|(id, _)| *id)
            .collect();

        for outer_id in outer_ids {
            // The correlation value is the one piece `evaluate_subgraph` needs
            // while holding `&mut self`: a bound id or a small id array, not a
            // materialised result set.
            let Some(correlation_value) = self
                .instances
                .get(&outer_id)
                .map(|state| state.correlation_value.clone())
            else {
                continue;
            };

            let (new_array, new_provenance, new_batch_provenance) =
                self.evaluate_subgraph(outer_id, &correlation_value, io, row_loader);

            let Some(old_state) = self.instances.get(&outer_id) else {
                continue;
            };
            if old_state.array_result == new_array
                && old_state.provenance == new_provenance
                && old_state.batch_provenance == new_batch_provenance
            {
                continue;
            }

            let old_tuple = self.build_output_tuple(
                &old_state.outer_tuple,
                &old_state.correlation_value,
                &old_state.array_result,
                &old_state.provenance,
                &old_state.batch_provenance,
            );
            let new_tuple = self.build_output_tuple(
                &old_state.outer_tuple,
                &old_state.correlation_value,
                &new_array,
                &new_provenance,
                &new_batch_provenance,
            );

            match (old_tuple, new_tuple) {
                (Some(old_tuple), Some(new_tuple)) => {
                    result.updated.push((old_tuple.clone(), new_tuple.clone()));
                    self.current_tuples.remove(&old_tuple);
                    self.current_tuples.insert(new_tuple);
                }
                (Some(old_tuple), None) => {
                    self.current_tuples.remove(&old_tuple);
                    result.removed.push(old_tuple);
                }
                (None, Some(new_tuple)) => {
                    self.current_tuples.insert(new_tuple.clone());
                    result.added.push(new_tuple);
                }
                (None, None) => {}
            }

            // Outer tuple and correlation stay as they were; only the
            // evaluated result moves — the same fields the re-insert used to
            // carry over, without rebuilding the entry.
            if let Some(state) = self.instances.get_mut(&outer_id) {
                state.array_result = new_array;
                state.provenance = new_provenance;
                state.batch_provenance = new_batch_provenance;
            }
        }

        result
    }

    /// Get the output tuple descriptor.
    pub fn output_tuple_descriptor(&self) -> &TupleDescriptor {
        &self.output_tuple_descriptor
    }
}

impl RowNode for ArraySubqueryNode {
    fn output_descriptor(&self) -> &RowDescriptor {
        &self.output_descriptor
    }

    fn process(&mut self, input: TupleDelta) -> TupleDelta {
        // This is a simplified process that doesn't have access to io/om.
        // Real processing should use process_with_context.
        // For now, just pass through with empty arrays.
        let mut result = TupleDelta::new();

        for tuple in input.removed {
            if let Some(outer_id) = tuple.first_id() {
                self.instances.remove(&outer_id);
            }
            let correlation_value = self
                .extract_correlation_value(&tuple)
                .unwrap_or(Value::Null);
            if let Some(output) = self.build_output_tuple(
                &tuple,
                &correlation_value,
                &Value::Array(vec![]),
                &TupleProvenance::default(),
                &TupleBatchProvenance::default(),
            ) {
                self.current_tuples.remove(&output);
                result.removed.push(output);
            }
        }

        for tuple in input.added {
            if let (Some(outer_id), Some(correlation_value)) =
                (tuple.first_id(), self.extract_correlation_value(&tuple))
            {
                // Without context, we can't evaluate - store empty array
                self.instances.insert(
                    outer_id,
                    ArrayInstanceState {
                        outer_tuple: tuple.clone(),
                        correlation_value,
                        array_result: Value::Array(vec![]),
                        provenance: TupleProvenance::default(),
                        batch_provenance: TupleBatchProvenance::default(),
                    },
                );
            }
            let correlation_value = self
                .extract_correlation_value(&tuple)
                .unwrap_or(Value::Null);
            if let Some(output) = self.build_output_tuple(
                &tuple,
                &correlation_value,
                &Value::Array(vec![]),
                &TupleProvenance::default(),
                &TupleBatchProvenance::default(),
            ) {
                self.current_tuples.insert(output.clone());
                result.added.push(output);
            }
        }

        self.dirty = false;
        result
    }

    fn current_tuples(&self) -> &AHashSet<Tuple> {
        &self.current_tuples
    }

    fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    fn is_dirty(&self) -> bool {
        self.dirty
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_manager::graph_nodes::subgraph::SubgraphBuilder;
    use crate::query_manager::types::TableName;

    fn test_schema() -> Schema {
        let mut schema = Schema::new();
        schema.insert(
            TableName::new("users"),
            RowDescriptor::new(vec![
                ColumnDescriptor::new("id", ColumnType::Integer),
                ColumnDescriptor::new("name", ColumnType::Text),
            ])
            .into(),
        );
        schema.insert(
            TableName::new("posts"),
            RowDescriptor::new(vec![
                ColumnDescriptor::new("id", ColumnType::Integer),
                ColumnDescriptor::new("title", ColumnType::Text),
                ColumnDescriptor::new("author_id", ColumnType::Integer),
            ])
            .into(),
        );
        schema
    }

    #[test]
    fn array_subquery_node_creates_output_descriptor() {
        let schema = test_schema();

        let outer_descriptor = TupleDescriptor::single_with_materialization(
            "users",
            schema
                .get(&TableName::new("users"))
                .unwrap()
                .columns
                .clone(),
            true,
        );

        let template = SubgraphBuilder::new("posts")
            .correlate("author_id")
            .select(&["id", "title"])
            .build(&schema)
            .unwrap();

        let node = ArraySubqueryNode::new(
            outer_descriptor,
            template,
            Correlate::Col(0),
            ArraySubqueryRequirement::Optional,
            "posts".to_string(),
            Arc::new(schema),
        );

        // Output should have: id, name, posts (array)
        assert_eq!(node.output_descriptor().columns.len(), 3);
        assert_eq!(node.output_descriptor().columns[0].name, "id");
        assert_eq!(node.output_descriptor().columns[1].name, "name");
        assert_eq!(node.output_descriptor().columns[2].name, "posts");
    }

    #[test]
    fn array_subquery_extracts_correlation_value() {
        let schema = test_schema();

        let outer_descriptor = TupleDescriptor::single_with_materialization(
            "users",
            schema
                .get(&TableName::new("users"))
                .unwrap()
                .columns
                .clone(),
            true,
        );

        let template = SubgraphBuilder::new("posts")
            .correlate("author_id")
            .build(&schema)
            .unwrap();

        let node = ArraySubqueryNode::new(
            outer_descriptor,
            template,
            Correlate::Col(0),
            ArraySubqueryRequirement::Optional,
            "posts".to_string(),
            Arc::new(schema.clone()),
        );

        // Create a tuple with user id=42
        let user_values = vec![Value::Integer(42), Value::Text("Alice".into())];
        let user_row_desc = &schema.get(&TableName::new("users")).unwrap().columns;
        let user_data = encode_row(user_row_desc, &user_values).unwrap();
        let user_tuple = Tuple::new(vec![TupleElement::Row {
            id: ObjectId::new(),
            content: user_data.into(),
            batch_id: crate::row_histories::BatchId([0; 16]),
            row_provenance: crate::metadata::RowProvenance::for_insert("jazz:test", 0),
        }]);

        let correlation = node.extract_correlation_value(&user_tuple);
        assert_eq!(correlation, Some(Value::Integer(42)));
    }

    #[test]
    fn array_subquery_extracts_object_id_correlation_value() {
        let schema = test_schema();

        let outer_descriptor = TupleDescriptor::single_with_materialization(
            "users",
            schema
                .get(&TableName::new("users"))
                .unwrap()
                .columns
                .clone(),
            true,
        );

        let template = SubgraphBuilder::new("posts")
            .correlate("author_id")
            .build(&schema)
            .unwrap();

        let node = ArraySubqueryNode::new(
            outer_descriptor,
            template,
            Correlate::Id,
            ArraySubqueryRequirement::Optional,
            "posts".to_string(),
            Arc::new(schema.clone()),
        );

        let row_id = ObjectId::new();
        let user_values = vec![Value::Integer(42), Value::Text("Alice".into())];
        let user_row_desc = &schema.get(&TableName::new("users")).unwrap().columns;
        let user_data = encode_row(user_row_desc, &user_values).unwrap();
        let user_tuple = Tuple::new(vec![TupleElement::Row {
            id: row_id,
            content: user_data.into(),
            batch_id: crate::row_histories::BatchId([0; 16]),
            row_provenance: crate::metadata::RowProvenance::for_insert("jazz:test", 0),
        }]);

        let correlation = node.extract_correlation_value(&user_tuple);
        assert_eq!(correlation, Some(Value::Uuid(row_id)));
    }

    /// Build a node whose subgraph cache can be populated directly.
    fn cache_test_node() -> ArraySubqueryNode {
        let schema = test_schema();
        let outer_descriptor = TupleDescriptor::single_with_materialization(
            "users",
            schema
                .get(&TableName::new("users"))
                .unwrap()
                .columns
                .clone(),
            true,
        );
        let template = SubgraphBuilder::new("posts")
            .correlate("author_id")
            .select(&["id", "title"])
            .build(&schema)
            .unwrap();
        ArraySubqueryNode::new(
            outer_descriptor,
            template,
            Correlate::Col(0),
            ArraySubqueryRequirement::Optional,
            "posts".to_string(),
            Arc::new(schema),
        )
    }

    fn seed_cache_entry(node: &mut ArraySubqueryNode, key: (ObjectId, usize), last_used: u64) {
        let correlation_value = Value::Integer(key.1 as i32);
        let instance = node
            .subgraph_template
            .instantiate(correlation_value.clone(), &node.schema)
            .expect("subgraph instantiates");
        node.subgraph_cache.insert(
            key,
            CachedSubgraph {
                correlation_value,
                instance,
                last_used,
                // Freshly instantiated: no buffered dirt has been applied to
                // it yet, so it starts behind the current pending generation.
                applied_generation: 0,
            },
        );
    }

    #[test]
    fn forgetting_an_outer_row_drops_only_its_subgraphs() {
        let mut node = cache_test_node();
        let kept = ObjectId::new();
        let dropped = ObjectId::new();
        seed_cache_entry(&mut node, (kept, 0), 1);
        seed_cache_entry(&mut node, (dropped, 0), 2);
        // A UUID[] forward include caches one subgraph per array element.
        seed_cache_entry(&mut node, (dropped, 1), 3);
        assert_eq!(node.subgraph_cache.len(), 3);

        node.forget_cached_subgraphs(dropped);

        assert_eq!(node.subgraph_cache.len(), 1);
        assert!(node.subgraph_cache.contains_key(&(kept, 0)));
    }

    #[test]
    fn eviction_removes_the_least_recently_used_entry() {
        let mut node = cache_test_node();
        let oldest = ObjectId::new();
        let newer = ObjectId::new();
        let newest = ObjectId::new();
        seed_cache_entry(&mut node, (oldest, 0), 1);
        seed_cache_entry(&mut node, (newer, 0), 5);
        seed_cache_entry(&mut node, (newest, 0), 9);

        // Pretend the map is at capacity so one insert has to make room.
        while node.subgraph_cache.len() < MAX_CACHED_SUBGRAPHS {
            seed_cache_entry(&mut node, (ObjectId::new(), 0), 100);
        }
        node.evict_subgraphs_over_capacity(&(ObjectId::new(), 0));

        assert!(!node.subgraph_cache.contains_key(&(oldest, 0)));
        assert!(node.subgraph_cache.contains_key(&(newer, 0)));
        assert!(node.subgraph_cache.contains_key(&(newest, 0)));
    }

    #[test]
    fn eviction_is_a_noop_below_capacity() {
        let mut node = cache_test_node();
        let only = ObjectId::new();
        seed_cache_entry(&mut node, (only, 0), 1);

        node.evict_subgraphs_over_capacity(&(ObjectId::new(), 0));

        assert_eq!(node.subgraph_cache.len(), 1);
    }
}
