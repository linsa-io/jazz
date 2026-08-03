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
}

#[derive(Debug)]
struct CachedSubgraph {
    correlation_value: Value,
    instance: SubgraphInstance,
    last_used: u64,
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
        }
    }

    /// Drop every cached subgraph belonging to an outer row.
    ///
    /// Called when the row leaves the result set, so the cache tracks live rows rather
    /// than every row ever seen. Without this the cache is what grows without bound —
    /// the capacity ceiling is only a backstop.
    fn forget_cached_subgraphs(&mut self, outer_id: ObjectId) {
        self.subgraph_cache.retain(|(id, _), _| *id != outer_id);
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
            self.subgraph_cache.remove(&victim);
        }
    }

    /// Table-level sibling of [`Self::note_inner_rows_changed`] for dirt with
    /// no row information (F2): with precise dirtiness enabled the table mark
    /// is threaded into every cached subgraph instance, whose scans on that
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
        for cached in self.subgraph_cache.values_mut() {
            cached.instance.graph.mark_dirty_for_table(table);
        }
    }

    /// Row-precise sibling of [`Self::note_inner_table_dirty`] (F2): `ids`
    /// changed in `table` (one of this include's inner tables — the direct
    /// one or a nested one, both registered against this node at compile
    /// time).
    ///
    /// With precise dirtiness enabled the ids are threaded into every cached
    /// subgraph instance: the instance's index scans learn exactly which rows
    /// changed (point-membership re-checks instead of full rescans), and the
    /// instance's own `mark_table_dependents_dirty` routes nested-table ids
    /// onward into nested ArraySubqueryNodes — the recursion that fixes
    /// nested-include staleness (FINDING manifestation 2 in
    /// `manager_tests/subscription_output_oracle.rs`). Content re-loads for
    /// held rows arrive separately via [`Self::forward_rows_updated`].
    ///
    /// With the kill switch off this degrades to the legacy coarse mark; the
    /// reused instances are then `mark_all_dirty`-ed at evaluation time (see
    /// `evaluate_subgraph_for_single`).
    pub fn note_inner_rows_changed(&mut self, table: &str, ids: &AHashSet<ObjectId>) {
        self.inner_dirty = true;
        if !precise_dirty_enabled() {
            return;
        }
        for cached in self.subgraph_cache.values_mut() {
            cached
                .instance
                .graph
                .mark_rows_changed_for_table(table, ids);
        }
    }

    /// Forward a content-update mark into every cached subgraph instance so
    /// include-inner materializers re-load rows they already hold (FINDING
    /// manifestation 1). Table-blind by design — ids not held by an instance
    /// are ignored there — mirroring the outer graph's `mark_rows_updated`
    /// contract. Returns whether any instance actually tracked one of the
    /// ids; only then does this node need re-evaluation (an untracked mark
    /// cannot change any array, and treating it as dirt would make every
    /// uncorrelated write re-settle every instance — the churn the precise
    /// path exists to remove). Legacy mode forwards nothing, preserving the
    /// old behavior byte for byte.
    pub fn forward_row_updated(&mut self, id: ObjectId) -> bool {
        if !precise_dirty_enabled() {
            return false;
        }
        let mut any_tracked = false;
        for cached in self.subgraph_cache.values_mut() {
            any_tracked |= cached.instance.graph.mark_row_updated(id);
        }
        self.inner_dirty |= any_tracked;
        any_tracked
    }

    /// Plural sibling of [`Self::forward_row_updated`].
    pub fn forward_rows_updated(&mut self, ids: &AHashSet<ObjectId>) -> bool {
        if !precise_dirty_enabled() {
            return false;
        }
        let mut any_tracked = false;
        for cached in self.subgraph_cache.values_mut() {
            any_tracked |= cached.instance.graph.mark_rows_updated(ids);
        }
        self.inner_dirty |= any_tracked;
        any_tracked
    }

    /// Forward a deletion mark into every cached subgraph instance — the
    /// removal-delta counterpart of [`Self::forward_row_updated`].
    pub fn forward_row_deleted(&mut self, id: ObjectId) -> bool {
        if !precise_dirty_enabled() {
            return false;
        }
        let mut any_tracked = false;
        for cached in self.subgraph_cache.values_mut() {
            any_tracked |= cached.instance.graph.mark_row_deleted(id);
        }
        self.inner_dirty |= any_tracked;
        any_tracked
    }

    /// Plural sibling of [`Self::forward_row_deleted`].
    pub fn forward_rows_deleted(&mut self, ids: &AHashSet<ObjectId>) -> bool {
        if !precise_dirty_enabled() {
            return false;
        }
        let mut any_tracked = false;
        for cached in self.subgraph_cache.values_mut() {
            any_tracked |= cached.instance.graph.mark_rows_deleted(ids);
        }
        self.inner_dirty |= any_tracked;
        any_tracked
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
                    self.subgraph_cache.insert(
                        cache_key,
                        CachedSubgraph {
                            correlation_value: correlation_value.clone(),
                            instance: fresh,
                            last_used: self.subgraph_cache_clock,
                        },
                    );
                }
                None => {
                    self.subgraph_cache.remove(&cache_key);
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
    /// is bound to the current correlation, and carries no dirty nodes — in
    /// which case re-evaluating it is provably a no-op (a settle over a clean
    /// bitmap evaluates zero nodes, so the output tuples cannot have moved).
    fn subgraph_state_clean(&self, outer_id: ObjectId, correlation_value: &Value) -> bool {
        let element_clean = |index: usize, element: &Value| {
            matches!(
                self.subgraph_cache.get(&(outer_id, index)),
                Some(cached) if &cached.correlation_value == element
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

        // Collect state snapshots to avoid borrow issues during re-evaluation.
        let precise = precise_dirty_enabled();
        let instances_snapshot: Vec<(ObjectId, ArrayInstanceState)> = self
            .instances
            .iter()
            .filter(|(id, state)| {
                !precise || !self.subgraph_state_clean(**id, &state.correlation_value)
            })
            .map(|(id, state)| (*id, state.clone()))
            .collect();

        for (outer_id, old_state) in instances_snapshot {
            // Re-evaluate subgraph
            let (new_array, new_provenance, new_batch_provenance) =
                self.evaluate_subgraph(outer_id, &old_state.correlation_value, io, row_loader);

            if old_state.array_result != new_array
                || old_state.provenance != new_provenance
                || old_state.batch_provenance != new_batch_provenance
            {
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

                self.instances.insert(
                    outer_id,
                    ArrayInstanceState {
                        outer_tuple: old_state.outer_tuple.clone(),
                        correlation_value: old_state.correlation_value.clone(),
                        array_result: new_array,
                        provenance: new_provenance,
                        batch_provenance: new_batch_provenance,
                    },
                );
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
