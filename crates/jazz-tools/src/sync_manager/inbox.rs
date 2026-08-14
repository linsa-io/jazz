use super::*;
use crate::batch_fate::{BatchFate, BatchMode, SealedBatchSubmission};
use crate::metadata::MetadataKey;
use crate::object::{BranchName, ObjectId};
use crate::query_manager::policy::Operation;
use crate::row_histories::{
    ApplyRowBatchWithContext, RowState, RowVisibilityChange, StoredRowBatch, apply_row_batch,
    apply_row_batch_with_context, patch_row_batch_state,
};
use crate::storage::{
    PreparedRowWriteContext, RowLocator, Storage, metadata_from_row_locator,
    prepared_row_table_context_for_schema_hash, prepared_row_write_context_from_table_context,
    row_locator_from_metadata,
};
use std::collections::{HashMap, HashSet};

struct AppliedRowBatch {
    metadata: HashMap<String, String>,
    row: StoredRowBatch,
    visibility_change: Option<RowVisibilityChange>,
}

/// Whether applying a visible row should also record this server's
/// authoritative fate for the row's batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum AuthoritativeFateRecording {
    Skip,
    AcceptedByLocalAuthority,
}

impl AuthoritativeFateRecording {
    fn should_record(self) -> bool {
        matches!(self, Self::AcceptedByLocalAuthority)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SealedBatchMode {
    Direct,
    Transactional,
}

/// Where an inbound row batch came from when it was applied.
///
/// A refused batch looks the same in the log whether a peer just sent it or
/// the server fed it back to itself from its own queues — and telling those
/// apart on a live server cost this team a day. The apply warn carries it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ApplySource {
    /// Straight off a peer's connection.
    ClientFrame,
    /// Released by the permission queue after its policy check settled.
    PermissionApproval,
    /// Replicated down from an upstream server.
    UpstreamServer,
}

impl ApplySource {
    fn as_str(self) -> &'static str {
        match self {
            Self::ClientFrame => "client_frame",
            Self::PermissionApproval => "permission_approval",
            Self::UpstreamServer => "upstream_server",
        }
    }
}

impl SyncManager {
    fn retain_client_batch_fate(&mut self, fate: &BatchFate) -> bool {
        tracing::debug!(
            batch_id = ?fate.batch_id(),
            "ignoring client-sent batch fate; authoritative fates are server-owned"
        );
        false
    }

    fn validate_sealed_batch_submission(
        &self,
        submission: &SealedBatchSubmission,
    ) -> Result<BranchName, BatchFate> {
        if submission.members.is_empty() {
            return Err(BatchFate::Rejected {
                batch_id: submission.batch_id,
                code: "invalid_batch_submission".to_string(),
                reason: "sealed batch must declare at least one member".to_string(),
            });
        }

        if submission.batch_digest
            != SealedBatchSubmission::compute_batch_digest(&submission.members)
        {
            return Err(BatchFate::Rejected {
                batch_id: submission.batch_id,
                code: "invalid_batch_submission".to_string(),
                reason: "sealed batch digest does not match declared members".to_string(),
            });
        }

        Ok(submission.target_branch_name)
    }

    fn validate_batch_rows_target_branch(
        &self,
        submission: &SealedBatchSubmission,
        batch_rows: &[(String, StoredRowBatch)],
    ) -> Result<(), BatchFate> {
        if batch_rows.iter().any(|(_, row)| {
            row.batch_id == submission.batch_id
                && row.branch.as_str() != submission.target_branch_name.as_str()
        }) {
            return Err(BatchFate::Rejected {
                batch_id: submission.batch_id,
                code: "invalid_batch_submission".to_string(),
                reason: "sealed batch rows must belong to the declared target branch".to_string(),
            });
        }

        Ok(())
    }

    fn infer_sealed_batch_mode(
        &self,
        submission: &SealedBatchSubmission,
        batch_rows: &[(String, StoredRowBatch)],
    ) -> Result<Option<SealedBatchMode>, BatchFate> {
        let mut mode = None;
        for (_, row) in batch_rows {
            let row_mode = match row.state {
                RowState::VisibleDirect => SealedBatchMode::Direct,
                RowState::StagingPending => match submission.mode {
                    BatchMode::Direct => SealedBatchMode::Direct,
                    BatchMode::Transactional => SealedBatchMode::Transactional,
                },
                _ => {
                    return Err(BatchFate::Rejected {
                        batch_id: submission.batch_id,
                        code: "invalid_batch_submission".to_string(),
                        reason: "sealed batch rows must be visible direct or staging pending"
                            .to_string(),
                    });
                }
            };

            match mode {
                Some(existing) if existing != row_mode => {
                    return Err(BatchFate::Rejected {
                        batch_id: submission.batch_id,
                        code: "invalid_batch_submission".to_string(),
                        reason: "sealed batch mixes direct and transactional rows".to_string(),
                    });
                }
                Some(_) => {}
                None => mode = Some(row_mode),
            }
        }

        Ok(mode)
    }

    fn parent_frontier_conflict_fate(&self, batch_id: crate::row_histories::BatchId) -> BatchFate {
        BatchFate::Rejected {
            batch_id,
            code: "transaction_conflict".to_string(),
            reason: "row visible parent changed since transaction write was staged".to_string(),
        }
    }

    fn normalize_frontier(
        mut frontier: Vec<crate::row_histories::BatchId>,
    ) -> Vec<crate::row_histories::BatchId> {
        frontier.sort();
        frontier.dedup();
        frontier
    }

    fn validate_transactional_parent_frontiers<H: Storage>(
        &self,
        storage: &H,
        submission: &SealedBatchSubmission,
        declared_rows: &[(String, StoredRowBatch)],
    ) -> Result<(), BatchFate> {
        for (table, row) in declared_rows {
            let expected_frontier = Self::normalize_frontier(row.parents.iter().copied().collect());
            let current_frontier = storage
                .load_visible_region_frontier(
                    table,
                    submission.target_branch_name.as_str(),
                    row.row_id,
                )
                .map_err(|error| BatchFate::Rejected {
                    batch_id: submission.batch_id,
                    code: "invalid_batch_submission".to_string(),
                    reason: format!("failed to load row visible parent frontier: {error}"),
                })?
                .map(Self::normalize_frontier)
                .unwrap_or_default();

            if current_frontier != expected_frontier {
                return Err(self.parent_frontier_conflict_fate(submission.batch_id));
            }
        }

        Ok(())
    }

    fn persist_authoritative_batch_fate<H: Storage>(
        &self,
        storage: &mut H,
        fate: &BatchFate,
    ) -> Result<(BatchFate, bool), crate::storage::StorageError> {
        let previous = storage.load_authoritative_batch_fate(fate.batch_id())?;
        let merged = match previous.as_ref() {
            Some(existing) => existing.merged_with(fate),
            None => fate.clone(),
        };
        if previous.as_ref() == Some(&merged) {
            return Ok((merged, false));
        }
        storage
            .upsert_authoritative_batch_fate(&merged)
            .map_err(|error| {
                tracing::trace!(
                    batch_id = ?fate.batch_id(),
                    %error,
                    "failed to persist authoritative batch fate"
                );
                error
            })?;
        Ok((merged, true))
    }

    fn persist_sealed_batch_submission<H: Storage>(
        &self,
        storage: &mut H,
        submission: &SealedBatchSubmission,
    ) -> Result<(), crate::storage::StorageError> {
        storage
            .upsert_sealed_batch_submission(submission)
            .map_err(|error| {
                tracing::trace!(
                    batch_id = ?submission.batch_id,
                    %error,
                    "failed to persist sealed batch submission"
                );
                error
            })
    }

    fn ensure_object_metadata<H: Storage>(
        &mut self,
        storage: &mut H,
        object_id: ObjectId,
        metadata: HashMap<String, String>,
    ) -> (bool, bool) {
        let existing_row_locator = storage.load_row_locator(object_id).ok().flatten();
        let Some(metadata_row_locator) = crate::storage::row_locator_from_metadata(&metadata)
        else {
            return (false, false);
        };
        let metadata_schema_hash = metadata_row_locator.origin_schema_hash;
        match existing_row_locator {
            Some(existing_row_locator) => (
                false,
                existing_row_locator.origin_schema_hash != metadata_schema_hash,
            ),
            None => {
                let _ = storage.put_row_locator(object_id, Some(&metadata_row_locator));
                (true, false)
            }
        }
    }

    fn row_metadata_from_payload<H: Storage>(
        &self,
        storage: &H,
        row: &StoredRowBatch,
        metadata: Option<&RowMetadata>,
    ) -> Option<HashMap<String, String>> {
        if let Some(metadata) = metadata {
            return Some(metadata.metadata.clone());
        }

        let locator = storage
            .load_row_locator(row.row_id)
            .ok()
            .flatten()
            .map(|locator| metadata_from_row_locator(&locator));
        if locator.is_none() {
            // The caller drops the row on `None`, so without this the peer
            // discards a row it was just sent and says nothing. That silence is
            // what made the offline-delivery defect take a night to find: the
            // row was delivered, applied nowhere, and no log line existed
            // between "sent" and "missing".
            tracing::warn!(
                row_id = %row.row_id,
                branch = %row.branch,
                batch_id = ?row.batch_id,
                "discarding a row batch that carries no metadata and has no local locator: \
                 the table cannot be resolved, so the row is unusable",
            );
        }
        locator
    }

    fn row_context_from_metadata<H: Storage>(
        &mut self,
        storage: &H,
        metadata: &HashMap<String, String>,
        needs_exact_locator: bool,
    ) -> Option<(RowLocator, PreparedRowWriteContext)> {
        let row_locator = row_locator_from_metadata(metadata)?;
        let schema_hash = row_locator.origin_schema_hash?;
        let table = row_locator.table.to_string();
        let cache_key = (table.clone(), schema_hash);
        let table_context = if let Some(context) = self.replay_table_contexts.get(&cache_key) {
            context.clone()
        } else {
            let context =
                prepared_row_table_context_for_schema_hash(storage, &table, schema_hash).ok()?;
            self.replay_table_contexts
                .insert(cache_key, context.clone());
            context
        };
        let write_context =
            prepared_row_write_context_from_table_context(table_context, needs_exact_locator);
        Some((row_locator, write_context))
    }

    fn matches_replayed_row_batch(existing: &StoredRowBatch, incoming: &StoredRowBatch) -> bool {
        existing.row_id == incoming.row_id
            && existing.batch_id == incoming.batch_id
            && existing.branch == incoming.branch
            && existing.parents == incoming.parents
            && existing.updated_at == incoming.updated_at
            && existing.created_by == incoming.created_by
            && existing.created_at == incoming.created_at
            && existing.updated_by == incoming.updated_by
            && existing.state == incoming.state
            && existing.delete_kind == incoming.delete_kind
            && existing.is_deleted == incoming.is_deleted
            && existing.data == incoming.data
            && existing.metadata == incoming.metadata
    }

    /// Returns the row's visible content immediately before the incoming batch
    /// This "old" content is used to permission-check the change as an operation
    /// from old content to new content
    fn pre_batch_visible_row<H: Storage>(
        &self,
        storage: &H,
        table: &str,
        row: &StoredRowBatch,
    ) -> Option<StoredRowBatch> {
        let row_locator = storage.load_row_locator(row.row_id).ok().flatten();
        if row.parents.is_empty() && row_locator.is_none() {
            return None;
        }
        // If the row has a single parent, we don't need any conflict resolution
        if let [parent_batch_id] = row.parents.as_slice() {
            let parent_row = storage
                .load_history_row_batch(table, row.branch.as_str(), row.row_id, *parent_batch_id)
                .ok()
                .flatten()?;
            return (parent_row.batch_id != row.batch_id && parent_row.state.is_visible())
                .then_some(parent_row);
        }

        // A batch with NO parents has no ancestry to resolve. The walk below
        // would return every visible version or nothing at all, decided purely
        // by whether they all sit on this batch's branch — and reading the
        // whole history is a costly way to ask that. Ask the branch question
        // directly: the history rows on the OTHER branches. When there are
        // none (the single-branch case) the walk's answer is `None`, and the
        // read is skipped entirely.
        //
        // This is not a corner: the server classifies a write with no parents
        // and no visible old content as an insert, so a diverged client's
        // writes all take this path. Production 2026-08-10 — a `users` row
        // grown to 2541 versions by presence heartbeats — paid a 2541-row read
        // per attempt, with the runtime mutex held, in bursts of hundreds a
        // minute.
        if row.parents.is_empty()
            && let Ok(Some(sole_branch)) = crate::storage::sole_branch_name(storage)
            && sole_branch.as_str() == row.branch.as_str()
        {
            // Every version this row has is on the incoming batch's branch, so
            // the walk below would disable its fallback and answer `None`.
            return None;
        }

        let history_table = row_locator
            .as_ref()
            .map(|locator| locator.table.to_string())
            .unwrap_or_else(|| table.to_string());
        let context =
            crate::storage::resolve_history_row_write_context(storage, &history_table, row).ok()?;

        // The ancestors this batch declares, fetched one by one rather than
        // sieved out of the row's whole history. The walk below only ever
        // reaches versions linked from `row.parents`, so the history read was
        // fetching thousands of rows to answer a question about a handful — and
        // when the parents are absent it fetched them to find nothing at all.
        //
        // Production 2026-08-14: a diverged client's writes against a `users`
        // row that presence heartbeats had grown to thousands of versions,
        // 23,661 refusals over 803 batches, every multi-parent one reading the
        // whole history under the runtime mutex before being refused for absent
        // parents. One core at 98%, the log silent for an hour.
        let mut ancestors = Vec::new();
        let mut seen = HashSet::new();
        let mut frontier = row.parents.clone();
        while let Some(batch_id) = frontier.pop() {
            if !seen.insert(batch_id) {
                continue;
            }
            let Some(candidate) = storage
                .load_history_row_batch(&history_table, row.branch.as_str(), row.row_id, batch_id)
                .ok()
                .flatten()
            else {
                continue;
            };
            if candidate.batch_id != row.batch_id && candidate.state.is_visible() {
                frontier.extend(candidate.parents.iter().copied());
                ancestors.push(candidate);
            }
        }

        // The sieve handed its rows over sorted; a depth-first pop order is not
        // obviously equivalent for a consumer that turns out to care, and the
        // sort costs nothing on a set this size.
        ancestors.sort_by(|a, b| {
            (a.branch.as_str(), a.updated_at, a.batch_id()).cmp(&(
                b.branch.as_str(),
                b.updated_at,
                b.batch_id(),
            ))
        });

        let pre_batch_rows = if ancestors.is_empty() {
            // No declared ancestor resolves to a visible version — either the
            // batch has no parents, or it names parents this authority does not
            // hold, or a migrated branch dropped the link. The walk answers that
            // case from one fact: does this row exist on another branch at all.
            // If not, its answer is `None`, and asking directly costs a probe
            // per branch instead of the read it used to cost.
            if !crate::storage::row_has_history_on_another_branch(
                storage,
                &history_table,
                row.row_id,
                row.branch.as_str(),
            )
            .ok()?
            {
                return None;
            }
            let visible_rows = storage
                .scan_history_row_batches(&history_table, row.row_id)
                .ok()?
                .into_iter()
                .filter(|candidate| {
                    candidate.batch_id != row.batch_id && candidate.state.is_visible()
                })
                .collect::<Vec<_>>();
            // Recomputed, not assumed. The probe above asks whether ANY history
            // row sits on another branch; this asks whether any VISIBLE one
            // does, which is the question the answer actually turns on. They
            // differ exactly when another branch holds only rejected,
            // superseded or staged versions — and passing `true` there would
            // hand the policy check an `old_content` of `Some(...)` where this
            // function used to answer `None`, turning an insert into an update.
            // The probe is a cost optimisation; it must not become an input.
            let only_incoming_branch = visible_rows
                .iter()
                .all(|candidate| candidate.branch.as_str() == row.branch.as_str());
            Self::history_rows_visible_before_batch(row, visible_rows, !only_incoming_branch)?
        } else {
            ancestors
        };

        crate::row_histories::visible_row_preview_from_history_rows(
            context.user_descriptor().as_ref(),
            &pre_batch_rows,
            None,
        )
        .ok()
        .flatten()
    }

    pub(super) fn history_rows_visible_before_batch(
        row: &StoredRowBatch,
        mut visible_rows: Vec<StoredRowBatch>,
        allow_unresolved_fallback: bool,
    ) -> Option<Vec<StoredRowBatch>> {
        if row.parents.is_empty() {
            return allow_unresolved_fallback.then_some(visible_rows);
        }

        // Resolve the ancestor set by REFERENCE, then narrow the caller's own
        // vector in place. Nothing here copies a row.
        //
        // It used to copy every visible row twice — once to index them by
        // batch id, once to collect the selection — though the walk reads
        // nothing but each candidate's parents, and the selection is a subset
        // of a vector we already own. On a row whose history is long that cost
        // tracked the history, not the change: production 2026-08-10, a
        // presence row at 2541 entries taking ~52 unappliable writes a second
        // meant a quarter of a million row copies a second, payloads and
        // parent vectors included, and a core pinned doing it. A linear
        // history makes every entry an ancestor, so trimming the index alone
        // would only have halved it.
        let included_batch_ids = {
            let visible_rows_by_batch = visible_rows
                .iter()
                .map(|candidate| (candidate.batch_id(), candidate))
                .collect::<HashMap<_, _>>();

            let mut included_batch_ids = HashSet::new();
            let mut frontier = row.parents.iter().copied().collect::<Vec<_>>();
            while let Some(batch_id) = frontier.pop() {
                if !included_batch_ids.insert(batch_id) {
                    continue;
                }
                if let Some(parent_row) = visible_rows_by_batch.get(&batch_id) {
                    frontier.extend(parent_row.parents.iter().copied());
                }
            }
            included_batch_ids
        };

        if !visible_rows
            .iter()
            .any(|candidate| included_batch_ids.contains(&candidate.batch_id()))
        {
            // A migrated branch may not carry parents from the old schema
            // branch, so its first write must still consider all visible rows.
            return allow_unresolved_fallback.then_some(visible_rows);
        }

        visible_rows.retain(|candidate| included_batch_ids.contains(&candidate.batch_id()));
        Some(visible_rows)
    }

    fn apply_row_updated<H: Storage>(
        &mut self,
        storage: &mut H,
        metadata: Option<RowMetadata>,
        mut row: StoredRowBatch,
        fate_recording: AuthoritativeFateRecording,
        source: ApplySource,
    ) -> Option<AppliedRowBatch> {
        let authoritative_tier = match (row.confirmed_tier, self.max_local_durability_tier()) {
            (Some(incoming), Some(local)) => Some(incoming.max(local)),
            (Some(incoming), None) => Some(incoming),
            (None, Some(local)) => Some(local),
            (None, None) => None,
        };
        row.confirmed_tier = None;

        let metadata = self.row_metadata_from_payload(storage, &row, metadata.as_ref())?;
        if matches!(
            storage.load_authoritative_batch_fate(row.batch_id),
            Ok(Some(BatchFate::Rejected { .. }))
        ) {
            let is_declared_member = storage
                .load_sealed_batch_submission(row.batch_id)
                .ok()
                .flatten()
                .is_some_and(|submission| {
                    submission.target_branch_name.as_str() == row.branch.as_str()
                        && submission.members.iter().any(|member| {
                            member.object_id == row.row_id
                                // Both forms of the identity, for the same
                                // reason as `declared_rows_for_submission`:
                                // a peer resending its copy of a DELIVERED row
                                // declares the digest without parents, because
                                // we stripped them. Reading that as "not a
                                // declared member" dropped the row silently
                                // instead of storing it as rejected.
                                && (member.row_digest == row.content_digest()
                                    || member.row_digest
                                        == row.content_digest_ignoring_parents())
                        })
                });
            if !is_declared_member {
                return None;
            }
            row.state = RowState::Rejected;
        }
        let (is_newly_located_object, needs_exact_locator) =
            self.ensure_object_metadata(storage, row.row_id, metadata.clone());
        let branch_name = BranchName::new(&row.branch);
        let visibility_change =
            match self.row_context_from_metadata(storage, &metadata, needs_exact_locator) {
                Some((row_locator, context)) => {
                    let table = row_locator.table.to_string();
                    let branch = row.branch.clone();
                    match apply_row_batch_with_context(
                        storage,
                        ApplyRowBatchWithContext {
                            object_id: row.row_id,
                            branch_name: &branch_name,
                            row: row.clone(),
                            index_mutations: &[],
                            row_locator,
                            table,
                            branch,
                            context,
                            is_known_new_object: is_newly_located_object && row.parents.is_empty(),
                        },
                    ) {
                        Ok(applied) => applied.visibility_change,
                        Err(err) => {
                            tracing::warn!(
                                row_id = %row.row_id,
                                %branch_name,
                                batch_id = ?row.batch_id,
                                source = source.as_str(),
                                parents = row.parents.len(),
                                ?err,
                                "failed to apply synced row batch"
                            );
                            return None;
                        }
                    }
                }
                None => {
                    match apply_row_batch(storage, row.row_id, &branch_name, row.clone(), &[]) {
                        Ok(applied) => applied.visibility_change,
                        Err(err) => {
                            tracing::warn!(
                                row_id = %row.row_id,
                                %branch_name,
                                batch_id = ?row.batch_id,
                                source = source.as_str(),
                                parents = row.parents.len(),
                                ?err,
                                "failed to apply synced row batch"
                            );
                            return None;
                        }
                    }
                }
            };
        if fate_recording.should_record()
            && let Some(confirmed_tier) = authoritative_tier
            && row.state.is_visible()
        {
            let fate = match row.state {
                RowState::VisibleDirect => BatchFate::DurableDirect {
                    batch_id: row.batch_id,
                    confirmed_tier,
                },
                RowState::VisibleTransactional => BatchFate::AcceptedTransaction {
                    batch_id: row.batch_id,
                    confirmed_tier,
                },
                RowState::StagingPending | RowState::Superseded | RowState::Rejected => {
                    unreachable!("row.state.is_visible() guarded non-visible states")
                }
            };
            if let Ok((fate, true)) = self.persist_authoritative_batch_fate(storage, &fate) {
                self.pending_batch_fates.push(fate);
            }
        }

        Some(AppliedRowBatch {
            metadata,
            row,
            visibility_change,
        })
    }

    pub(super) fn respond_to_batch_fate_request<H: Storage>(
        &mut self,
        storage: &H,
        destination: Destination,
        mut batch_ids: Vec<crate::row_histories::BatchId>,
    ) {
        batch_ids.sort();
        batch_ids.dedup();
        // The list is the peer's to choose and nothing capped its length: a frame may name
        // millions of batches, each one a storage read and a queued answer. Capped here
        // rather than at a call site, because the two client arms are easy to mistake for
        // each other and only one of them is on the path a socket takes. Whoever asked —
        // including an upstream, which is trusted but not therefore unlimited — asks again
        // on its next connection, which is the same deferral silence gives.
        if batch_ids.len() > MAX_TRACKED_MISSING_ANSWERS {
            tracing::warn!(
                target: "jazz::sync",
                ?destination, asked = batch_ids.len(), answered = MAX_TRACKED_MISSING_ANSWERS,
                "answering only part of an oversized fate request"
            );
            batch_ids.truncate(MAX_TRACKED_MISSING_ANSWERS);
        }
        for batch_id in batch_ids {
            let fate = self
                .load_batch_fate_by_batch_id_from_storage(storage, batch_id)
                .unwrap_or(BatchFate::Missing { batch_id });
            match destination {
                Destination::Client(client_id) => {
                    // Every other fate is an answer this authority owes and can only give
                    // once it exists. `Missing` is the one that asks for work back, and
                    // where it came from does not change that: a node with an upstream can
                    // hold a STORED Missing, and the replay short-circuit queues a request
                    // here for every replayed row.
                    if matches!(fate, BatchFate::Missing { .. })
                        && !self.may_tell_client_a_batch_is_missing(client_id, batch_id)
                    {
                        continue;
                    }
                    self.queue_batch_fate_to_client_unfiltered(client_id, fate);
                }
                // Not bounded towards a server, and that is a trust argument rather than a
                // mechanical one: `apply_received_batch_fate` turns a `Missing` into a
                // retransmission there exactly as it does on a client. Upstreams are
                // trusted to ask only about batches they hold; if that ever stops being
                // true, this needs the same budget.
                Destination::Server(_) => {
                    self.outbox.push(OutboxEntry {
                        destination: destination.clone(),
                        payload: SyncPayload::BatchFate { fate },
                    });
                }
            }
        }
    }

    pub(super) fn batch_fate_for_client(
        &self,
        client_id: ClientId,
        fate: &BatchFate,
    ) -> Option<BatchFate> {
        self.clients.get(&client_id)?;
        match fate {
            BatchFate::DurableDirect { batch_id, .. }
            | BatchFate::AcceptedTransaction { batch_id, .. }
            | BatchFate::Rejected { batch_id, .. } => {
                let row_interest = self.row_batch_interest.iter().any(|(key, clients)| {
                    key.batch_id == *batch_id && clients.contains(&client_id)
                });
                let fate_interest = self
                    .batch_fate_interest
                    .get(batch_id)
                    .is_some_and(|clients| clients.contains(&client_id));
                (row_interest || fate_interest).then(|| fate.clone())
            }
            BatchFate::Missing { .. } => Some(fate.clone()),
        }
    }

    pub(super) fn interested_clients_for_batch_fate(&self, fate: &BatchFate) -> HashSet<ClientId> {
        match fate {
            BatchFate::DurableDirect { batch_id, .. }
            | BatchFate::AcceptedTransaction { batch_id, .. }
            | BatchFate::Rejected { batch_id, .. } => {
                let mut interested = HashSet::new();
                for (key, clients) in &self.row_batch_interest {
                    if key.batch_id == *batch_id {
                        interested.extend(clients.iter().copied());
                    }
                }
                if let Some(clients) = self.batch_fate_interest.get(batch_id) {
                    interested.extend(clients.iter().copied());
                }
                interested
            }
            BatchFate::Missing { .. } => HashSet::new(),
        }
    }

    fn register_client_batch_fate_interest(
        &mut self,
        client_id: ClientId,
        batch_ids: &[crate::row_histories::BatchId],
    ) {
        // Same list, same reason as in `respond_to_batch_fate_request`: one entry per named
        // batch, held until the client goes away, and the peer picks how many.
        //
        // Past the cap the list is put in the same order that function truncates in, so the
        // batches registered are the batches answered. Taking the peer's raw order instead
        // would answer about batches this client is not recorded as interested in, and that
        // interest is what gates every later fate broadcast.
        if batch_ids.len() <= MAX_TRACKED_MISSING_ANSWERS {
            for batch_id in batch_ids {
                self.batch_fate_interest
                    .entry(*batch_id)
                    .or_default()
                    .insert(client_id);
            }
            return;
        }
        tracing::warn!(
            target: "jazz::sync",
            %client_id, asked = batch_ids.len(), registered = MAX_TRACKED_MISSING_ANSWERS,
            "registering interest in only part of an oversized fate request"
        );
        let mut canonical = batch_ids.to_vec();
        canonical.sort();
        canonical.dedup();
        for batch_id in canonical.iter().take(MAX_TRACKED_MISSING_ANSWERS) {
            self.batch_fate_interest
                .entry(*batch_id)
                .or_default()
                .insert(client_id);
        }
    }

    fn transactional_batch_rows<H: Storage>(
        &self,
        storage: &H,
        batch_id: crate::row_histories::BatchId,
        object_ids: &[ObjectId],
    ) -> Vec<(String, StoredRowBatch)> {
        let object_ids = object_ids.iter().copied().collect::<HashSet<_>>();
        let Ok(Some(batch_rows)) = storage.load_local_batch_row_index(batch_id) else {
            return Vec::new();
        };
        let mut rows = batch_rows
            .into_iter()
            .filter(|member| object_ids.contains(&member.object_id))
            .filter_map(|member| {
                let Ok(Some(row)) = storage.load_history_row_batch_for_schema_hash(
                    member.table_name.as_str(),
                    member.schema_hash,
                    member.branch_name.as_str(),
                    member.object_id,
                    batch_id,
                ) else {
                    return None;
                };
                (row.content_digest() == member.row_digest).then_some((member.table_name, row))
            })
            .collect::<Vec<_>>();

        rows.sort_by(|(_, left), (_, right)| {
            left.row_id
                .uuid()
                .as_bytes()
                .cmp(right.row_id.uuid().as_bytes())
                .then_with(|| left.branch.as_str().cmp(right.branch.as_str()))
                .then_with(|| left.batch_id.0.cmp(&right.batch_id.0))
        });
        rows
    }

    fn known_transactional_batch_rows_for_fate<H: Storage>(
        &self,
        storage: &H,
        batch_id: crate::row_histories::BatchId,
    ) -> Vec<(String, StoredRowBatch)> {
        let mut object_ids = HashSet::new();
        for scope in self.remote_query_scopes.values() {
            object_ids.extend(scope.iter().map(|(object_id, _)| *object_id));
        }
        for key in self.row_batch_interest.keys() {
            if key.batch_id == batch_id {
                object_ids.insert(key.row_id);
            }
        }
        if let Ok(Some(submission)) = storage.load_sealed_batch_submission(batch_id) {
            object_ids.extend(submission.members.iter().map(|member| member.object_id));
            return self.transactional_batch_rows(
                storage,
                batch_id,
                &object_ids.into_iter().collect::<Vec<_>>(),
            );
        }
        self.transactional_batch_rows(
            storage,
            batch_id,
            &object_ids.into_iter().collect::<Vec<_>>(),
        )
    }

    fn metadata_for_batch_row<H: Storage>(
        &self,
        storage: &H,
        table: &str,
        row: &StoredRowBatch,
    ) -> Option<HashMap<String, String>> {
        if let Ok(Some(locator)) = storage.load_history_row_batch_table_locator(
            row.branch.as_str(),
            row.row_id,
            row.batch_id(),
        ) {
            return Some(metadata_from_row_locator(&RowLocator {
                table: locator.table_name,
                origin_schema_hash: Some(locator.schema_hash),
            }));
        }

        storage
            .load_row_locator(row.row_id)
            .ok()
            .flatten()
            .map(|locator| metadata_from_row_locator(&locator))
            .or_else(|| {
                crate::storage::resolve_history_row_write_context(storage, table, row)
                    .ok()
                    .map(|context| {
                        metadata_from_row_locator(&RowLocator {
                            table: table.to_string().into(),
                            origin_schema_hash: Some(
                                context.history_row_raw_table_id().schema_hash,
                            ),
                        })
                    })
            })
    }

    fn apply_row_batch_for_table<H: Storage>(
        &self,
        storage: &mut H,
        table: &str,
        row: StoredRowBatch,
    ) -> Option<crate::row_histories::ApplyRowBatchResult> {
        let context =
            crate::storage::resolve_history_row_write_context(storage, table, &row).ok()?;
        let branch_name = BranchName::new(&row.branch);
        let row_locator = storage
            .load_row_locator(row.row_id)
            .ok()
            .flatten()
            .unwrap_or_else(|| RowLocator {
                table: table.to_string().into(),
                origin_schema_hash: Some(context.history_row_raw_table_id().schema_hash),
            });

        apply_row_batch_with_context(
            storage,
            ApplyRowBatchWithContext {
                object_id: row.row_id,
                branch_name: &branch_name,
                row,
                index_mutations: &[],
                row_locator,
                table: table.to_string(),
                branch: branch_name.as_str().to_string().into(),
                context,
                is_known_new_object: false,
            },
        )
        .ok()
    }

    fn apply_transactional_batch_fate_to_rows<H: Storage>(
        &mut self,
        storage: &mut H,
        origin_client_id: Option<ClientId>,
        origin_server_id: Option<ServerId>,
        fate: &BatchFate,
        batch_rows: &[(String, StoredRowBatch)],
    ) {
        let server_ids: Vec<_> = self
            .servers
            .keys()
            .copied()
            .filter(|server_id| Some(*server_id) != origin_server_id)
            .collect();
        match fate {
            BatchFate::DurableDirect { .. } => {
                for (table, row) in batch_rows {
                    let row_id = row.row_id;
                    let branch_name = BranchName::new(&row.branch);
                    let mut direct_row = row.clone();
                    direct_row.state = RowState::VisibleDirect;
                    direct_row.confirmed_tier = None;
                    let applied =
                        self.apply_row_batch_for_table(storage, table, direct_row.clone());

                    let metadata = self.metadata_for_batch_row(storage, table, &direct_row);

                    if let Some(metadata) = metadata {
                        for server_id in &server_ids {
                            self.outbox.push(OutboxEntry {
                                destination: Destination::Server(*server_id),
                                payload: SyncPayload::RowBatchNeeded {
                                    metadata: Some(RowMetadata {
                                        id: row_id,
                                        metadata: metadata.clone(),
                                    }),
                                    row: direct_row.clone(),
                                },
                            });
                        }
                    }

                    if let Some(applied) = applied
                        && let Some(update) = applied.visibility_change
                    {
                        self.pending_row_visibility_changes.push(update);
                        if let Some(client_id) = origin_client_id {
                            self.forward_update_to_clients_except_with_storage(
                                storage,
                                row_id,
                                branch_name,
                                client_id,
                            );
                        } else {
                            self.forward_update_to_clients_with_storage(
                                storage,
                                row_id,
                                branch_name,
                            );
                        }
                    }
                }

                for server_id in &server_ids {
                    self.outbox.push(OutboxEntry {
                        destination: Destination::Server(*server_id),
                        payload: SyncPayload::BatchFate { fate: fate.clone() },
                    });
                }
            }
            BatchFate::AcceptedTransaction { confirmed_tier, .. } => {
                for (table, row) in batch_rows {
                    let row_id = row.row_id;
                    let branch_name = BranchName::new(&row.branch);
                    let accepted_row = row.accepted_transaction_output(*confirmed_tier);
                    let applied =
                        self.apply_row_batch_for_table(storage, table, accepted_row.clone());

                    let metadata = self.metadata_for_batch_row(storage, table, &accepted_row);

                    if let Some(metadata) = metadata {
                        for server_id in &server_ids {
                            self.outbox.push(OutboxEntry {
                                destination: Destination::Server(*server_id),
                                payload: SyncPayload::RowBatchNeeded {
                                    metadata: Some(RowMetadata {
                                        id: row_id,
                                        metadata: metadata.clone(),
                                    }),
                                    row: accepted_row.clone(),
                                },
                            });
                        }
                    }

                    if let Some(applied) = applied
                        && let Some(update) = applied.visibility_change
                    {
                        self.pending_row_visibility_changes.push(update);
                        if let Some(client_id) = origin_client_id {
                            self.forward_update_to_clients_except_with_storage(
                                storage,
                                row_id,
                                branch_name,
                                client_id,
                            );
                        } else {
                            self.forward_update_to_clients_with_storage(
                                storage,
                                row_id,
                                branch_name,
                            );
                        }
                    }
                }

                for server_id in &server_ids {
                    self.outbox.push(OutboxEntry {
                        destination: Destination::Server(*server_id),
                        payload: SyncPayload::BatchFate { fate: fate.clone() },
                    });
                }
            }
            BatchFate::Rejected { .. } => {
                for (_, row) in batch_rows {
                    let row_id = row.row_id;
                    let branch_name = BranchName::new(&row.branch);
                    let row_batch_id = row.batch_id();

                    let visibility_change = patch_row_batch_state(
                        storage,
                        row_id,
                        &branch_name,
                        row_batch_id,
                        Some(RowState::Rejected),
                        None,
                    )
                    .ok()
                    .flatten();

                    if let Some(update) = visibility_change {
                        self.pending_row_visibility_changes.push(update);
                        if let Some(client_id) = origin_client_id {
                            self.forward_update_to_clients_except_with_storage(
                                storage,
                                row_id,
                                branch_name,
                                client_id,
                            );
                        } else {
                            self.forward_update_to_clients_with_storage(
                                storage,
                                row_id,
                                branch_name,
                            );
                        }
                    }
                }
            }
            BatchFate::Missing { .. } => return,
        }

        if matches!(fate, BatchFate::DurableDirect { .. }) {
            return;
        }

        if let Some(client_id) = origin_client_id {
            self.outbox.push(OutboxEntry {
                destination: Destination::Client(client_id),
                payload: SyncPayload::BatchFate { fate: fate.clone() },
            });
        }
    }

    fn apply_authoritative_transaction_fate_for_row<H: Storage>(
        &mut self,
        storage: &mut H,
        origin_server_id: ServerId,
        row: &StoredRowBatch,
    ) {
        let fate = match storage.load_authoritative_batch_fate(row.batch_id) {
            Ok(Some(fate @ BatchFate::AcceptedTransaction { .. })) => fate,
            Ok(Some(_)) | Ok(None) => return,
            Err(error) => {
                tracing::warn!(
                    batch_id = ?row.batch_id,
                    %error,
                    "failed to load authoritative batch fate for received row"
                );
                return;
            }
        };

        let rows = vec![(
            storage
                .load_row_locator(row.row_id)
                .ok()
                .flatten()
                .map(|locator| locator.table.to_string())
                .unwrap_or_default(),
            row.clone(),
        )];
        self.apply_transactional_batch_fate_to_rows(
            storage,
            None,
            Some(origin_server_id),
            &fate,
            &rows,
        );
    }

    fn reject_sealed_transactional_batch<H: Storage>(
        &mut self,
        storage: &mut H,
        origin_client_id: Option<ClientId>,
        fate: BatchFate,
        batch_rows: &[(String, StoredRowBatch)],
    ) {
        let fate = match self.persist_authoritative_batch_fate(storage, &fate) {
            Ok((fate, true)) => {
                self.pending_batch_fates.push(fate.clone());
                fate
            }
            Ok((fate, false)) => fate,
            Err(_) => return,
        };
        if let Err(error) = storage.delete_sealed_batch_submission(fate.batch_id()) {
            tracing::warn!(
                batch_id = ?fate.batch_id(),
                %error,
                "failed to delete rejected sealed batch submission"
            );
        }
        self.apply_transactional_batch_fate_to_rows(
            storage,
            origin_client_id,
            None,
            &fate,
            batch_rows,
        );
    }

    fn declared_rows_for_submission(
        submission: &SealedBatchSubmission,
        batch_rows: &[(String, StoredRowBatch)],
    ) -> Option<Vec<(String, StoredRowBatch)>> {
        let mut declared_rows = Vec::with_capacity(submission.members.len());
        for member in &submission.members {
            let matching_row = batch_rows.iter().find(|(_, row)| {
                row.row_id == member.object_id
                    && row.branch.as_str() == submission.target_branch_name.as_str()
                    // Either form of the identity: a peer that authored the row
                    // declares the digest with parents, a peer that RECEIVED it
                    // declares one without, because the sender stripped them
                    // (`scope_delivery_row`). Reading the second as a mismatch
                    // made the seal permanently uncompletable, and the answer to
                    // an uncompletable seal — `Missing` — asks the peer to send
                    // the rows again, which is a cycle with no exit (production
                    // 2026-08-10: a pinned core and an empty log).
                    && (row.content_digest() == member.row_digest
                        || row.content_digest_ignoring_parents() == member.row_digest)
            })?;
            declared_rows.push(matching_row.clone());
        }
        Some(declared_rows)
    }

    /// True when this authority's tier outranks an existing `DurableDirect`
    /// fate, so re-validating the seal can promote the batch to this tier.
    fn can_promote_direct_fate(&self, fate: &BatchFate) -> bool {
        matches!(
            fate,
            BatchFate::DurableDirect { confirmed_tier, .. }
                if self
                    .max_local_durability_tier()
                    .is_some_and(|authority_tier| *confirmed_tier < authority_tier)
        )
    }

    fn settle_sealed_batch<H: Storage>(
        &mut self,
        storage: &mut H,
        origin_client_id: Option<ClientId>,
        submission: SealedBatchSubmission,
        batch_rows: Vec<(String, StoredRowBatch)>,
        declared_rows: Vec<(String, StoredRowBatch)>,
        mode: SealedBatchMode,
    ) {
        let batch_id = submission.batch_id;
        let fate = match storage.load_authoritative_batch_fate(batch_id) {
            Ok(Some(BatchFate::DurableDirect { confirmed_tier, .. }))
                if mode == SealedBatchMode::Direct =>
            {
                let confirmed_tier = self
                    .my_tiers
                    .iter()
                    .copied()
                    .max()
                    .map(|authority_tier| authority_tier.max(confirmed_tier))
                    .unwrap_or(confirmed_tier);
                let fate = BatchFate::DurableDirect {
                    batch_id,
                    confirmed_tier,
                };
                let (fate, changed) = match self.persist_authoritative_batch_fate(storage, &fate) {
                    Ok(result) => result,
                    Err(_) => return,
                };
                if changed {
                    self.pending_batch_fates.push(fate.clone());
                }
                fate
            }
            Ok(Some(existing_fate)) => existing_fate,
            Ok(None) => {
                if batch_rows.is_empty() {
                    BatchFate::Missing { batch_id }
                } else {
                    let Some(confirmed_tier) = self.my_tiers.iter().copied().max() else {
                        tracing::warn!(
                            ?batch_id,
                            "received a sealed batch but this node has no durability tier; \
                             dropping settlement - the origin's wait() can only resolve via another peer"
                        );
                        return;
                    };
                    let fate = match mode {
                        SealedBatchMode::Direct => BatchFate::DurableDirect {
                            batch_id,
                            confirmed_tier,
                        },
                        SealedBatchMode::Transactional => BatchFate::AcceptedTransaction {
                            batch_id,
                            confirmed_tier,
                        },
                    };
                    let (fate, changed) =
                        match self.persist_authoritative_batch_fate(storage, &fate) {
                            Ok(result) => result,
                            Err(_) => return,
                        };
                    if changed {
                        self.pending_batch_fates.push(fate.clone());
                    }
                    fate
                }
            }
            Err(error) => {
                tracing::warn!(?batch_id, %error, "failed to load authoritative batch fate");
                return;
            }
        };

        if self.batch_fate_is_settled(&fate)
            && let Err(error) = storage.delete_sealed_batch_submission(batch_id)
        {
            tracing::warn!(?batch_id, %error, "failed to delete sealed batch submission");
        }
        let rows_to_patch: &[(String, StoredRowBatch)] = match fate {
            BatchFate::DurableDirect { .. } | BatchFate::AcceptedTransaction { .. } => {
                &declared_rows
            }
            BatchFate::Rejected { .. } => &batch_rows,
            BatchFate::Missing { .. } => &[],
        };
        self.apply_transactional_batch_fate_to_rows(
            storage,
            origin_client_id,
            None,
            &fate,
            rows_to_patch,
        );

        if matches!(fate, BatchFate::DurableDirect { .. }) {
            let mut interested_clients = self.interested_clients_for_batch_fate(&fate);
            if let Some(client_id) = origin_client_id {
                self.queue_batch_fate_to_client_unfiltered(client_id, fate.clone());
                interested_clients.remove(&client_id);
            }
            for client_id in interested_clients {
                self.queue_batch_fate_to_client(client_id, fate.clone());
            }
        }
    }

    /// Decide whether to tell a client that a batch is `Missing` — and record having done
    /// so, because the deciding factor is how often we have said it already.
    ///
    /// Both places that produce the answer go through here: the seal that cannot be
    /// completed, and the fate request synthesised for a batch with no stored fate. The
    /// second one is not a lesser case — the replay short-circuit queues a fate request for
    /// every replayed row, so a peer retransmitting rows regenerates the answer without
    /// ever re-sending the seal. Bounding one emitter and not the other bounds nothing.
    ///
    /// Refusing to answer retracts nothing. The submission stays in storage, no fate is
    /// invented — a `Rejected` would destroy the peer's row and the graft tool is
    /// offline-only — and the budget is connection-scoped, so a reconnect asks again. What
    /// the peer loses is the instruction to retransmit, which is exactly the thing that was
    /// costing both sides everything.
    fn may_tell_client_a_batch_is_missing(
        &mut self,
        client_id: ClientId,
        batch_id: crate::row_histories::BatchId,
    ) -> bool {
        let now = self.clock.reserve_timestamp();
        let tracked = self.missing_answers.entry(client_id).or_default();
        if !tracked.budgets.contains_key(&batch_id)
            && tracked.budgets.len() >= MAX_TRACKED_MISSING_ANSWERS
        {
            // Make room only out of a batch that is no longer being repaired: one this
            // authority has given up on, or one nobody has asked about in a give-up
            // window. Evicting a live budget instead would hand the peer back the free
            // first answer for a batch it is already being throttled on — the same hole as
            // re-arming on a changed declaration, moved into the id space — and would drop
            // the bookkeeping of a batch that was genuinely mid-repair.
            //
            // Oldest-created first, which is O(1) and cannot stall. Staying non-dormant
            // costs the head an answer per window, and every answer counts against the
            // cap, so a peer cannot both keep it fresh and keep it under the cap: it goes
            // silent or it goes dormant. The worst case is not one window, though — a peer
            // asking about the head just inside every window spends one answer each time,
            // so silencing takes `MAX_MISSING_ANSWERS` of them and the head can block
            // eviction for that many give-up windows.
            let evicted = loop {
                let Some(candidate) = tracked.order.front().copied() else {
                    break None;
                };
                let Some(budget) = tracked.budgets.get(&candidate) else {
                    tracked.order.pop_front();
                    continue;
                };
                let dormant = now.saturating_sub(budget.last_answered_at)
                    > MISSING_ANSWER_GIVE_UP_AFTER_MICROS;
                if !budget.silenced && !dormant {
                    break None;
                }
                tracked.order.pop_front();
                break Some(candidate);
            };
            match evicted {
                Some(evicted) => {
                    tracked.budgets.remove(&evicted);
                }
                None => {
                    tracing::warn!(
                        target: "jazz::sync",
                        %client_id, ?batch_id,
                        tracked = tracked.budgets.len(),
                        "not answering: this client is tracking the maximum number of \
                         batches and every one of them is still live"
                    );
                    return false;
                }
            }
        }
        if !tracked.budgets.contains_key(&batch_id) {
            tracked.order.push_back(batch_id);
        }
        let budget = tracked
            .budgets
            .entry(batch_id)
            .or_insert_with(|| MissingAnswerBudget {
                answers: 0,
                first_answered_at: now,
                last_answered_at: 0,
                silenced: false,
            });
        if budget.silenced {
            return false;
        }
        let answers = budget.answers;
        if answers >= MAX_MISSING_ANSWERS
            && now.saturating_sub(budget.first_answered_at) > MISSING_ANSWER_GIVE_UP_AFTER_MICROS
        {
            budget.silenced = true;
            tracing::warn!(
                target: "jazz::sync",
                %client_id, ?batch_id, answers,
                "stopping the Missing answer for a batch that never completed; the \
                 submission is kept and a reconnect will ask again"
            );
            return false;
        }
        // The rate limit is checked after the give-up policy so a throttled attempt cannot
        // hold the budget open, and before the count so it cannot spend it either.
        if budget.answers > 0
            && now.saturating_sub(budget.last_answered_at) < MISSING_ANSWER_MIN_INTERVAL_MICROS
        {
            return false;
        }
        budget.answers += 1;
        budget.last_answered_at = now;
        true
    }

    pub(super) fn try_accept_completed_sealed_batch_from_client<H: Storage>(
        &mut self,
        storage: &mut H,
        client_id: ClientId,
        batch_id: crate::row_histories::BatchId,
    ) {
        let submission = match storage.load_sealed_batch_submission(batch_id) {
            Ok(Some(submission)) => submission,
            Ok(None) => return,
            Err(error) => {
                tracing::warn!(?batch_id, %error, "failed to load sealed batch submission");
                return;
            }
        };
        match storage.load_authoritative_batch_fate(batch_id) {
            Ok(Some(fate)) => {
                if self.can_promote_direct_fate(&fate) {
                    // Continue into seal validation so this authority can promote a
                    // previously local direct fate to its own durability tier.
                } else {
                    let should_prune_submission = self.batch_fate_is_settled(&fate);
                    let prune_result = if should_prune_submission {
                        storage.delete_sealed_batch_submission(batch_id)
                    } else {
                        Ok(())
                    };
                    if let Err(error) = prune_result {
                        tracing::warn!(
                            ?batch_id,
                            %error,
                            "failed to delete sealed batch submission"
                        );
                    }
                    self.queue_batch_fate_to_client_unfiltered(client_id, fate);
                    return;
                }
            }
            Ok(None) => {}
            Err(error) => {
                tracing::warn!(?batch_id, %error, "failed to load authoritative batch fate");
                return;
            }
        }

        let batch_rows = self.transactional_batch_rows(
            storage,
            batch_id,
            &submission
                .members
                .iter()
                .map(|member| member.object_id)
                .collect::<Vec<_>>(),
        );
        if let Err(rejection) = self.validate_sealed_batch_submission(&submission) {
            self.reject_sealed_transactional_batch(
                storage,
                Some(client_id),
                rejection,
                &batch_rows,
            );
            return;
        }
        if let Err(rejection) = self.validate_batch_rows_target_branch(&submission, &batch_rows) {
            self.reject_sealed_transactional_batch(
                storage,
                Some(client_id),
                rejection,
                &batch_rows,
            );
            return;
        }
        let Some(declared_rows) = Self::declared_rows_for_submission(&submission, &batch_rows)
        else {
            // The seal arrived but its declared rows never did — they died with
            // an earlier connection. Staying silent here left the sealer
            // retrying forever while this authority re-derived the void on
            // every attempt (production incident 2026-08-09). `Missing` is the
            // existing answer whose client-side handler retransmits rows +
            // seal, closing the two-phase loop. Not persisted: the fate-request
            // path already synthesizes Missing for unknown batches, and a
            // stored Missing would wrongly outlive the arrival of the rows.
            //
            // Bounded, because that same handler is what makes the answer worth
            // repeating and what makes repeating it dangerous: see
            // `may_tell_client_a_batch_is_missing`.
            if self.may_tell_client_a_batch_is_missing(client_id, batch_id) {
                self.queue_batch_fate_to_client_unfiltered(
                    client_id,
                    BatchFate::Missing { batch_id },
                );
            }
            return;
        };
        let mode = match self.infer_sealed_batch_mode(&submission, &batch_rows) {
            Ok(Some(mode)) => mode,
            Ok(None) => return,
            Err(rejection) => {
                self.reject_sealed_transactional_batch(
                    storage,
                    Some(client_id),
                    rejection,
                    &batch_rows,
                );
                return;
            }
        };
        if mode == SealedBatchMode::Transactional
            && let Err(rejection) =
                self.validate_transactional_parent_frontiers(storage, &submission, &declared_rows)
        {
            self.reject_sealed_transactional_batch(
                storage,
                Some(client_id),
                rejection,
                &batch_rows,
            );
            return;
        }

        self.settle_sealed_batch(
            storage,
            Some(client_id),
            submission,
            batch_rows,
            declared_rows,
            mode,
        );
    }

    pub(crate) fn recover_completed_sealed_batches_with_storage<H: Storage>(
        &mut self,
        storage: &mut H,
    ) -> bool {
        if self.my_tiers.is_empty() {
            return false;
        }

        let submissions = match storage.scan_sealed_batch_submissions() {
            Ok(submissions) => submissions,
            Err(error) => {
                tracing::warn!(%error, "failed to scan sealed batch submissions for recovery");
                return false;
            }
        };

        let mut recovered_any = false;
        for submission in submissions {
            match storage.load_authoritative_batch_fate(submission.batch_id) {
                Ok(Some(fate)) if self.can_promote_direct_fate(&fate) => {
                    // Continue into validation so this authority can promote a
                    // direct fate that was previously confirmed by a lower tier.
                }
                Ok(Some(_)) => continue,
                Ok(None) => {}
                Err(error) => {
                    tracing::warn!(
                        batch_id = ?submission.batch_id,
                        %error,
                        "failed to load authoritative batch fate during sealed batch recovery"
                    );
                    continue;
                }
            }

            let batch_rows = self.transactional_batch_rows(
                storage,
                submission.batch_id,
                &submission
                    .members
                    .iter()
                    .map(|member| member.object_id)
                    .collect::<Vec<_>>(),
            );
            if let Err(rejection) = self.validate_sealed_batch_submission(&submission) {
                self.reject_sealed_transactional_batch(storage, None, rejection, &batch_rows);
                recovered_any = true;
                continue;
            }
            if let Err(rejection) = self.validate_batch_rows_target_branch(&submission, &batch_rows)
            {
                self.reject_sealed_transactional_batch(storage, None, rejection, &batch_rows);
                recovered_any = true;
                continue;
            }
            let Some(declared_rows) = Self::declared_rows_for_submission(&submission, &batch_rows)
            else {
                continue;
            };
            let mode = match self.infer_sealed_batch_mode(&submission, &batch_rows) {
                Ok(Some(mode)) => mode,
                Ok(None) => continue,
                Err(rejection) => {
                    self.reject_sealed_transactional_batch(storage, None, rejection, &batch_rows);
                    recovered_any = true;
                    continue;
                }
            };
            if mode == SealedBatchMode::Transactional
                && let Err(rejection) = self.validate_transactional_parent_frontiers(
                    storage,
                    &submission,
                    &declared_rows,
                )
            {
                self.reject_sealed_transactional_batch(storage, None, rejection, &batch_rows);
                recovered_any = true;
                continue;
            }

            self.settle_sealed_batch(storage, None, submission, batch_rows, declared_rows, mode);
            recovered_any = true;
        }

        recovered_any
    }

    /// Process a single inbox entry.
    pub(super) fn process_inbox_entry<H: Storage>(&mut self, storage: &mut H, entry: InboxEntry) {
        tracing::trace!(source = ?entry.source, payload = entry.payload.variant_name(), "processing inbox entry");
        match entry.source {
            Source::Server(server_id) => {
                self.process_from_server(storage, server_id, entry.payload)
            }
            Source::Client(client_id) => {
                self.process_from_client(storage, client_id, entry.payload)
            }
        }
    }

    /// Process a payload from a server.
    pub(super) fn process_from_server<H: Storage>(
        &mut self,
        storage: &mut H,
        server_id: ServerId,
        payload: SyncPayload,
    ) {
        let _span = tracing::debug_span!("process_from_server", %server_id, payload = payload.variant_name()).entered();
        match payload {
            // A confirmation only ever travels client -> server. Receiving one here means
            // the peer is confused about its role; ignoring it is the safe reading.
            SyncPayload::DeliveryConfirmed { .. } => {
                tracing::warn!(%server_id, "ignoring a delivery confirmation from a server");
            }
            SyncPayload::CatalogueEntryUpdated { entry } => {
                tracing::debug!(
                    object_id = %entry.object_id,
                    object_type = ?entry.object_type(),
                    "server→CatalogueEntryUpdated"
                );
                if self.persist_catalogue_entry(storage, entry.clone()) {
                    self.pending_catalogue_updates.push(entry.clone());
                    self.forward_catalogue_entry_to_clients(entry, None);
                }
            }
            SyncPayload::RowBatchCreated { metadata, row }
            | SyncPayload::RowBatchNeeded { metadata, row } => {
                let object_id = row.row_id;
                let branch_name = BranchName::new(&row.branch);
                tracing::debug!(
                    %object_id,
                    %branch_name,
                    "server→row-batch payload"
                );
                if let Some(applied) = self.apply_row_updated(
                    storage,
                    metadata,
                    row.clone(),
                    AuthoritativeFateRecording::AcceptedByLocalAuthority,
                    ApplySource::UpstreamServer,
                ) {
                    // Applied — and only now may the sender record it as delivered. A
                    // duplicate re-offer lands here too: the apply is an idempotent no-op
                    // that still reports success, which is what lets a redelivery loop
                    // converge instead of repeating forever.
                    self.note_applied_row(
                        applied.row.row_id,
                        applied.row.branch.as_str(),
                        applied.row.batch_id,
                    );
                    self.apply_authoritative_transaction_fate_for_row(
                        storage,
                        server_id,
                        &applied.row,
                    );

                    if let Some(update) = applied.visibility_change {
                        self.pending_row_visibility_changes.push(update);
                        self.forward_update_to_clients_with_storage(
                            storage,
                            object_id,
                            branch_name,
                        );
                    }
                }
            }
            SyncPayload::BatchFate { fate } => {
                let (fate, changed) = match self.persist_authoritative_batch_fate(storage, &fate) {
                    Ok(result) => result,
                    Err(_) => return,
                };
                if changed {
                    self.pending_batch_fates.push(fate.clone());
                    if let BatchFate::AcceptedTransaction { batch_id, .. } = fate {
                        let rows = self.known_transactional_batch_rows_for_fate(storage, batch_id);
                        self.apply_transactional_batch_fate_to_rows(
                            storage,
                            None,
                            Some(server_id),
                            &fate,
                            &rows,
                        );
                    }
                }
                // Keep forwarding downstream: another connected client may not know it.
                let interested = self.interested_clients_for_batch_fate(&fate);
                for cid in interested {
                    if let Some(fate) = self.batch_fate_for_client(cid, &fate) {
                        self.outbox.push(OutboxEntry {
                            destination: Destination::Client(cid),
                            payload: SyncPayload::BatchFate { fate },
                        });
                    }
                }
            }
            SyncPayload::BatchFateNeeded { batch_ids } => {
                self.respond_to_batch_fate_request(
                    storage,
                    Destination::Server(server_id),
                    batch_ids,
                );
            }
            SyncPayload::QuerySettled {
                query_id,
                tier,
                scope,
                through_seq,
            } => {
                let scope_set: HashSet<(ObjectId, BranchName)> = scope.iter().copied().collect();
                let scope_changed = self
                    .remote_query_scopes
                    .get(&(server_id, query_id))
                    .is_none_or(|previous_scope| previous_scope != &scope_set);
                let tier_changed = self
                    .remote_query_scope_tiers
                    .get(&(server_id, query_id))
                    .is_none_or(|previous_tier| *previous_tier != tier);
                self.remote_query_scopes
                    .insert((server_id, query_id), scope_set);
                self.remote_query_scope_tiers
                    .insert((server_id, query_id), tier);
                if scope_changed || tier_changed {
                    self.remote_query_scope_dirty.insert(query_id);
                }

                tracing::debug!(?query_id, "server→QuerySettled");
                // Queue for local QueryManager to process
                self.pending_query_settled.push(PendingQuerySettled {
                    server_id: Some(server_id),
                    query_id,
                    tier,
                    through_seq,
                });

                // RuntimeCore relays this to interested clients once the
                // upstream stream watermark proves the scope's rows are local.
            }
            SyncPayload::SchemaWarning(warning) => {
                super::log_schema_warning(&warning, Some("server"), None);

                if let Some(clients) = self.query_origin.get(&warning.query_id) {
                    for &cid in clients {
                        self.outbox.push(OutboxEntry {
                            destination: Destination::Client(cid),
                            payload: SyncPayload::SchemaWarning(warning.clone()),
                        });
                    }
                }
            }
            SyncPayload::ConnectionSchemaDiagnostics(diagnostics) => {
                super::log_connection_schema_diagnostics(&diagnostics, Some("server"));
            }
            SyncPayload::Error(err) => match err {
                SyncError::QuerySubscriptionRejected {
                    query_id,
                    code,
                    reason,
                } => {
                    tracing::warn!(
                        ?server_id,
                        query_id = query_id.0,
                        code = %code,
                        error = %reason,
                        "server rejected query subscription"
                    );
                    self.pending_query_rejections.push(PendingQueryRejection {
                        query_id,
                        code: code.clone(),
                        reason: reason.clone(),
                    });
                }
                _ => {
                    tracing::warn!(?server_id, error = ?err, "error from server");
                }
            },
            // Servers shouldn't send these to us
            SyncPayload::QuerySubscription { .. }
            | SyncPayload::QueryUnsubscription { .. }
            | SyncPayload::SealBatch { .. } => {}
        }
    }

    /// Process a payload from a client.
    pub(super) fn process_from_client<H: Storage>(
        &mut self,
        storage: &mut H,
        client_id: ClientId,
        payload: SyncPayload,
    ) {
        let _span = tracing::debug_span!("process_from_client", %client_id, payload = payload.variant_name()).entered();
        let Some(client) = self.clients.get(&client_id) else {
            tracing::warn!(%client_id, "message from unknown client, ignoring");
            return;
        };
        tracing::trace!(%client_id, role = ?client.role, payload = payload.variant_name(), "client→payload");

        match &payload {
            // The receiver says these rows are applied. This is the only trustworthy
            // delivery signal: every sender-side one reports success for payloads that a
            // dying socket still accepts and never transmits.
            SyncPayload::DeliveryConfirmed { rows } => {
                let confirmed: Vec<_> = rows
                    .iter()
                    .map(|row| {
                        (
                            client_id,
                            row.row_id,
                            BranchName::new(row.branch.as_str()),
                            row.batch_id,
                        )
                    })
                    .collect();
                tracing::debug!(
                    target: "jazz::delivery",
                    %client_id,
                    rows = confirmed.len(),
                    "receiver confirmed rows"
                );
                self.confirm_client_deliveries(&confirmed);
            }
            SyncPayload::CatalogueEntryUpdated { entry } => {
                let object_id = entry.object_id;
                let branch_name = BranchName::new("main");
                match client.role {
                    ClientRole::Peer | ClientRole::Backend => {
                        self.outbox.push(OutboxEntry {
                            destination: Destination::Client(client_id),
                            payload: SyncPayload::Error(SyncError::CatalogueWriteDenied {
                                object_id,
                                branch_name,
                            }),
                        });
                    }
                    ClientRole::Admin => {
                        self.apply_payload_from_client(
                            storage,
                            client_id,
                            payload,
                            AuthoritativeFateRecording::Skip,
                            ApplySource::ClientFrame,
                        );
                    }
                    ClientRole::User => {
                        let Some(_session) = &client.session else {
                            self.outbox.push(OutboxEntry {
                                destination: Destination::Client(client_id),
                                payload: SyncPayload::Error(SyncError::SessionRequired {
                                    object_id,
                                    branch_name,
                                }),
                            });
                            return;
                        };
                        if self.allow_unprivileged_schema_catalogue_writes
                            && entry.is_structural_schema_catalogue()
                        {
                            self.apply_payload_from_client(
                                storage,
                                client_id,
                                payload,
                                AuthoritativeFateRecording::Skip,
                                ApplySource::ClientFrame,
                            );
                            return;
                        }
                        self.outbox.push(OutboxEntry {
                            destination: Destination::Client(client_id),
                            payload: SyncPayload::Error(SyncError::CatalogueWriteDenied {
                                object_id,
                                branch_name,
                            }),
                        });
                    }
                }
            }
            SyncPayload::RowBatchCreated { metadata, row }
            | SyncPayload::RowBatchNeeded { metadata, row } => {
                let object_id = row.row_id;
                let branch_name = BranchName::new(&row.branch);
                match client.role {
                    ClientRole::Peer => {
                        if payload.is_catalogue() {
                            self.outbox.push(OutboxEntry {
                                destination: Destination::Client(client_id),
                                payload: SyncPayload::Error(SyncError::CatalogueWriteDenied {
                                    object_id,
                                    branch_name,
                                }),
                            });
                            return;
                        }
                        self.apply_payload_from_client(
                            storage,
                            client_id,
                            payload,
                            AuthoritativeFateRecording::Skip,
                            ApplySource::ClientFrame,
                        );
                    }
                    ClientRole::Admin => {
                        self.apply_payload_from_client(
                            storage,
                            client_id,
                            payload,
                            AuthoritativeFateRecording::Skip,
                            ApplySource::ClientFrame,
                        );
                    }
                    ClientRole::Backend => {
                        if payload.is_catalogue() {
                            self.outbox.push(OutboxEntry {
                                destination: Destination::Client(client_id),
                                payload: SyncPayload::Error(SyncError::CatalogueWriteDenied {
                                    object_id,
                                    branch_name,
                                }),
                            });
                            return;
                        }
                        self.apply_payload_from_client(
                            storage,
                            client_id,
                            payload,
                            AuthoritativeFateRecording::Skip,
                            ApplySource::ClientFrame,
                        );
                    }
                    ClientRole::User => {
                        let Some(session) = &client.session else {
                            self.outbox.push(OutboxEntry {
                                destination: Destination::Client(client_id),
                                payload: SyncPayload::Error(SyncError::SessionRequired {
                                    object_id,
                                    branch_name,
                                }),
                            });
                            return;
                        };
                        if payload.is_catalogue() {
                            if self.allow_unprivileged_schema_catalogue_writes
                                && payload.is_structural_schema_catalogue()
                            {
                                self.apply_payload_from_client(
                                    storage,
                                    client_id,
                                    payload,
                                    AuthoritativeFateRecording::Skip,
                                    ApplySource::ClientFrame,
                                );
                                return;
                            }
                            self.outbox.push(OutboxEntry {
                                destination: Destination::Client(client_id),
                                payload: SyncPayload::Error(SyncError::CatalogueWriteDenied {
                                    object_id,
                                    branch_name,
                                }),
                            });
                            return;
                        }

                        // Resolve only what the replay decision needs: the
                        // table, and the stored row for this exact batch.
                        // Everything heavier waits until we know this is not a
                        // replay — a peer that reconnects, or that was answered
                        // `Missing`, resends rows we already hold byte for byte,
                        // and preparing a policy check for those is work for a
                        // check that never runs. Production 2026-08-10: a core
                        // pinned with an empty log, absorbing a peer's replays.
                        let (stored_metadata, existing_history_row) = self
                            .row_metadata_from_payload(storage, row, metadata.as_ref())
                            .and_then(|stored_metadata| {
                                let table =
                                    stored_metadata.get(MetadataKey::Table.as_str())?.clone();
                                let existing_history_row = storage
                                    .load_history_row_batch(
                                        &table,
                                        &row.branch,
                                        row.row_id,
                                        row.batch_id,
                                    )
                                    .ok()
                                    .flatten();
                                Some((stored_metadata, existing_history_row))
                            })
                            .unwrap_or_else(|| (HashMap::new(), None));

                        // Idempotent replay short-circuit: reconnect replays row
                        // history, so only an exact stored row-batch match counts as
                        // a true no-op. Same-batch corrections must still flow
                        // through permission evaluation.
                        if let Some(existing_history_row) = existing_history_row.as_ref()
                            && Self::matches_replayed_row_batch(existing_history_row, row)
                        {
                            self.row_batch_interest
                                .entry(RowBatchKey::from_row(row))
                                .or_default()
                                .insert(client_id);
                            self.try_accept_completed_sealed_batch_from_client(
                                storage,
                                client_id,
                                row.batch_id,
                            );
                            self.pending_client_batch_fates
                                .entry(client_id)
                                .or_default()
                                .insert(row.batch_id);
                            return;
                        }

                        // Not a replay: now pay for the policy check's inputs.
                        let payload_metadata = metadata
                            .as_ref()
                            .map(|meta| meta.metadata.clone())
                            .unwrap_or_default();
                        let pre_batch_visible_row = stored_metadata
                            .get(MetadataKey::Table.as_str())
                            .and_then(|table| self.pre_batch_visible_row(storage, table, row));

                        let old_content = pre_batch_visible_row
                            .as_ref()
                            .map(|previous| previous.data.clone());
                        let metadata = if old_content.is_none() && stored_metadata.is_empty() {
                            payload_metadata
                        } else {
                            stored_metadata
                        };
                        let new_content = (!row.is_deleted).then_some(row.data.clone());
                        let operation = if row.is_deleted {
                            Operation::Delete
                        } else if old_content.is_some() || !row.parents.is_empty() {
                            Operation::Update
                        } else {
                            Operation::Insert
                        };
                        self.queue_for_permission_check(
                            client_id,
                            payload,
                            session.clone(),
                            metadata,
                            old_content.map(|content| content.to_vec()),
                            new_content.map(|content| content.to_vec()),
                            operation,
                        );
                    }
                }
            }
            SyncPayload::SealBatch { .. } => {
                self.apply_payload_from_client(
                    storage,
                    client_id,
                    payload,
                    AuthoritativeFateRecording::Skip,
                    ApplySource::ClientFrame,
                );
            }
            // Handle query subscription with full Query struct
            // Queue for QueryManager to process (SyncManager doesn't know about QueryGraph)
            SyncPayload::QuerySubscription {
                query_id,
                query,
                session,
                required_tier,
                propagation,
                policy_context_tables,
            } => {
                // Build effective session: identity (user_id) comes from the
                // server-established session (set during the SSE auth handshake) and
                // cannot be overridden by the payload. However, ephemeral per-subscription
                // claims supplied in the payload — such as a join_code for invite flows —
                // are merged in when the user_id matches, so that policy conditions like
                // `claims.join_code` evaluate correctly for this subscription.
                let effective_session = match (&client.session, session) {
                    (Some(client_session), Some(payload_session)) => {
                        if client_session.user_id != payload_session.user_id {
                            tracing::warn!(
                                %client_id,
                                "QuerySubscription payload session user_id does not match client session; ignoring payload session"
                            );
                            Some(client_session.clone())
                        } else {
                            // Same user: merge claims. Payload provides ephemeral claims
                            // (e.g. join_code); client session claims take precedence so
                            // auth-established values cannot be spoofed.
                            let merged_claims = if let (
                                serde_json::Value::Object(client_map),
                                serde_json::Value::Object(payload_map),
                            ) =
                                (&client_session.claims, &payload_session.claims)
                            {
                                let mut merged = payload_map.clone();
                                merged.extend(client_map.clone());
                                serde_json::Value::Object(merged)
                            } else {
                                client_session.claims.clone()
                            };
                            Some(Session {
                                user_id: client_session.user_id.clone(),
                                claims: merged_claims,
                                auth_mode: client_session.auth_mode,
                            })
                        }
                    }
                    (Some(client_session), None) => Some(client_session.clone()),
                    (None, payload_session) => payload_session.clone(),
                };
                // Track origin for QuerySettled relay
                self.query_origin
                    .entry(*query_id)
                    .or_default()
                    .insert(client_id);
                tracing::trace!(
                    %client_id,
                    query_id = query_id.0,
                    table = %query.table,
                    ?propagation,
                    "jazz trace received query subscription from client"
                );
                self.pending_query_subscriptions
                    .push(PendingQuerySubscription {
                        client_id,
                        query_id: *query_id,
                        query: query.as_ref().clone(),
                        session: effective_session,
                        required_tier: *required_tier,
                        propagation: *propagation,
                        policy_context_tables: policy_context_tables.clone(),
                    });
            }
            // Handle query unsubscription
            // Queue for QueryManager to process (remove server-side QueryGraph, forward upstream)
            SyncPayload::QueryUnsubscription { query_id } => {
                tracing::trace!(
                    %client_id,
                    query_id = query_id.0,
                    "jazz trace received query unsubscription from client"
                );
                // Clean up query origin
                if let Some(clients) = self.query_origin.get_mut(query_id) {
                    clients.remove(&client_id);
                    if clients.is_empty() {
                        self.query_origin.remove(query_id);
                    }
                }
                self.pending_query_unsubscriptions
                    .push(PendingQueryUnsubscription {
                        client_id,
                        query_id: *query_id,
                    });
            }
            SyncPayload::BatchFate { fate } => {
                if self.retain_client_batch_fate(fate) {
                    self.pending_batch_fates.push(fate.clone());
                }
            }
            SyncPayload::BatchFateNeeded { batch_ids } => {
                self.register_client_batch_fate_interest(client_id, batch_ids);
                self.respond_to_batch_fate_request(
                    storage,
                    Destination::Client(client_id),
                    batch_ids.clone(),
                );
            }
            SyncPayload::QuerySettled {
                query_id,
                tier,
                scope: _,
                through_seq,
            } => {
                // Client relaying a QuerySettled from downstream
                self.pending_query_settled.push(PendingQuerySettled {
                    server_id: None,
                    query_id: *query_id,
                    tier: *tier,
                    through_seq: *through_seq,
                });
            }
            SyncPayload::SchemaWarning(warning) => {
                tracing::warn!(
                    %client_id,
                    query_id = warning.query_id.0,
                    "client attempted to send SchemaWarning payload; ignoring"
                );
            }
            SyncPayload::ConnectionSchemaDiagnostics(_) => {
                tracing::warn!(
                    %client_id,
                    "client attempted to send ConnectionSchemaDiagnostics payload; ignoring"
                );
            }
            // Clients shouldn't send these
            SyncPayload::Error(_) => {}
        }
    }

    /// Apply a payload from a client (either directly or after approval).
    pub(super) fn apply_payload_from_client<H: Storage>(
        &mut self,
        storage: &mut H,
        client_id: ClientId,
        payload: SyncPayload,
        fate_recording: AuthoritativeFateRecording,
        source: ApplySource,
    ) {
        match payload {
            SyncPayload::CatalogueEntryUpdated { entry } => {
                if self.persist_catalogue_entry(storage, entry.clone()) {
                    self.pending_catalogue_updates.push(entry.clone());
                    self.forward_catalogue_entry_to_servers(entry.clone());
                    self.forward_catalogue_entry_to_clients(entry, Some(client_id));
                }
            }
            SyncPayload::RowBatchCreated { metadata, row }
            | SyncPayload::RowBatchNeeded { metadata, row } => {
                let object_id = row.row_id;
                let branch_name = BranchName::new(&row.branch);
                let batch_id = row.batch_id;
                self.row_batch_interest
                    .entry(RowBatchKey::new(object_id, branch_name, batch_id))
                    .or_default()
                    .insert(client_id);

                if let Some(applied) =
                    self.apply_row_updated(storage, metadata, row.clone(), fate_recording, source)
                {
                    self.forward_row_batch_to_servers(
                        storage,
                        object_id,
                        applied.metadata.clone(),
                        row,
                    );
                    if !matches!(
                        applied.row.state,
                        RowState::StagingPending | RowState::Superseded
                    ) && let Some(update) = applied.visibility_change
                    {
                        self.pending_row_visibility_changes.push(update);
                        self.forward_update_to_clients_except_with_storage(
                            storage,
                            object_id,
                            branch_name,
                            client_id,
                        );
                    }
                }
                self.try_accept_completed_sealed_batch_from_client(storage, client_id, batch_id);
            }
            SyncPayload::SealBatch { submission } => {
                if submission.members.is_empty() {
                    tracing::warn!(batch_id = ?submission.batch_id, "ignoring SealBatch with no declared members");
                    return;
                }
                match storage.load_authoritative_batch_fate(submission.batch_id) {
                    Ok(Some(fate @ BatchFate::Missing { .. })) => {
                        if self.may_tell_client_a_batch_is_missing(client_id, submission.batch_id) {
                            self.queue_batch_fate_to_client(client_id, fate);
                        }
                        return;
                    }
                    Ok(Some(fate @ BatchFate::Rejected { .. }))
                    | Ok(Some(fate @ BatchFate::AcceptedTransaction { .. })) => {
                        self.queue_batch_fate_to_client(client_id, fate);
                        return;
                    }
                    Ok(Some(BatchFate::DurableDirect { .. })) => {}
                    Ok(None) => {}
                    Err(error) => {
                        tracing::warn!(
                            batch_id = ?submission.batch_id,
                            %error,
                            "failed to load authoritative batch fate"
                        );
                        return;
                    }
                }
                if let Err(rejection) = self.validate_sealed_batch_submission(&submission) {
                    let batch_rows = self.transactional_batch_rows(
                        storage,
                        submission.batch_id,
                        &submission
                            .members
                            .iter()
                            .map(|member| member.object_id)
                            .collect::<Vec<_>>(),
                    );
                    self.reject_sealed_transactional_batch(
                        storage,
                        Some(client_id),
                        rejection,
                        &batch_rows,
                    );
                    return;
                }
                if let Err(error) = self.persist_sealed_batch_submission(storage, &submission) {
                    tracing::warn!(
                        batch_id = ?submission.batch_id,
                        %error,
                        "failed to persist sealed batch submission"
                    );
                    return;
                }
                self.seal_batch_to_servers(submission.clone());
                self.try_accept_completed_sealed_batch_from_client(
                    storage,
                    client_id,
                    submission.batch_id,
                );
            }
            SyncPayload::BatchFate { fate } => {
                if self.retain_client_batch_fate(&fate) {
                    self.pending_batch_fates.push(fate.clone());
                }
            }
            SyncPayload::BatchFateNeeded { batch_ids } => {
                self.register_client_batch_fate_interest(client_id, &batch_ids);
                self.respond_to_batch_fate_request(
                    storage,
                    Destination::Client(client_id),
                    batch_ids,
                );
            }
            _ => {}
        }
    }
}
