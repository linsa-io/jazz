//! A delivered snapshot that lost its parents in transit is not a row creation.
//!
//! `scope_delivery_row` clears `parents` on every visible row before it goes out to a client
//! (`sync_manager/sync_logic.rs`). The receiver's frontier rule is "a visible row is a non-tip
//! iff some visible row names it as a parent" (`resolution.rs`), and a parentless row names
//! nobody — so nothing it should supersede is ever superseded. 460 deliveries produce 460
//! tips. Measured in a real client store: row `1234c757` of table `users` holds 460 history
//! batches and a 7,622-byte visible entry, which is 460 ids of 16 bytes plus the row itself.
//!
//! `parents == []` is overloaded. It means both "this is the row's creation" and "I withheld
//! the ancestry", and the frontier rule reads the second as the first. But the two are
//! distinguishable without any help from the sender: `RowProvenance::for_insert` sets
//! `created_at == updated_at` (`metadata.rs:135-143`), `for_update` copies `created_at`
//! verbatim and takes a fresh `updated_at` (`metadata.rs:145-152`), and the clock guarantees
//! every reservation is strictly greater than the last (`sync_manager/clock.rs:14-28`).
//! Therefore:
//!
//! > A visible row with `parents.is_empty() && created_at != updated_at` provably is not a row
//! > creation. It is a batch whose ancestry was elided in transit.
//!
//! These gates pin what the frontier must do with that knowledge. They need no wire change and
//! no sender cooperation, which is what makes the rule work against senders already in the
//! field — including the ones that produced the 460-tip row.
//!
//! The failure direction is deliberately safe: a false negative (two writes inside one clock
//! tick, so `created_at == updated_at` on an update) degrades to exactly today's behaviour.

use std::collections::HashMap;

use super::resolution::{branch_frontier, build_computed_visible_preview};
use super::*;
use crate::metadata::{DeleteKind, RowProvenance};
use crate::object::ObjectId;
use crate::query_manager::types::{ColumnDescriptor, ColumnType, RowDescriptor, Value};
use crate::row_format::encode_row;
use crate::sync_manager::DurabilityTier;

const CREATED_AT: u64 = 1_000;

fn descriptor() -> RowDescriptor {
    RowDescriptor::new(vec![ColumnDescriptor::new("label", ColumnType::Text)])
}

fn body(label: &str) -> Vec<u8> {
    encode_row(&descriptor(), &[Value::Text(label.into())]).expect("encode row")
}

/// A row exactly as a client stores it after delivery: real batch id, real content, ancestry
/// stripped by the sender.
fn elided_snapshot(row_id: ObjectId, updated_at: u64, label: &str) -> StoredRowBatch {
    let mut row = StoredRowBatch::new(
        row_id,
        "main",
        Vec::new(),
        body(label),
        RowProvenance::for_update(
            &RowProvenance::for_insert("alice".to_string(), CREATED_AT),
            "alice".to_string(),
            updated_at,
        ),
        HashMap::new(),
        RowState::VisibleDirect,
        Some(DurabilityTier::GlobalServer),
    );
    row.parents.clear();
    row
}

/// A genuine creation: parentless because it really is the first batch of the row.
fn creation(row_id: ObjectId, at: u64, label: &str) -> StoredRowBatch {
    StoredRowBatch::new(
        row_id,
        "main",
        Vec::new(),
        body(label),
        RowProvenance::for_insert("alice".to_string(), at),
        HashMap::new(),
        RowState::VisibleDirect,
        Some(DurabilityTier::GlobalServer),
    )
}

fn soft_deleted(row_id: ObjectId, updated_at: u64) -> StoredRowBatch {
    let mut row = elided_snapshot(row_id, updated_at, "gone");
    row.delete_kind = Some(DeleteKind::Soft);
    row.is_deleted = true;
    row
}

#[test]
fn a_row_of_elided_snapshots_collapses_to_one_tip() {
    let row_id = ObjectId::new();
    let rows: Vec<_> = (1..=460)
        .map(|step| elided_snapshot(row_id, CREATED_AT + step, &format!("v{step}")))
        .collect();
    let newest = rows.last().expect("fixture has rows").batch_id();

    let tips = branch_frontier(&rows);

    assert_eq!(
        tips,
        vec![newest],
        "460 snapshots of one row are 460 states of the same lineage, not 460 concurrent \
         branches. Every one of them has `created_at != updated_at`, so not one of them is a \
         row creation, so they cannot all be tips. Leaving them as tips is what makes \
         `parents_cover_frontier_exactly` unsatisfiable for the life of the row — a \
         single-parent arrival can never match a 460-wide frontier — and it is what a real \
         client store measured at 460 tips on one `users` row. Got {} tips",
        tips.len()
    );
}

#[test]
fn a_stale_soft_delete_stops_winning() {
    let row_id = ObjectId::new();
    let rows = vec![
        elided_snapshot(row_id, CREATED_AT + 1, "live"),
        soft_deleted(row_id, CREATED_AT + 2),
        elided_snapshot(row_id, CREATED_AT + 3, "live again"),
    ];

    let preview = build_computed_visible_preview(&descriptor(), &rows, None)
        .expect("preview should build")
        .expect("three visible rows produce a preview");

    assert!(
        !preview.row.is_deleted,
        "the delete is the middle state of one lineage and the newest state is not deleted, so \
         the row is live. `delete_winner` ranks only rows that carry a delete_kind and then \
         hands the whole preview its `is_deleted`, so while every snapshot stays a tip ANY \
         delete anywhere in the frontier deletes the row permanently, no matter how many newer \
         live states arrive. That is a soft delete no restore can undo — silent data loss, not \
         a cost defect."
    );
}

#[test]
fn a_creation_root_is_dominated_by_a_later_snapshot() {
    let row_id = ObjectId::new();
    let rows = vec![
        creation(row_id, CREATED_AT, "born"),
        elided_snapshot(row_id, CREATED_AT + 5, "grown"),
    ];
    let newest = rows[1].batch_id();

    assert_eq!(
        branch_frontier(&rows),
        vec![newest],
        "the snapshot is a later state OF this row, so the creation it descends from must stop \
         being a tip. A rule that only lets elided snapshots dominate each other strands the \
         creation root as a permanent second tip, which keeps the frontier at two forever and \
         keeps the fast path shut."
    );
}

#[test]
fn two_genuine_creations_still_merge() {
    let row_id = ObjectId::new();
    let rows = vec![
        creation(row_id, CREATED_AT, "one"),
        creation(row_id, CREATED_AT + 1, "two"),
    ];

    assert_eq!(
        branch_frontier(&rows).len(),
        2,
        "two real creations of one row id ARE concurrent and must still merge as two tips. The \
         discriminator is `created_at != updated_at`, not `parents.is_empty()`; a rule that \
         collapses every parentless row would silently drop one of two genuine inserts."
    );
}

#[test]
fn a_parented_history_is_left_alone() {
    let row_id = ObjectId::new();
    let root = creation(row_id, CREATED_AT, "root");
    let mut child = elided_snapshot(row_id, CREATED_AT + 1, "child");
    child.parents = smallvec::smallvec![root.batch_id()];
    let child_id = child.batch_id();

    assert_eq!(
        branch_frontier(&[root, child]),
        vec![child_id],
        "a row that kept its ancestry is resolved by the ancestry, exactly as before. This gate \
         exists so the new rule cannot be written in a way that also rewrites the healthy case."
    );
}

/// Lineage, not recency. This is the case that makes the `created_at` conjunct load-bearing.
///
/// `branch_frontier` is not a display choice: `query_manager::writes::load_branch_tip_ids`
/// feeds it straight into the parent set of the next local write. Dropping a row from it is
/// therefore an assertion to the authority that the surviving batch subsumes the dropped one —
/// and a receiver holding only stripped snapshots has no evidence for that ACROSS lineages.
/// Device clocks are unrelated, so a creation authored on a fast-running clock can carry a
/// larger `updated_at` than a snapshot of an entirely different lineage. Superseding it there
/// would make the next local write name only the wrong parent, and the AUTHORITY — not the
/// device — ends up permanently forked.
#[test]
fn a_snapshot_never_supersedes_another_lineage() {
    let row_id = ObjectId::new();
    // A creation from a device whose clock runs ahead: newest by `updated_at`, and a genuine
    // creation of its own lineage.
    let skewed = creation(row_id, CREATED_AT + 10_000, "from a fast clock");
    // A snapshot of a different lineage entirely: same row id, different `created_at`.
    let mut other_lineage = elided_snapshot(row_id, CREATED_AT + 1, "elsewhere");
    other_lineage.created_at = CREATED_AT + 5_000;

    let tips = branch_frontier(&[skewed, other_lineage]);

    assert_eq!(
        tips.len(),
        2,
        "these two rows share no lineage — their `created_at` differs, and `for_update` copies \
         that field verbatim down a chain, so differing values mean neither descends from the \
         other. Collapsing them would have the receiver tell the authority that one subsumes \
         the other on no evidence at all, and ordinary clock skew between two devices is enough \
         to reach it."
    );
}

/// The tiebreak is not decoration: two replicas resolving the same set must pick the same
/// winner, and `updated_at` alone does not decide it.
#[test]
fn a_tie_on_updated_at_is_broken_by_batch_id() {
    let row_id = ObjectId::new();
    let a = elided_snapshot(row_id, CREATED_AT + 7, "a");
    let b = elided_snapshot(row_id, CREATED_AT + 7, "b");
    let expected = a.batch_id().max(b.batch_id());

    assert_eq!(
        branch_frontier(&[a, b]),
        vec![expected],
        "two snapshots stamped in the same clock tick must still resolve to one tip, and to the \
         SAME one on every replica. A rule ordering by `updated_at` alone leaves the choice to \
         iteration order, and two replicas would then disagree about the frontier while \
         agreeing about every value."
    );
}

/// A non-visible batch must neither arm the rule nor win it.
///
/// `Rejected` is the dangerous one: a rejected row is a parent-stripped delivered copy stored
/// with a non-visible state, so it looks exactly like a snapshot on every field the rule reads.
/// While the rule only ADDED to `non_tips` this was harmless; a rule that REMOVES a tip can
/// delete a live row in favour of a rejected one.
#[test]
fn a_rejected_batch_neither_arms_the_rule_nor_wins_it() {
    let row_id = ObjectId::new();
    let mut rejected = elided_snapshot(row_id, CREATED_AT + 9_000, "rejected");
    rejected.state = RowState::Rejected;
    let live_one = creation(row_id, CREATED_AT, "live one");
    let live_two = creation(row_id, CREATED_AT + 1, "live two");

    let tips = branch_frontier(&[rejected, live_one, live_two]);

    assert_eq!(
        tips.len(),
        2,
        "the only row here that looks like a snapshot is rejected, so nothing arms the rule and \
         both live creations stay tips. If a rejected batch can arm it, it also wins it — it is \
         the newest — and a live tip is deleted in favour of a batch the authority refused."
    );
}

/// The tier-filtered arm needs the rule for the same reason the unfiltered one does: a
/// parentless row has no ancestors, so causal domination cannot reach it either.
#[test]
fn the_tier_filtered_frontier_collapses_snapshots_too() {
    let row_id = ObjectId::new();
    let rows: Vec<_> = (1..=6)
        .map(|step| elided_snapshot(row_id, CREATED_AT + step, &format!("v{step}")))
        .collect();
    let newest = rows.last().expect("fixture has rows").batch_id();

    let preview =
        build_computed_visible_preview(&descriptor(), &rows, Some(DurabilityTier::GlobalServer))
            .expect("preview should build")
            .expect("visible rows produce a preview");

    assert_eq!(
        preview.row.batch_id(),
        newest,
        "a tier read of a row whose snapshots all satisfy the tier must resolve to the newest of \
         them, not merge six states of one lineage as if they were concurrent."
    );
}
