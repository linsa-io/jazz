//! A batch member is identified by its payload, not by who its parents were.
//!
//! `LocalBatchMember::row_digest` is minted with `content_digest()`, which covers `parents`,
//! and checked with the same call — so the two agree today only because the two copies of a
//! row that ever meet here have the same parents.
//!
//! That coincidence is about to end. Delivery currently strips parents from every visible
//! row it puts on the wire (`sync_logic::scope_delivery_row`), so a receiver's copy is
//! parentless and its `content_digest()` happens to equal the authority's
//! `content_digest_ignoring_parents()` — which is the arm that rescues a seal at
//! `declared_rows_for_submission`. Stamping real parents on delivery breaks the coincidence
//! at both arms, and an uncompletable seal is answered `Missing`, which asks the peer to
//! resend and seal again: the cycle with no exit, production 2026-08-10, one core pinned and
//! nothing in the log.
//!
//! So the mint goes parent-blind — and the checks must accept BOTH forms, because index
//! entries minted under the old rule are already on disk in every installed store. Getting
//! that half wrong breaks the upgrade with no network involved: the member stops matching,
//! the row drops out of the batch, and the same cycle starts locally.
//!
//! Mint narrow, check wide. This gate pins both directions.

use super::*;
use crate::batch_fate::LocalBatchMember;
use crate::storage::Storage as _;

fn member_for(
    row: &crate::row_histories::StoredRowBatch,
    digest: crate::digest::Digest32,
) -> LocalBatchMember {
    LocalBatchMember {
        object_id: row.row_id,
        table_name: "users".to_string(),
        branch_name: BranchName::new(row.branch.as_str()),
        schema_hash: crate::query_manager::types::SchemaHash::compute(&users_test_schema()),
        row_digest: digest,
    }
}

/// A row with parents, indexed under either digest rule, must still be found.
#[test]
fn a_batch_member_matches_whichever_digest_rule_minted_it() {
    let sm = SyncManager::new().with_durability_tier(DurabilityTier::Local);
    let mut io = MemoryStorage::new();
    seed_users_schema(&mut io);

    let row_id = ObjectId::new();
    let parent = BatchId::new();
    let batch_id = BatchId::new();
    // Parents are what the two rules disagree about, so the row must have some.
    let row = row_with_batch_state(
        visible_row(row_id, "main", vec![parent], 1_000, b"alice"),
        batch_id,
        crate::row_histories::RowState::VisibleDirect,
        None,
    );
    io.append_history_region_rows("users", std::slice::from_ref(&row))
        .expect("seed the row");

    for (rule, digest) in [
        (
            "parent-blind (the new mint)",
            row.content_digest_ignoring_parents(),
        ),
        ("parents included (already on disk)", row.content_digest()),
    ] {
        io.upsert_local_batch_row_index(batch_id, &[member_for(&row, digest)])
            .expect("index the member");

        let found = sm.transactional_batch_rows(&io, batch_id, &[row_id]);
        assert!(
            !found.is_empty(),
            "a member indexed with its digest {rule} was not matched back to its row. The \
             check must accept both forms: the mint is going parent-blind, and every store \
             already carries entries minted the other way. A member that stops matching \
             drops its row from the batch, `declared_rows_for_submission` returns None, and \
             the seal becomes uncompletable — answered `Missing`, resent, sealed again, with \
             no exit."
        );
    }
}
