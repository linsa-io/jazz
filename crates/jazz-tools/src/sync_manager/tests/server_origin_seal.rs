//! A row that arrives by replication must complete the seal it belongs to.
//!
//! Seal completion is wired asymmetrically. Every path that applies a row from a CLIENT
//! re-checks whether that row was the last one its sealed batch was waiting for —
//! `process_from_client`'s row arms and the parked-batch heal both call
//! `try_accept_completed_sealed_batch_from_client`. The SERVER arms do not:
//! `process_from_server`'s `RowBatchCreated`/`RowBatchNeeded` arm applies the row, records
//! the fate for an already-accepted transaction, and forwards visibility — and stops there.
//! The server arm of `finish_parked_row_apply` does the same. A `SealBatch` from a server is
//! an inert match arm.
//!
//! So for a submission this node retains whose declared rows arrive from an upstream, the
//! only thing that ever completes it is the recovery sweep at the top of every
//! `immediate_tick` — a full scan of the retained submission table, measured at 11.06 ms per
//! tick and 70% of this server's CPU under a user simply typing into a chat draft. The sweep
//! is paying, every tick, for an event nobody wired up.
//!
//! That makes this gate a precondition for any change to the sweep, and a defect on its own:
//! a `SyncManager` driven without a `RuntimeCore` — which is how a great deal of this suite
//! drives it — has no sweep at all, and this path has no coverage anywhere in the tree.
//!
//! The gate asserts the two things completion means: the batch acquires an authoritative
//! fate, and its submission is retired. Both fail today.

use super::*;
use crate::batch_fate::{BatchMode, SealedBatchMember, SealedBatchSubmission};
use crate::object::BranchName;
use crate::storage::Storage as _;

#[test]
#[ignore = "open defect: only the client-origin arms complete a seal. Fixing it means \
            extracting an origin-agnostic drive_sealed_batch from \
            try_accept_completed_sealed_batch_from_client and calling it with None from \
            process_from_server's row arm and finish_parked_row_apply's server arm."]
fn a_row_replicated_from_a_server_completes_the_seal_it_belongs_to() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Local);
    let mut io = MemoryStorage::new();
    let server_id = ServerId::new();
    let row_id = ObjectId::new();
    let batch_id = BatchId::new();

    seed_users_schema(&mut io);
    add_server(&mut sm, &io, server_id);
    sm.take_outbox();

    let row = row_with_batch_state(
        visible_row(row_id, "main", Vec::new(), 1_000, b"alice"),
        batch_id,
        crate::row_histories::RowState::VisibleDirect,
        None,
    );

    // The seal is already on disk with its declared member, and the row is not here yet:
    // the shape a restart leaves behind, and the shape replication lag produces live.
    io.upsert_sealed_batch_submission(&SealedBatchSubmission::new(
        batch_id,
        BatchMode::Direct,
        BranchName::new("main"),
        vec![SealedBatchMember {
            object_id: row_id,
            row_digest: row.content_digest(),
        }],
        Vec::new(),
    ))
    .unwrap();

    // The last row the seal was waiting for, delivered by an upstream server rather than
    // by the client that sealed it.
    sm.process_from_server(
        &mut io,
        server_id,
        SyncPayload::RowBatchNeeded {
            metadata: Some(RowMetadata {
                id: row_id,
                metadata: row_metadata("users"),
            }),
            row,
        },
    );

    let fate = io.load_authoritative_batch_fate(batch_id).unwrap();
    assert!(
        fate.is_some(),
        "the row this seal was waiting for has arrived, so the batch must acquire an \
         authoritative fate. Only the client-origin arms re-check the seal; the \
         server-origin arm applies the row and stops, leaving the per-tick recovery sweep \
         as the only thing that ever finishes the job. Got {fate:?}"
    );
    assert_eq!(
        io.load_sealed_batch_submission(batch_id).unwrap(),
        None,
        "a completed seal must retire its submission. Left behind, it is re-read by every \
         subsequent tick of the process — which is exactly the cost this whole change is \
         about."
    );
}
