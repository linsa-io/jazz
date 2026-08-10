//! A peer re-sealing a row it RECEIVED must be able to settle.
//!
//! `declared_rows_for_submission` matches a seal's declared members by
//! `content_digest()`, and that digest covers `parents`
//! (`row_histories/types.rs`). Delivery strips them: `scope_delivery_row`
//! clears `parents` on every visible row a server sends
//! (`sync_manager/sync_logic.rs`). So a peer's copy of a delivered row hashes
//! differently from the authority's own, through no fault of either — the
//! sender mutated a field the identity is built on.
//!
//! This fork already reached that conclusion once, in the graft tool:
//! `storage/graft.rs` refuses to judge by content digest because "the digest
//! covers parents, and a client's copy of a delivered batch has its parents
//! STRIPPED by the sender — normal, not divergence."
//!
//! Unfixed, the seal can never complete, and the answer to a seal that cannot
//! complete is `BatchFate::Missing`, which drives the peer to retransmit the
//! rows and seal again — a cycle with no exit. Production 2026-08-10: a core
//! pinned with an empty log, the runtime spending its time absorbing the
//! peer's own replays.
//!
//! The gate: a seal whose declared digests come from the DELIVERED (stripped)
//! form settles, and is not answered `Missing`.

use super::*;

#[test]
fn a_seal_declaring_delivered_rows_settles_instead_of_asking_forever() {
    let mut io = MemoryStorage::new();
    seed_users_schema(&mut io);
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::EdgeServer);
    let client_id = ClientId::new();
    add_client(&mut sm, &io, client_id);
    sm.set_client_role(client_id, ClientRole::Peer);

    // A root the row can descend from, so the child applies cleanly.
    let row_id = ObjectId::new();
    let root = visible_row(row_id, "main", Vec::new(), 1_000, b"root");
    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: row_id,
                metadata: row_metadata("users"),
            }),
            row: root.clone(),
        },
    );

    // The row as the AUTHORITY holds it: with its parent.
    let batch_id = BatchId(*ObjectId::new().uuid().as_bytes());
    let held = row_with_batch_state(
        visible_row(row_id, "main", vec![root.batch_id], 1_100, b"update"),
        batch_id,
        crate::row_histories::RowState::VisibleDirect,
        None,
    );
    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: row_id,
                metadata: row_metadata("users"),
            }),
            row: held.clone(),
        },
    );
    sm.take_outbox();

    // The row as a PEER holds it after delivery — same batch, parents gone.
    let delivered = SyncManager::scope_delivery_row(held.clone());
    assert!(
        delivered.parents.is_empty() && !held.parents.is_empty(),
        "delivery must be the thing that strips the parents, else this gates nothing"
    );
    assert_ne!(
        delivered.content_digest(),
        held.content_digest(),
        "the digests must differ, else there is no mismatch to fix"
    );

    // The peer seals what it holds.
    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::SealBatch {
            submission: sealed_submission(
                batch_id,
                "main",
                vec![SealedBatchMember {
                    object_id: row_id,
                    row_digest: delivered.content_digest(),
                }],
                Vec::new(),
            ),
        },
    );

    let fates: Vec<_> = sm
        .take_outbox()
        .into_iter()
        .filter_map(|entry| match entry {
            OutboxEntry {
                destination: Destination::Client(id),
                payload: SyncPayload::BatchFate { fate },
            } if id == client_id => Some(fate),
            _ => None,
        })
        .collect();

    eprintln!("fates answered to the sealing peer: {fates:?}");
    assert!(
        !fates
            .iter()
            .any(|fate| matches!(fate, BatchFate::Missing { .. })),
        "the authority answered Missing for a seal whose rows it HOLDS — the peer will \
         retransmit them and be told Missing again, forever; the declared identity must \
         not depend on a field delivery strips"
    );
    assert!(
        fates.iter().any(|fate| matches!(
            fate,
            BatchFate::DurableDirect { .. } | BatchFate::AcceptedTransaction { .. }
        )),
        "the seal must settle; got {fates:?}"
    );

    // Settling is not the contract — the contract is that the cycle STOPS. The
    // submission must be retired, and a peer that replays the same seal (which
    // is exactly what the cycle did) must not be asked again.
    assert!(
        io.load_sealed_batch_submission(batch_id)
            .expect("submission lookup")
            .is_none(),
        "a settled submission must be retired, or every later replay re-derives it"
    );

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::SealBatch {
            submission: sealed_submission(
                batch_id,
                "main",
                vec![SealedBatchMember {
                    object_id: row_id,
                    row_digest: delivered.content_digest(),
                }],
                Vec::new(),
            ),
        },
    );
    let replay_fates: Vec<_> = sm
        .take_outbox()
        .into_iter()
        .filter_map(|entry| match entry {
            OutboxEntry {
                destination: Destination::Client(id),
                payload: SyncPayload::BatchFate { fate },
            } if id == client_id => Some(fate),
            _ => None,
        })
        .collect();
    eprintln!("fates on the replayed seal: {replay_fates:?}");
    assert!(
        !replay_fates
            .iter()
            .any(|fate| matches!(fate, BatchFate::Missing { .. })),
        "replaying the same seal was answered Missing again — that answer is what drives \
         the peer to retransmit, so the cycle would resume"
    );
}
