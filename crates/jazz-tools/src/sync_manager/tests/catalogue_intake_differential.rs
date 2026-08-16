//! Randomized differential test: catalogue intake vs a model of its intended
//! semantics.
//!
//! `SyncManager::persist_catalogue_entry` used to answer ONE question — "did
//! storage change?" — and gate TWO unrelated decisions on it: forwarding the
//! entry to peers, and handing it to the schema layer via
//! `pending_catalogue_updates`. That conflation is the whole outage: after a
//! restart the store already held a generation's catalogue entry byte for byte,
//! a peer re-sent it, "storage unchanged" came back, and the schema layer was
//! never told — so the generation stayed unknown, its branch never entered the
//! query universe, and every row under it was unreadable at every durability
//! tier (rpc-server, measured 2026-08-16: users 2 of 22, chat_members 4 of 11).
//!
//! The intake now answers both questions separately. That makes it a small state
//! machine with two independent maps, driven by an op stream whose order is the
//! entire risk: a re-offer after a publish behaves differently from a publish
//! after a re-offer, and a reconnect replays storage into a third map that must
//! influence neither. Hand-written cases encode the sequences we already thought
//! of; this compares against a model on randomized streams.
//!
//! The model, in full:
//!
//! * `storage_holds[id]` — the bytes storage has. An intake writes storage iff
//!   the incoming entry differs from it. THIS gates forwarding onward.
//! * `schema_layer_saw[id]` — the entry the schema layer has been handed. A
//!   peer offer is handed over iff it differs from this. THIS gates
//!   `pending_catalogue_updates`, and NOTHING else may write this map — in
//!   particular not the connect replay, which is exactly the seeding that would
//!   restore the bug.
//! * A local publish (`upsert_catalogue_entry`) never marks: the schema layer is
//!   where that entry came from. The peer echo it provokes is therefore handed
//!   over once, which is why the downstream handlers must be idempotent (see
//!   `catalogue_same_permissions_head_push_is_noop`).
//!
//! No proptest/quickcheck in this crate, so the stream comes from the same
//! hand-rolled xorshift over a fixed seed list the storage differential uses;
//! every assertion carries the seed and op index so a failure replays exactly.
//!
//! Generator limitations (conscious):
//! - Content is opaque here. Intake does not interpret it, and what the SCHEMA
//!   layer does with a handed entry is gated separately.
//! - One node under test with up to two peers; no server-to-server topologies.
//! - No storage restart mid-stream. The restart is what created the outage, but
//!   it is the boot path (`rehydrate_schema_manager_from_catalogue`) that
//!   handles it, gated in cold_boot_generation_universe.rs and in jazz-napi.

use std::collections::HashMap;

use crate::catalogue::CatalogueEntry;
use crate::metadata::{MetadataKey, ObjectType};
use crate::object::ObjectId;
use crate::storage::{MemoryStorage, Storage};
use crate::sync_manager::{ClientId, ClientRole, InboxEntry, Source, SyncManager, SyncPayload};

const OPS_PER_SEED: usize = 300;
const SEEDS: [u64; 6] = [
    0x0CA7_0106_0000_0001,
    0x0CA7_0106_0000_0002,
    0x0CA7_0106_0000_0003,
    0xD1FF_0CA7_0000_0004,
    0xD1FF_0CA7_0000_0005,
    0xD1FF_0CA7_0000_0006,
];

struct Xorshift(u64);

impl Xorshift {
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.next_u64() % bound as u64) as usize
    }
}

/// A small pool of object ids, so the stream keeps revisiting the same ids with
/// the same and with different bodies — re-offers and version bumps are the
/// interesting shapes, and both need collisions to occur.
fn pooled_object_id(index: usize) -> ObjectId {
    let mut bytes = [0u8; 16];
    bytes[0] = 0xCA;
    bytes[1] = index as u8;
    ObjectId::from_uuid(uuid::Uuid::from_bytes(bytes))
}

/// Entries vary by (id, body) so the same id can carry different bytes, and the
/// same bytes can arrive under different ids. Metadata varies too — it is part
/// of `CatalogueEntry` equality, so it must be part of the difference the intake
/// keys on.
fn pooled_entry(id_index: usize, body: usize) -> CatalogueEntry {
    let mut metadata = HashMap::new();
    metadata.insert(
        MetadataKey::Type.to_string(),
        ObjectType::CatalogueSchema.to_string(),
    );
    metadata.insert(MetadataKey::SchemaHash.to_string(), format!("hash-{body}"));
    CatalogueEntry {
        object_id: pooled_object_id(id_index),
        metadata,
        content: format!("content-{id_index}-{body}").into_bytes(),
    }
}

#[derive(Debug)]
enum Op {
    /// A peer offers an entry. The one case the outage lived in.
    PeerOffer { entry: CatalogueEntry, from: usize },
    /// This node publishes locally — the schema layer already knows it.
    LocalPublish { entry: CatalogueEntry },
    /// A peer connects and the node replays its catalogue from storage. Must
    /// influence neither map; seeding `handed_to_schema_layer` here is exactly
    /// the bug.
    Reconnect,
}

#[test]
fn catalogue_intake_differential_random_ops() {
    for seed in SEEDS {
        run_seed(seed);
    }
}

fn run_seed(seed: u64) {
    const ID_POOL: usize = 4;
    const BODY_POOL: usize = 3;

    let mut rng = Xorshift(seed);
    let mut io = MemoryStorage::new();
    let mut sm = SyncManager::new();

    let mut peers = vec![ClientId::new()];
    sm.add_client_with_storage(&io, peers[0]);
    sm.set_client_role(peers[0], ClientRole::Admin);
    sm.take_outbox();

    // The model.
    let mut storage_holds: HashMap<ObjectId, CatalogueEntry> = HashMap::new();
    let mut schema_layer_saw: HashMap<ObjectId, CatalogueEntry> = HashMap::new();
    /// Which side last wrote an object's bytes. A local publish legitimately
    /// advances storage without telling the schema layer — the schema layer is
    /// where those bytes came from — so the end-state invariant below can only
    /// be stated over objects a PEER last wrote.
    #[derive(PartialEq)]
    enum LastWriter {
        Peer,
        Local,
    }
    let mut last_writer: HashMap<ObjectId, LastWriter> = HashMap::new();

    for op_index in 0..OPS_PER_SEED {
        let op = match rng.below(10) {
            0 => Op::Reconnect,
            1..=3 => Op::LocalPublish {
                entry: pooled_entry(rng.below(ID_POOL), rng.below(BODY_POOL)),
            },
            _ => Op::PeerOffer {
                entry: pooled_entry(rng.below(ID_POOL), rng.below(BODY_POOL)),
                from: rng.below(peers.len()),
            },
        };

        // What the model says must happen.
        let expected_handoff = match &op {
            Op::PeerOffer { entry, .. } => {
                let hand_over = schema_layer_saw.get(&entry.object_id) != Some(entry);
                if hand_over {
                    schema_layer_saw.insert(entry.object_id, entry.clone());
                }
                if storage_holds.get(&entry.object_id) != Some(entry) {
                    storage_holds.insert(entry.object_id, entry.clone());
                    last_writer.insert(entry.object_id, LastWriter::Peer);
                }
                hand_over.then(|| entry.clone())
            }
            Op::LocalPublish { entry } => {
                if storage_holds.get(&entry.object_id) != Some(entry) {
                    storage_holds.insert(entry.object_id, entry.clone());
                    last_writer.insert(entry.object_id, LastWriter::Local);
                }
                None
            }
            Op::Reconnect => None,
        };

        // Drive the real thing.
        match &op {
            Op::PeerOffer { entry, from } => {
                sm.push_inbox(InboxEntry {
                    source: Source::Client(peers[*from]),
                    payload: SyncPayload::CatalogueEntryUpdated {
                        entry: entry.clone(),
                    },
                });
                sm.process_inbox(&mut io);
            }
            Op::LocalPublish { entry } => {
                sm.upsert_catalogue_entry(&mut io, entry.clone());
            }
            Op::Reconnect => {
                let client_id = ClientId::new();
                sm.add_client_with_storage(&io, client_id);
                sm.set_client_role(client_id, ClientRole::Admin);
                peers.push(client_id);
            }
        }
        sm.take_outbox();

        let handed = sm.take_pending_catalogue_updates();
        match &expected_handoff {
            Some(entry) => assert_eq!(
                handed.as_slice(),
                std::slice::from_ref(entry),
                "seed {seed:#x} op {op_index} ({op:?}): the schema layer must be handed exactly \
                 this entry — it has not seen these bytes for this object id. Handing it over is \
                 what a byte-identical re-send after a restart used to skip, leaving a schema \
                 generation on disk and unknown to the running process."
            ),
            None => assert!(
                handed.is_empty(),
                "seed {seed:#x} op {op_index} ({op:?}): nothing should reach the schema layer — \
                 it has already been handed these exact bytes. Every redundant hand-off of a \
                 permissions head rebuilds authorization and marks every subscription for \
                 recompilation. Got {handed:?}"
            ),
        }

        // And storage must agree with the model at every step, so a stream can
        // never drift into agreeing about hand-offs while disagreeing about
        // bytes.
        for (object_id, entry) in &storage_holds {
            let stored = io
                .load_catalogue_entry(*object_id)
                .expect("loading a catalogue entry should succeed");
            assert_eq!(
                stored.as_ref(),
                Some(entry),
                "seed {seed:#x} op {op_index}: storage must hold the bytes the model recorded \
                 for {object_id}"
            );
        }
    }

    // The property the whole fix exists for, restated over the end state:
    // wherever a PEER last wrote an object's bytes, the schema layer has been
    // handed those exact bytes. A store holding a generation the schema layer
    // never heard of IS the outage.
    //
    // Restricted to peer-written objects on purpose: a local publish advances
    // storage without marking, because the schema layer is where those bytes
    // came from. An earlier draft asserted this over every object and failed —
    // correctly — on `peer offers E1, then this node publishes E2`.
    //
    // Note the case this invariant covers rather than hides: a peer re-offering
    // a STALE entry (say an older permissions head) rewrites storage backwards
    // and is NOT handed over, because the schema layer already saw those bytes.
    // Storage and the schema layer's view then agree, so the invariant holds —
    // but the store has regressed to an older head, and only the in-memory
    // version guard (`process_catalogue_permissions_head`) keeps this process
    // on the newer one; a restart would rehydrate the older. That storage
    // regression predates this work — `persist_catalogue_entry` has always
    // written whenever the bytes differ, in either direction.
    for (object_id, entry) in &storage_holds {
        if last_writer.get(object_id) != Some(&LastWriter::Peer) {
            continue;
        }
        assert_eq!(
            schema_layer_saw.get(object_id),
            Some(entry),
            "seed {seed:#x}: a peer wrote {object_id} into storage, so the schema layer must \
             have been handed those exact bytes"
        );
    }
}
