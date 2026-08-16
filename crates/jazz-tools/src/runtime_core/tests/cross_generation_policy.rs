//! Defect-27 SECURITY gate: a `whereOld` (USING) UPDATE policy must be checked
//! against the row's CURRENT content, not against the copy left behind in the
//! schema generation the row was born in.
//!
//! A row whose family spans two `rowtable:*:<table>:<schema-hash>` generations
//! used to keep a visible head in BOTH, and every visible read served the
//! stale one. Two policy surfaces read exactly those two things:
//!
//! - `query_manager/server_queries.rs` (`evaluate_update_permission`) fills an
//!   empty `old_content` from `storage.load_visible_region_row(...)`, which
//!   goes through the locator ladder in
//!   `storage::load_visible_region_row_bytes_with_storage`;
//! - the same function and `sync_manager/inbox.rs` both take
//!   `old_content_schema_hash` from `__row_locator.origin_schema_hash`, the
//!   pointer the inbound sync path never rewrote — so old-content bytes were
//!   lensed through the wrong generation's descriptor.
//!
//! Both surfaces read the same two pieces of state, so this file pins both
//! pieces directly — exactly one visible head for the `(row, branch)`, and
//! `__row_locator.origin_schema_hash` naming the generation the write landed
//! in — and then pins the CONSEQUENCE each surface has, end to end through the
//! real `QueryManager::evaluate_update_permission`:
//!
//! - a session that has LOST ownership must not keep writing (the stale
//!   visible read reaching the USING arm as the row's old content);
//! - the session that HOLDS ownership must still write, including on the write
//!   whose old content `evaluate_update_permission` fills in itself — the
//!   `load_visible_region_row` call at the top of that function.
//!
//! Nothing here uses the test-only `take_pending_permission_checks` /
//! `approve_permission_check` shortcut: every verdict below is the server's
//! own, reached by pumping a `ClientRole::User` device's writes at it.

use super::*;

use crate::batch_fate::BatchFate;
use crate::metadata::{MetadataKey, RowProvenance};
use crate::row_format::decode_row;
use crate::row_histories::{RowState, StoredRowBatch};
use crate::storage::SqliteStorage;
use crate::sync_manager::RowMetadata;

const TABLE: &str = "docs";

/// A backend that actually models the `rowtable:*:<table>:<schema-hash>` raw
/// table families. `MemoryStorage` keeps visible rows as structs and OVERRIDES
/// `load_visible_region_row` / `load_visible_region_entry`
/// (`storage/memory.rs:899`, `:929`), so it never runs the locator ladder in
/// `load_visible_region_row_bytes_with_storage` — the code under test. A gate
/// built on `MemoryStorage` would be vacuous. Every real deployment (sqlite on
/// the client, rocksdb on the server) runs the ladder.
fn split_capable_storage() -> SqliteStorage {
    SqliteStorage::open(":memory:").expect("in-memory sqlite storage should open")
}

fn docs_table() -> crate::query_manager::types::TableSchemaBuilder {
    TableSchema::builder(TABLE)
        .column("owner", ColumnType::Text)
        .column("body", ColumnType::Text)
}

/// Generation A of the app's structural schema.
fn docs_gen_a() -> Schema {
    SchemaBuilder::new().table(docs_table()).build()
}

/// Generation B: the same `docs`, in a schema that hashes differently — which
/// is all it takes to mint a second `rowtable:*:docs:<hash>` family.
///
/// `docs` itself is byte-identical across the two generations ON PURPOSE. The
/// defect is about WHICH family a visible read serves, so the only difference
/// the reader can surface must be the row's CONTENT — `owner = alice` in the
/// generation the row was born in, `owner = bob` in the one the writes land
/// in. A shape difference would instead make the stale read fail to decode,
/// and the gate would pass for a reason that is not the policy verdict. (It
/// would also make every write in the fixture cross a lens, and the branch's
/// own descriptor — which `validate_json_for_content` decodes an incoming
/// INSERT with — would reject the generation-A seed outright.)
fn docs_gen_b() -> Schema {
    SchemaBuilder::new()
        .table(docs_table())
        .table(TableSchema::builder("attachments").column("name", ColumnType::Text))
        .build()
}

/// The permissions head: generation B's shape carrying the row policies.
///
/// It is a DIFFERENT schema value from the runtime/structural one, which is
/// what makes the explicit authorization path (and with it the USING arm)
/// reachable — see `the_explicit_auth_using_arm_is_reachable` in the parent
/// test module.
///
/// `whereOld` is ownership; `whereNew` is permissive, so the current owner may
/// hand the row to someone else. That is what makes "who owns it now" differ
/// from "who owned it in the previous generation".
fn docs_policy_schema() -> Schema {
    let owner = PolicyExpr::eq_session("owner", vec!["user_id".into()]);
    SchemaBuilder::new()
        .table(
            docs_table().policies(
                TablePolicies::new()
                    .with_select(PolicyExpr::True)
                    .with_insert(PolicyExpr::True)
                    .with_update(Some(owner), PolicyExpr::True),
            ),
        )
        .table(TableSchema::builder("attachments").column("name", ColumnType::Text))
        .build()
}

fn gen_a_hash() -> SchemaHash {
    SchemaHash::compute(&docs_gen_a())
}

fn gen_b_hash() -> SchemaHash {
    SchemaHash::compute(&docs_gen_b())
}

fn gen_a_metadata() -> HashMap<String, String> {
    HashMap::from([
        (MetadataKey::Table.to_string(), TABLE.to_string()),
        (
            MetadataKey::OriginSchemaHash.to_string(),
            gen_a_hash().to_string(),
        ),
    ])
}

/// The row as generation A wrote it: owned by `owner`, encoded under
/// generation A's descriptor.
fn gen_a_row(row_id: ObjectId, branch: &str, owner: &str, body: &str) -> StoredRowBatch {
    StoredRowBatch::new(
        row_id,
        branch,
        Vec::new(),
        encode_row(
            &docs_gen_a()[&TableName::new(TABLE)].columns,
            &[
                Value::Text(owner.to_string()),
                Value::Text(body.to_string()),
            ],
        )
        .expect("the generation-A row should encode"),
        RowProvenance::for_insert(owner.to_string(), 1_000),
        HashMap::new(),
        RowState::VisibleDirect,
        None,
    )
}

/// One-shot bidirectional pump; the shared `sync_server_with_clients` helper is
/// hardwired to `MemoryStorage`. Copied from
/// `runtime_core/tests/accepted_batch_downgrade.rs:59`.
fn pump<S: Storage, Sch: Scheduler>(
    server: &mut RuntimeCore<S, Sch>,
    server_id: ServerId,
    client: &mut RuntimeCore<S, Sch>,
    client_id: ClientId,
) {
    for _ in 0..12 {
        let mut any = false;
        client.batched_tick();
        for entry in client.sync_sender().take() {
            if entry.destination == Destination::Server(server_id) {
                any = true;
                server.park_sync_message(InboxEntry {
                    source: Source::Client(client_id),
                    payload: entry.payload,
                });
            }
        }
        server.batched_tick();
        server.immediate_tick();
        server.batched_tick();
        for entry in server.sync_sender().take() {
            if entry.destination == Destination::Client(client_id) {
                any = true;
                client.park_sync_message(InboxEntry {
                    source: Source::Server(server_id),
                    payload: entry.payload,
                });
            }
        }
        client.batched_tick();
        client.immediate_tick();
        if !any {
            break;
        }
    }
}

fn new_server<S: Storage>(storage: S, app_name: &str) -> RuntimeCore<S, NoopScheduler> {
    let schema_manager = SchemaManager::new(
        SyncManager::new().with_durability_tier(DurabilityTier::EdgeServer),
        docs_gen_b(),
        AppId::from_name(app_name),
        "dev",
        "main",
    )
    .unwrap();
    let mut server = new_test_core(schema_manager, storage, NoopScheduler);
    server.immediate_tick();
    teach_both_generations(&mut server);
    server
        .schema_manager_mut()
        .query_manager_mut()
        .set_authorization_schema(docs_policy_schema());
    server
        .schema_manager_mut()
        .query_manager_mut()
        .require_authorization_schema();
    server.immediate_tick();
    server
}

/// The client is a plain device: no permissions head, so its LOCAL write path
/// enforces nothing and every verdict in this file is the server's.
fn new_client<S: Storage>(storage: S, app_name: &str) -> RuntimeCore<S, NoopScheduler> {
    let schema_manager = SchemaManager::new(
        SyncManager::new(),
        docs_gen_b(),
        AppId::from_name(app_name),
        "dev",
        "main",
    )
    .unwrap();
    let mut client = new_test_core(schema_manager, storage, NoopScheduler);
    client.immediate_tick();
    teach_both_generations(&mut client);
    client.immediate_tick();
    client
}

/// Both generations live in the store, connected by the deployment's lens —
/// the state every runtime is in after a schema push.
fn teach_both_generations<S: Storage>(core: &mut RuntimeCore<S, NoopScheduler>) {
    crate::test_support::persist_test_schema(core.storage_mut(), &docs_gen_a());
    core.schema_manager_mut()
        .query_manager_mut()
        .add_live_schema(docs_gen_a());
    let lens = crate::schema_manager::auto_lens::generate_lens(&docs_gen_a(), &docs_gen_b());
    core.publish_lens(&lens).expect("the v1->v2 lens publishes");
    core.immediate_tick();
}

/// The raw-table name layout is `rowtable:<kind>:<table>:<schema-hash>`
/// (`storage::RowRawTableId::new`).
fn visible_family_name(schema_hash: SchemaHash) -> String {
    format!("rowtable:visible:{TABLE}:{schema_hash}")
}

/// Every `rowtable:visible:docs:<schema-hash>` family the store has registered.
fn registered_visible_families<S: Storage>(core: &RuntimeCore<S, NoopScheduler>) -> Vec<String> {
    core.storage()
        .scan_raw_table_headers()
        .expect("raw table header scan should succeed")
        .into_iter()
        .map(|(name, _)| name)
        .filter(|name| name.starts_with(&format!("rowtable:visible:{TABLE}:")))
        .collect()
}

/// The families that actually hold a visible head for this `(row, branch)`.
fn visible_raw_tables_holding<S: Storage>(
    core: &RuntimeCore<S, NoopScheduler>,
    branch: &str,
    row_id: ObjectId,
) -> Vec<String> {
    // `<branch>:<row-uuid-hex>` is the visible raw-table key layout
    // (`storage::key_codec::visible_row_raw_table_key`).
    let key = format!("{branch}:{}", row_id.uuid().simple());
    registered_visible_families(core)
        .into_iter()
        .filter(|raw_table| {
            core.storage()
                .raw_table_get(raw_table, &key)
                .expect("raw table probe should succeed")
                .is_some()
        })
        .collect()
}

fn rejection_reasons<S: Storage>(core: &mut RuntimeCore<S, NoopScheduler>) -> Vec<String> {
    core.sync_sender()
        .take()
        .into_iter()
        .filter_map(|entry| match entry.payload {
            SyncPayload::BatchFate {
                fate: BatchFate::Rejected { reason, .. },
            } => Some(reason),
            _ => None,
        })
        .collect()
}

fn column_of<S: Storage>(
    core: &RuntimeCore<S, NoopScheduler>,
    branch: &str,
    row_id: ObjectId,
    index: usize,
) -> Option<String> {
    let row = core
        .storage()
        .load_visible_region_row(TABLE, branch, row_id)
        .expect("visible read should succeed")?;
    let values = decode_row(&docs_gen_b()[&TableName::new(TABLE)].columns, &row.data).ok()?;
    match values.get(index) {
        Some(Value::Text(text)) => Some(text.clone()),
        _ => None,
    }
}

fn owner_of<S: Storage>(
    core: &RuntimeCore<S, NoopScheduler>,
    branch: &str,
    row_id: ObjectId,
) -> Option<String> {
    column_of(core, branch, row_id, 0)
}

fn body_of<S: Storage>(
    core: &RuntimeCore<S, NoopScheduler>,
    branch: &str,
    row_id: ObjectId,
) -> Option<String> {
    column_of(core, branch, row_id, 1)
}

/// Precondition on the FIXTURE, not the system: the two generations must
/// really be two generations, or "a cross-generation row is read as the newer
/// generation" is vacuously satisfied and this file stops discriminating the
/// moment someone edits the schema pair.
fn assert_the_generations_are_distinct() {
    assert_ne!(
        gen_a_hash(),
        gen_b_hash(),
        "the two schemas hash the same, so they share one raw-table family and \
         no generation split can exist"
    );
    assert_ne!(
        visible_family_name(gen_a_hash()),
        visible_family_name(gen_b_hash()),
        "both generations name the same visible raw table, so there is no split \
         for a read to guess between"
    );
    // ...and `docs` must be shape-identical across them, or the stale read
    // fails to DECODE rather than serving the wrong owner, and the gate would
    // pass for a reason that has nothing to do with the policy verdict.
    assert_eq!(
        docs_gen_a()[&TableName::new(TABLE)].columns,
        docs_gen_b()[&TableName::new(TABLE)].columns,
        "the two generations disagree about the shape of `docs`"
    );
    // The verdict must be able to come only from the permissions head: the
    // runtime schema the server and the device both run carries no policies at
    // all, and the head carries the USING arm.
    assert!(
        docs_gen_b()[&TableName::new(TABLE)]
            .policies
            .update_using_policy()
            .is_none(),
        "the structural runtime schema carries an update policy of its own, so a \
         verdict here would not prove the permissions head ran"
    );
    assert!(
        docs_policy_schema()[&TableName::new(TABLE)]
            .policies
            .update_using_policy()
            .is_some(),
        "the permissions head has no USING arm; there is no `whereOld` to gate"
    );
}

struct CrossGenerationWorld {
    server: RuntimeCore<SqliteStorage, NoopScheduler>,
    client: RuntimeCore<SqliteStorage, NoopScheduler>,
    server_id: ServerId,
    alice_client_id: ClientId,
    bob_client_id: ClientId,
    row_id: ObjectId,
    branch: String,
    /// The batch id of the most recent `attempt`.
    last_batch_id: BatchId,
}

/// The world the defect lives in: one row, seeded under generation A owned by
/// alice, then legitimately handed to bob by a generation-B write — the write
/// that puts the row's family across two generations.
fn cross_generation_world(app_name: &str) -> CrossGenerationWorld {
    assert_the_generations_are_distinct();

    let mut server = new_server(split_capable_storage(), app_name);
    let mut client = new_client(split_capable_storage(), app_name);

    let server_id = ServerId::new();
    let alice_client_id = ClientId::new();
    let bob_client_id = ClientId::new();
    server.add_client(alice_client_id, Some(Session::new("alice")));
    server.add_client(bob_client_id, Some(Session::new("bob")));
    client.add_server(server_id);

    let branch = server.schema_manager().branch_name().as_str().to_string();
    assert_eq!(
        branch,
        client.schema_manager().branch_name().as_str(),
        "server and client must share one branch, or the split under test is a \
         cross-BRANCH split and a different defect"
    );

    // Seed: the generation-A row, into both stores, as the frame a peer sends.
    // The server's copy goes through the real INSERT policy check.
    let row_id = ObjectId::new();
    let seed = gen_a_row(row_id, &branch, "alice", "draft");
    server.park_sync_message(InboxEntry {
        source: Source::Client(alice_client_id),
        payload: SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: row_id,
                metadata: gen_a_metadata(),
            }),
            row: seed.clone(),
        },
    });
    server.batched_tick();
    server.immediate_tick();
    client.park_sync_message(InboxEntry {
        source: Source::Server(server_id),
        payload: SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: row_id,
                metadata: gen_a_metadata(),
            }),
            row: seed,
        },
    });
    client.batched_tick();
    client.immediate_tick();
    let seed_rejections = rejection_reasons(&mut server);
    assert!(
        seed_rejections.is_empty(),
        "control: the generation-A seed was rejected ({seed_rejections:?}); the \
         world this file tests never gets built"
    );
    server.sync_sender().take();
    client.sync_sender().take();

    assert_eq!(
        owner_of(&server, &branch, row_id).as_deref(),
        Some("alice"),
        "control: the generation-A seed must be visible and owned by alice on \
         the server before anything crosses generations"
    );
    assert_eq!(
        owner_of(&client, &branch, row_id).as_deref(),
        Some("alice"),
        "control: the seed must be visible on the device too"
    );

    CrossGenerationWorld {
        server,
        client,
        server_id,
        alice_client_id,
        bob_client_id,
        row_id,
        branch,
        last_batch_id: BatchId::new(),
    }
}

/// The ownership transfer: alice, the current owner, hands the row to bob from
/// the upgraded runtime. Written under generation B, so the row's family now
/// spans both generations.
fn transfer_ownership_to_bob(world: &mut CrossGenerationWorld) {
    world
        .client
        .update(
            world.row_id,
            vec![("owner".to_string(), Value::Text("bob".to_string()))],
            Some(&WriteContext::from_session(Session::new("alice"))),
        )
        .expect("the owner's own update applies locally");
    pump(
        &mut world.server,
        world.server_id,
        &mut world.client,
        world.alice_client_id,
    );
    world.server.batched_tick();
    world.server.immediate_tick();

    let rejections = rejection_reasons(&mut world.server);
    assert!(
        rejections.is_empty(),
        "positive control: the current owner's ownership transfer was rejected \
         ({rejections:?}); the observation channel is broken and nothing below \
         would prove anything"
    );

    world.server.sync_sender().take();
    world.client.sync_sender().take();
}

/// The two surfaces' INPUTS, pinned directly on the server: one visible head,
/// and `__row_locator.origin_schema_hash` naming the generation the write
/// landed in. Asserted after each test's verdict, so the security consequence
/// is what a regression reports first and this says why.
fn assert_the_row_reads_as_the_generation_it_moved_to(world: &CrossGenerationWorld) {
    // Non-vacuity first: the store really did register BOTH generations'
    // visible families, so "exactly one head" below is a repair, not an
    // accident of the row never having crossed a generation at all.
    let families = registered_visible_families(&world.server);
    assert!(
        families.contains(&visible_family_name(gen_a_hash()))
            && families.contains(&visible_family_name(gen_b_hash())),
        "the row never crossed a generation on the server — visible families \
         present: {families:?}"
    );

    let heads = visible_raw_tables_holding(&world.server, &world.branch, world.row_id);
    assert_eq!(
        heads.len(),
        1,
        "one (row, branch) must have exactly one visible head; it has {}: \
         {heads:?} — two heads is the state every read then has to guess \
         between, and the guess is what the USING arm is handed",
        heads.len()
    );
    assert_eq!(
        world
            .server
            .storage()
            .load_row_locator(world.row_id)
            .expect("row locator readable")
            .and_then(|locator| locator.origin_schema_hash),
        Some(gen_b_hash()),
        "`__row_locator.origin_schema_hash` still names the generation the row \
         was BORN in; that is the field both `evaluate_update_permission` and \
         the inbound sync path stamp onto `old_content_schema_hash`"
    );
}

/// Alice's write, pumped at the server from her device, and the batch's fate.
fn attempt(world: &mut CrossGenerationWorld, session: &str, body: &str) -> Option<BatchFate> {
    let client_id = match session {
        "alice" => world.alice_client_id,
        "bob" => world.bob_client_id,
        other => panic!("no client registered for session {other}"),
    };
    let batch_id = world
        .client
        .update(
            world.row_id,
            vec![("body".to_string(), Value::Text(body.to_string()))],
            Some(&WriteContext::from_session(Session::new(session))),
        )
        .expect("the device applies the write locally; the server decides");
    pump(
        &mut world.server,
        world.server_id,
        &mut world.client,
        client_id,
    );
    world.server.batched_tick();
    world.server.immediate_tick();
    world.server.sync_sender().take();
    world.last_batch_id = batch_id;
    world
        .server
        .storage()
        .load_authoritative_batch_fate(batch_id)
        .expect("batch fate readable")
}

/// The write that follows a REFUSED one arrives parented on a batch the server
/// holds no visible row for, so `sync_manager::inbox::pre_batch_visible_row`
/// answers `None` and `evaluate_update_permission` must resolve the old content
/// itself, out of `storage.load_visible_region_row(...)`. That is the read this
/// defect made stale, and it is where both stale-policy surfaces meet: the
/// bytes come from the locator ladder, and the shape they are lensed with comes
/// from `__row_locator.origin_schema_hash`.
fn assert_the_next_write_takes_the_old_content_fill_in(world: &CrossGenerationWorld) {
    assert!(
        world
            .server
            .storage()
            .load_history_row_batch(TABLE, &world.branch, world.row_id, world.last_batch_id)
            .expect("history read should succeed")
            .is_none_or(|row| !row.state.is_visible()),
        "the server holds the preceding write as a visible history row — either \
         it was never refused, or it was kept; either way the next write \
         resolves its old content from that parent and never reaches \
         `evaluate_update_permission`'s visible-region fill-in"
    );
}

/// THE GATE. Alice handed the row to bob; she must not be able to keep writing.
///
/// Against the CURRENT (generation-B) content the USING arm denies her.
/// Against the copy stranded in generation A — the one the stale locator ladder
/// served, lensed with the shape the stale locator named — it reads
/// `owner = alice` and lets her write. A session that has lost access keeps
/// writing, which is the security consequence this file exists for.
#[test]
fn a_session_that_lost_ownership_cannot_write_through_a_cross_generation_row() {
    let mut world = cross_generation_world("defect27-lost-ownership");
    transfer_ownership_to_bob(&mut world);

    // Her first try is refused off the parent her own device correctly named.
    let first = attempt(&mut world, "alice", "hijacked once");
    assert!(
        matches!(&first, Some(BatchFate::Rejected { reason, .. }) if reason.contains("USING")),
        "alice no longer owns this row and her update was not denied by the \
         USING arm: fate={first:?}"
    );

    // Her second try is the one that matters: it is parented on that refusal,
    // so the server has to resolve the old content from its own visible read.
    assert_the_next_write_takes_the_old_content_fill_in(&world);
    let second = attempt(&mut world, "alice", "hijacked twice");
    assert!(
        matches!(&second, Some(BatchFate::Rejected { reason, .. }) if reason.contains("USING")),
        "alice no longer owns this row, and the USING arm APPROVED her update: \
         the old content it was handed came from the generation-A copy the row \
         left behind, which still says `owner = alice`. fate={second:?}"
    );

    assert_ne!(
        body_of(&world.server, &world.branch, world.row_id).as_deref(),
        Some("hijacked twice"),
        "the denied write landed on the server's row anyway"
    );
    assert_eq!(
        owner_of(&world.server, &world.branch, world.row_id).as_deref(),
        Some("bob"),
        "the row must still be bob's after the refused writes"
    );
    assert_the_row_reads_as_the_generation_it_moved_to(&world);
}

/// The positive control the gate above cannot pass without — "deny everything"
/// satisfies it — reached through the same `old_content` fill-in, so the same
/// stale read is being asked the opposite question.
#[test]
fn the_current_owner_still_writes_through_a_cross_generation_row() {
    let mut world = cross_generation_world("defect27-current-owner");
    transfer_ownership_to_bob(&mut world);

    // Alice's refused write runs first on purpose: it is what puts bob's write
    // on the fill-in path rather than on the parent-row path.
    let refused = attempt(&mut world, "alice", "hijacked");
    assert!(
        matches!(&refused, Some(BatchFate::Rejected { .. })),
        "control: alice's write should have been refused, and it was not: \
         fate={refused:?}"
    );

    assert_the_next_write_takes_the_old_content_fill_in(&world);
    let approved = attempt(&mut world, "bob", "bobs edit");
    assert!(
        !matches!(&approved, Some(BatchFate::Rejected { .. })),
        "the CURRENT owner's update was denied: the old content \
         `evaluate_update_permission` filled in named an owner this row no \
         longer has. fate={approved:?}"
    );
    assert_eq!(
        body_of(&world.server, &world.branch, world.row_id).as_deref(),
        Some("bobs edit"),
        "bob's approved write never reached the server's row"
    );
    assert_the_row_reads_as_the_generation_it_moved_to(&world);
}
