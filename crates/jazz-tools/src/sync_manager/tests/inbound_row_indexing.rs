//! A row applied from INBOUND sync must leave index entries, not just a visible head.
//!
//! MEASURED on the local stack (2026-08-17). One `unique_names` row, written by the
//! rpc-server and synced onward, on branch `dev-b32dae47bbd9-main`:
//!
//! ```text
//!                         visible head   idx:unique_names:userId:<branch>
//!   rpc-server (writer)   VisibleDirect   1 entry
//!   jazz-sync (receiver)  VisibleDirect   0 entries
//! ```
//!
//! Same row, same branch, same generation. The writer indexed it; the receiver did not.
//! Neighbouring tables on the SAME branch of the SAME store are indexed normally
//! (`user_emails:userId` 1, `chat_members:chatId` 9, `calls:chatId` 13), so this is not a
//! store-wide failure.
//!
//! What that costs, and why it is invisible: a row findable only by `_id` still answers a
//! direct lookup, so the server reports it as present — while every backref
//! (`unique_namesViaUser`) resolves through `IndexScanNode` over
//! `idx:<table>:<column>:<branch>` and finds nothing. The row therefore never enters any
//! client's subscription scope and is never offered. Measured consequence: the handle a
//! user had just taken existed on both servers and on no client, permanently — neither an
//! app restart nor a jazz-sync restart delivered it (both were tried; jazz-sync holds its
//! subscription and delivery bookkeeping in memory, so its restart re-derives scope from
//! scratch and still produced nothing).
//!
//! The suspected mechanism, to be confirmed by this gate rather than assumed:
//! `QueryManager::prepare_row_update_with_origin` has three exits that `return None`
//! WITHOUT buffering the update and WITHOUT logging anything — the table missing from the
//! schema resolved for the row's branch (query_manager/manager.rs:1893, :1899, :1905).
//! Its two loud exits (:1861, :1871, :1915) push the update onto
//! `pending_row_visibility_changes` and log at error; the jazz-sync logs contain zero of
//! those, which is what points at the silent three. On that exit the sync layer has
//! already persisted the visible row, so the store ends up exactly as measured: head
//! present, index absent, nothing marked dirty, nothing retried.
//!
//! This is the same geometry as defects 20 and 27 — the local write path cured, the
//! inbound path left behind — one layer up: there it was which raw-table FAMILY a read
//! consults, here it is whether the INDEX is written at all.

use super::*;
use crate::query_manager::types::Schema;
use crate::schema_manager::{AppId, SchemaManager};
use crate::storage::SqliteStorage;

const APP: &str = "inbound-row-indexing";

/// A table whose rows are found by backref: `owner` is a ref, exactly like
/// `unique_names.userId`.
fn handles_schema() -> Schema {
    SchemaBuilder::new()
        .table(TableSchema::builder("users").column("name", ColumnType::Text))
        .table(
            TableSchema::builder("handles")
                .fk_column("owner", "users")
                .column("handle", ColumnType::Text),
        )
        .build()
}

fn index_entry_count<H: Storage>(io: &H, table: &str, column: &str, branch: &str) -> usize {
    io.raw_table_scan_prefix(&format!("idx:{table}:{column}:{branch}"), "")
        .map(|entries| entries.len())
        .unwrap_or(0)
}

/// THE GATE.
///
/// A client writes a row and syncs it to a server-mode peer (the jazz-sync shape:
/// `SchemaManager::new_server`, no declared schema, learns generations from the
/// catalogue). The server must end up with BOTH the visible head and the index
/// entries — anything less is a row that exists and cannot be found by a backref.
#[test]
fn an_inbound_row_leaves_index_entries_on_the_receiver() {
    let schema = handles_schema();
    let app_id = AppId::from_name(APP);

    let mut io_client = SqliteStorage::open(":memory:").expect("client storage");
    let mut io_server = SqliteStorage::open(":memory:").expect("server storage");

    let mut client =
        SchemaManager::new(SyncManager::new(), schema.clone(), app_id, "dev", "main").unwrap();
    let mut server = SchemaManager::new_server(SyncManager::new(), app_id, "dev");

    let client_id = ClientId::new();
    let server_id = ServerId::new();
    server
        .query_manager_mut()
        .sync_manager_mut()
        .add_client_with_storage(&io_server, client_id);
    server
        .query_manager_mut()
        .sync_manager_mut()
        .set_client_role(client_id, ClientRole::Admin);
    client
        .query_manager_mut()
        .sync_manager_mut()
        .add_server_with_storage(server_id, false, &io_client);

    // The server learns the generation the way jazz-sync does — over the wire.
    client.persist_schema(&mut io_client);
    client.process(&mut io_client);
    for entry in client.query_manager_mut().sync_manager_mut().take_outbox() {
        if let SyncPayload::CatalogueEntryUpdated { entry } = &entry.payload {
            server
                .query_manager_mut()
                .sync_manager_mut()
                .push_inbox(InboxEntry {
                    source: Source::Client(client_id),
                    payload: SyncPayload::CatalogueEntryUpdated {
                        entry: entry.clone(),
                    },
                });
        }
    }
    server.process(&mut io_server);

    let owner = ObjectId::new();
    let written = client
        .insert(
            &mut io_client,
            "handles",
            HashMap::from([
                ("owner".to_string(), Value::Uuid(owner)),
                ("handle".to_string(), Value::Text("ekka".to_string())),
            ]),
            None,
            None,
        )
        .expect("the client writes the row");
    client.process(&mut io_client);

    let branch = client.branch_name().to_string();

    // Control: the WRITER indexed it. If this fails the fixture is wrong, not the engine.
    assert_eq!(
        index_entry_count(&io_client, "handles", "owner", &branch),
        1,
        "precondition: the writing peer must index its own row on {branch}"
    );

    // Ship every row the client produced to the server, the way sync does.
    for entry in client.query_manager_mut().sync_manager_mut().take_outbox() {
        if matches!(
            entry.payload,
            SyncPayload::RowBatchCreated { .. } | SyncPayload::RowBatchNeeded { .. }
        ) {
            server
                .query_manager_mut()
                .sync_manager_mut()
                .push_inbox(InboxEntry {
                    source: Source::Client(client_id),
                    payload: entry.payload.clone(),
                });
        }
    }
    server.process(&mut io_server);
    for check in server
        .query_manager_mut()
        .sync_manager_mut()
        .take_pending_permission_checks()
    {
        server
            .query_manager_mut()
            .sync_manager_mut()
            .approve_permission_check(&mut io_server, check);
    }
    server.process(&mut io_server);

    // The row did arrive — this is the half that already works, and it is what makes the
    // defect invisible: a direct lookup answers, so the server reports the row as present.
    let head = io_server
        .load_visible_region_row("handles", &branch, written.row_id)
        .expect("visible read should succeed");
    assert!(
        head.is_some(),
        "precondition: the inbound row must be persisted as a visible head on {branch}"
    );

    // THE ASSERTION. Red today: the receiver holds the head and no index entry, so every
    // backref over `handles.owner` misses it and no subscription scope can ever contain it.
    assert_eq!(
        index_entry_count(&io_server, "handles", "owner", &branch),
        1,
        "a row applied from inbound sync must leave its index entries: the receiver holds \
         the visible head for row {} on {branch} but idx:handles:owner:{branch} is empty, \
         so the row answers a direct `_id` lookup and is invisible to every backref \
         (IndexScanNode reads exactly this table). Measured live on jazz-sync for \
         unique_names, where it left a freshly taken handle undeliverable to every client, \
         permanently.",
        written.row_id
    );
}

/// Generation B: `handles` is byte-identical, an added table moves the schema hash and
/// therefore the branch name — the shape of the production migration that added
/// `chat_activities`.
fn handles_schema_v2() -> Schema {
    SchemaBuilder::new()
        .table(TableSchema::builder("users").column("name", ColumnType::Text))
        .table(
            TableSchema::builder("handles")
                .fk_column("owner", "users")
                .column("handle", ColumnType::Text),
        )
        .table(TableSchema::builder("activities").column("kind", ColumnType::Text))
        .build()
}

/// Feed one client's outbox into the server and settle it.
fn ship<H: Storage>(
    client: &mut SchemaManager,
    server: &mut SchemaManager,
    io_server: &mut H,
    client_id: ClientId,
) {
    for entry in client.query_manager_mut().sync_manager_mut().take_outbox() {
        server
            .query_manager_mut()
            .sync_manager_mut()
            .push_inbox(InboxEntry {
                source: Source::Client(client_id),
                payload: entry.payload.clone(),
            });
    }
    server.process(io_server);
    for check in server
        .query_manager_mut()
        .sync_manager_mut()
        .take_pending_permission_checks()
    {
        server
            .query_manager_mut()
            .sync_manager_mut()
            .approve_permission_check(io_server, check);
    }
    server.process(io_server);
}

/// The two-generation variant, which is the shape production is actually in: the
/// receiving store already holds rows of this table under the OLD generation's family
/// when a row of the NEW generation arrives.
///
/// This is the only structural difference between `unique_names` — measured with an empty
/// index on the new branch — and its neighbours `user_emails`, `chat_members`, `calls`,
/// which are indexed there normally and exist in one generation only.
// OPEN DEFECT — this gate is red on purpose and is NOT the scope-exemption defect fixed
// in `query_manager::manager_tests::local_write_exemption`. A row taken in from a Backend
// writer, for a table the receiver already holds under an OLDER generation, lands with a
// visible head and no index entry: every inbound apply exits at "buffering row update for
// unknown schema hash". Measured on jazz-sync, table `unique_names`. Ignored so it does
// not block unrelated releases; run it with
//   cargo test -p jazz-tools --features "sqlite rocksdb test-utils" --lib -- --ignored inbound_row_indexing
#[test]
#[ignore = "open defect: inbound row lands unindexed when its table exists in an older generation"]
fn an_inbound_row_is_indexed_when_the_table_already_exists_in_an_older_generation() {
    let app_id = AppId::from_name(APP);
    let mut io_server = SqliteStorage::open(":memory:").expect("server storage");
    let mut server = SchemaManager::new_server(SyncManager::new(), app_id, "dev");

    // ── generation A: a client writes a handle, the server takes it in ──────────
    let mut io_a = SqliteStorage::open(":memory:").expect("client A storage");
    let mut client_a =
        SchemaManager::new(SyncManager::new(), handles_schema(), app_id, "dev", "main").unwrap();
    let client_a_id = ClientId::new();
    server
        .query_manager_mut()
        .sync_manager_mut()
        .add_client_with_storage(&io_server, client_a_id);
    server
        .query_manager_mut()
        .sync_manager_mut()
        .set_client_role(client_a_id, ClientRole::Admin);
    client_a
        .query_manager_mut()
        .sync_manager_mut()
        .add_server_with_storage(ServerId::new(), false, &io_a);

    client_a.persist_schema(&mut io_a);
    client_a.process(&mut io_a);
    ship(&mut client_a, &mut server, &mut io_server, client_a_id);

    client_a
        .insert(
            &mut io_a,
            "handles",
            HashMap::from([
                ("owner".to_string(), Value::Uuid(ObjectId::new())),
                ("handle".to_string(), Value::Text("old_gen".to_string())),
            ]),
            None,
            None,
        )
        .expect("generation A row inserts");
    client_a.process(&mut io_a);
    ship(&mut client_a, &mut server, &mut io_server, client_a_id);

    let branch_a = client_a.branch_name().to_string();
    assert_eq!(
        index_entry_count(&io_server, "handles", "owner", &branch_a),
        1,
        "precondition: the server indexes the generation-A row it took in on {branch_a}"
    );

    // ── generation B: the migration lands, and a new handle arrives ─────────────
    let mut io_b = SqliteStorage::open(":memory:").expect("client B storage");
    let mut client_b = SchemaManager::new(
        SyncManager::new(),
        handles_schema_v2(),
        app_id,
        "dev",
        "main",
    )
    .unwrap();
    let client_b_id = ClientId::new();
    server
        .query_manager_mut()
        .sync_manager_mut()
        .add_client_with_storage(&io_server, client_b_id);
    server
        .query_manager_mut()
        .sync_manager_mut()
        // The rpc-server connects to jazz-sync as role="backend" (measured in the live
        // sync log), not as a session/Admin client — and roles take different inbound
        // branches.
        .set_client_role(client_b_id, ClientRole::Backend);
    client_b
        .query_manager_mut()
        .sync_manager_mut()
        .add_server_with_storage(ServerId::new(), false, &io_b);

    client_b.persist_schema(&mut io_b);
    client_b.process(&mut io_b);
    ship(&mut client_b, &mut server, &mut io_server, client_b_id);

    let written = client_b
        .insert(
            &mut io_b,
            "handles",
            HashMap::from([
                ("owner".to_string(), Value::Uuid(ObjectId::new())),
                ("handle".to_string(), Value::Text("ekka".to_string())),
            ]),
            None,
            None,
        )
        .expect("generation B row inserts");
    client_b.process(&mut io_b);
    ship(&mut client_b, &mut server, &mut io_server, client_b_id);

    let branch_b = client_b.branch_name().to_string();
    assert_ne!(branch_a, branch_b, "the fixture must cross generations");

    let head = io_server
        .load_visible_region_row("handles", &branch_b, written.row_id)
        .expect("visible read should succeed");
    assert!(
        head.is_some(),
        "precondition: the generation-B row must be persisted as a visible head on {branch_b}"
    );

    assert_eq!(
        index_entry_count(&io_server, "handles", "owner", &branch_b),
        1,
        "a row taken in under a NEW generation must be indexed on its own branch even when \
         the receiver already holds this table under an older generation. Measured on \
         jazz-sync: idx:unique_names:userId:dev-b32dae47bbd9-main held 0 entries while the \
         row was VisibleDirect, and unique_names is the one table in that store living in \
         two generations — its single-generation neighbours index fine."
    );
}
