//! Security: the wire ClientId presented on the `/ws` handshake is attacker
//! controlled.
//!
//! The server parses `client_id` from the handshake BEFORE authenticating and
//! never cross-checks it against the authenticated subject. It then keys its
//! delivery bookkeeping by that id: `ConnectionEventHub::prepare_payload`
//! fans every payload out to ALL live connections sharing the client id, and
//! server subscriptions live in a `(ClientId, QueryId)` map.
//!
//! So a second, differently-authenticated user can present someone else's
//! client id. This test pins the security property that must hold: whatever
//! the server does with such a handshake (reject it, or isolate it), the
//! impersonator must never receive rows their own session cannot read.

#![cfg(feature = "test")]

mod support;

use std::time::Duration;

use jazz_tools::query_manager::types::{permissions, policy_expr as pe};
use jazz_tools::row_input;
use jazz_tools::server::{JazzServer, TestJwtIssuer};
use jazz_tools::{
    AppContext, ClientStorage, ColumnType, DurabilityTier, JazzClient, QueryBuilder, Schema,
    SchemaBuilder, TableSchema, Value,
};
use support::wait_for_query;
use tempfile::TempDir;

/// `secrets` rows are readable only by their owner.
fn owner_gated_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("secrets")
                .column("owner_id", ColumnType::Text)
                .column("body", ColumnType::Text)
                .policies(permissions(|p| {
                    p.allow_insert().always();
                    p.allow_read()
                        .where_(pe::eq("owner_id", pe::session("user_id")));
                })),
        )
        .build()
}

fn context_for(
    server: &JazzServer,
    schema: &Schema,
    user_id: &str,
    data_dir: &TempDir,
    client_id: Option<jazz_tools::ClientId>,
) -> AppContext {
    AppContext {
        app_id: server.app_id(),
        client_id,
        schema: schema.clone(),
        server_url: server.base_url(),
        data_dir: data_dir.path().to_path_buf(),
        storage: ClientStorage::Persistent,
        jwt_token: Some(TestJwtIssuer::jwt_for_user(user_id)),
        backend_secret: None,
        admin_secret: None,
        sync_tracer: None,
    }
}

#[tokio::test]
async fn impersonating_another_clients_wire_id_must_not_leak_their_rows() {
    let schema = owner_gated_schema();
    let server = JazzServer::start_with_schema(schema.clone()).await;

    // ── alice: legitimate client with a live subscription ──────────────────
    let alice_dir = TempDir::new().expect("alice dir");
    let alice = JazzClient::connect(context_for(&server, &schema, "alice", &alice_dir, None))
        .await
        .expect("connect alice");
    let alice_wire = alice.client_id().expect("alice wire client id");

    let alice_query = QueryBuilder::new("secrets").build();
    let _alice_sub = alice
        .subscribe(alice_query.clone())
        .await
        .expect("alice subscribes");

    let (_secret_id, _, batch) = alice
        .insert(
            "secrets",
            row_input!("owner_id" => "alice", "body" => "alice-private-material"),
        )
        .expect("alice inserts secret");
    alice
        .wait_for_batch(batch, DurabilityTier::EdgeServer)
        .await
        .expect("secret reaches the server");

    let alice_rows = wait_for_query(
        &alice,
        alice_query.clone(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(30),
        "alice sees her own secret".to_string(),
        |rows| (!rows.is_empty()).then_some(rows),
    )
    .await;
    assert_eq!(alice_rows.len(), 1, "alice must see her own secret");

    // ── mallory: different user, same wire client id ───────────────────────
    // Modelled as an UNTRUSTED client (`PermissiveLocal`): an attacker forging
    // a wire id already controls their binary, so they would not run the
    // client-side policy filter that drops rows their session cannot read.
    // Anything the server sends them, they keep.
    let mallory_dir = TempDir::new().expect("mallory dir");
    let mallory_connect = JazzClient::connect_with_row_policy_mode(
        context_for(&server, &schema, "mallory", &mallory_dir, Some(alice_wire)),
        jazz_tools::query_manager::types::RowPolicyMode::PermissiveLocal,
    )
    .await;

    let mallory = match mallory_connect {
        Ok(mallory) => {
            eprintln!("OUTCOME: server ACCEPTED the impersonating handshake");
            mallory
        }
        Err(error) => {
            // Rejecting the handshake outright is a perfectly good outcome.
            eprintln!("OUTCOME: server REJECTED the impersonating handshake: {error:?}");
            return;
        }
    };
    assert_eq!(
        mallory.client_id(),
        Some(alice_wire),
        "test setup: mallory must actually present alice's wire id"
    );

    // A THIRD party writes a row alice owns. Delivery of this row to alice is
    // genuinely server-driven (unlike alice's own writes, which she holds
    // locally regardless), so it measures both the leak to mallory and
    // whether the impersonator broke alice's own sync.
    let carol_dir = TempDir::new().expect("carol dir");
    let carol = JazzClient::connect(context_for(&server, &schema, "carol", &carol_dir, None))
        .await
        .expect("connect carol");
    let (_second_id, _, second_batch) = carol
        .insert(
            "secrets",
            row_input!("owner_id" => "alice", "body" => "alice-second-secret"),
        )
        .expect("carol inserts a secret owned by alice");
    carol
        .wait_for_batch(second_batch, DurabilityTier::EdgeServer)
        .await
        .expect("second secret reaches the server");

    // Give the server every chance to deliver to mallory's connection.
    tokio::time::sleep(Duration::from_secs(3)).await;

    let mallory_rows = mallory
        .query(QueryBuilder::new("secrets").build(), None)
        .await
        .expect("mallory queries secrets");
    eprintln!(
        "mallory local query returned {} row(s): {:?}",
        mallory_rows.len(),
        mallory_rows,
    );
    let leaked: Vec<String> = mallory_rows
        .iter()
        .flat_map(|(_, values)| {
            values.iter().filter_map(|value| match value {
                Value::Text(text) if text.starts_with("alice-") => Some(text.clone()),
                _ => None,
            })
        })
        .collect();

    assert!(
        leaked.is_empty(),
        "a client authenticated as `mallory` received alice's rows by \
         presenting her wire client id: {leaked:?}"
    );

    // A policy-filtered read proves nothing on its own: the rows could be
    // sitting in mallory's local store while only the read path hides them.
    // Re-open her store as an untrusted client (PermissiveLocal ignores local
    // policy, the documented way to model a tampered client) and look again.
    mallory.shutdown().await.expect("shutdown mallory");

    let mut offline = context_for(&server, &schema, "mallory", &mallory_dir, None);
    offline.server_url = String::new();
    let tampered = JazzClient::connect_with_row_policy_mode(
        offline,
        jazz_tools::query_manager::types::RowPolicyMode::PermissiveLocal,
    )
    .await
    .expect("re-open mallory's store as an untrusted client");
    let on_disk = tampered
        .query(QueryBuilder::new("secrets").build(), None)
        .await
        .expect("untrusted read of mallory's store");
    eprintln!(
        "mallory's ON-DISK store holds {} secrets row(s): {:?}",
        on_disk.len(),
        on_disk,
    );
    let on_disk_leak: Vec<String> = on_disk
        .iter()
        .flat_map(|(_, values)| {
            values.iter().filter_map(|value| match value {
                Value::Text(text) if text.starts_with("alice-") => Some(text.clone()),
                _ => None,
            })
        })
        .collect();
    assert!(
        on_disk_leak.is_empty(),
        "alice's rows were synced onto the impersonator's device (hidden only \
         by the client-side read filter): {on_disk_leak:?}"
    );

    tampered.shutdown().await.expect("shutdown tampered reader");

    // ── availability: the victim must survive being impersonated ──────────
    // `ensure_client_with_session` REPLACES the session on the shared client
    // state, so the impersonator's session becomes the one the server uses to
    // decide what that client id may see. Alice stays connected but goes
    // silent. The control test below proves this row does arrive when nobody
    // impersonates her.
    let alice_rows_after = wait_for_query(
        &alice,
        alice_query.clone(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(20),
        "alice still receives rows while impersonated".to_string(),
        |rows| (rows.len() == 2).then_some(rows),
    )
    .await;
    assert_eq!(
        alice_rows_after.len(),
        2,
        "a client authenticated as `mallory` silenced alice's sync by \
         presenting her wire client id"
    );

    carol.shutdown().await.expect("shutdown carol");
    alice.shutdown().await.expect("shutdown alice");
    server.shutdown().await;
}

/// Control for the test above: with NO impersonator attached, a row written by
/// a third party and owned by alice must reach alice. Without this, a "alice
/// never got carol's row" result would be unattributable — it could just mean
/// this policy shape never delivers.
#[tokio::test]
async fn third_party_write_reaches_the_owner_without_an_impersonator() {
    let schema = owner_gated_schema();
    let server = JazzServer::start_with_schema(schema.clone()).await;

    let alice_dir = TempDir::new().expect("alice dir");
    let alice = JazzClient::connect(context_for(&server, &schema, "alice", &alice_dir, None))
        .await
        .expect("connect alice");
    let alice_query = QueryBuilder::new("secrets").build();
    let _alice_sub = alice
        .subscribe(alice_query.clone())
        .await
        .expect("alice subscribes");

    let (_own_id, _, own_batch) = alice
        .insert(
            "secrets",
            row_input!("owner_id" => "alice", "body" => "alice-private-material"),
        )
        .expect("alice inserts secret");
    alice
        .wait_for_batch(own_batch, DurabilityTier::EdgeServer)
        .await
        .expect("own secret reaches the server");

    let carol_dir = TempDir::new().expect("carol dir");
    let carol = JazzClient::connect(context_for(&server, &schema, "carol", &carol_dir, None))
        .await
        .expect("connect carol");
    let (_second_id, _, second_batch) = carol
        .insert(
            "secrets",
            row_input!("owner_id" => "alice", "body" => "alice-second-secret"),
        )
        .expect("carol inserts a secret owned by alice");
    carol
        .wait_for_batch(second_batch, DurabilityTier::EdgeServer)
        .await
        .expect("second secret reaches the server");

    let rows = wait_for_query(
        &alice,
        alice_query,
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(20),
        "alice receives a third party's write for a row she owns".to_string(),
        |rows| (rows.len() == 2).then_some(rows),
    )
    .await;
    eprintln!(
        "CONTROL: alice sees {} row(s) with no impersonator",
        rows.len()
    );

    alice.shutdown().await.expect("shutdown alice");
    carol.shutdown().await.expect("shutdown carol");
    server.shutdown().await;
}
