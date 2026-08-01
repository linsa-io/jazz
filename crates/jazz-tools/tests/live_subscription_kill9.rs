//! Live-subscription delivery across an UNGRACEFUL server death (SIGKILL).
//!
//! Companion to `live_subscription_ingest.rs`, which proved the graceful
//! path sound. The field outage (2026-08-01, linsa) happened on a server
//! that was OOM-killed mid-write and restarted over its RocksDB dir —
//! after which long-lived client subscriptions delivered nothing while the
//! storage verifiably held the rows. This test reproduces that lifecycle
//! with the real `jazz-tools` server binary:
//!
//!   - rows carry a Bytea column (the production payload shape),
//!   - one subscription is a bare Eq filter, the other is the production
//!     list shape (`order_by desc` + `limit`),
//!   - the server is SIGKILLed (no shutdown path runs) and restarted on the
//!     same data dir and port,
//!   - both pre-kill live subscriptions and a fresh one must then deliver
//!     rows written after the restart.
//!
//! Self-contained harness: integration test files in this crate each carry
//! their own process/JWKS plumbing by convention (see
//! client_restart_integration.rs), since `mod support` holds only the
//! in-process helpers.

#![cfg(feature = "test")]

mod support;

use std::collections::BTreeSet;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::{Json, Router, routing::get};
use base64::Engine;
use futures::StreamExt as _;
use jazz_tools::object::ObjectId;
use jazz_tools::row_input;
use jazz_tools::schema_manager::AppId;
use jazz_tools::sync_manager::DurabilityTier;
use jazz_tools::{
    AppContext, ClientStorage, ColumnType, JazzClient, QueryBuilder, SchemaBuilder, TableSchema,
    Value,
};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use support::publish_allow_all_permissions;
use tempfile::TempDir;

const APP_ID_STR: &str = "00000000-0000-0000-0000-000000000002";
const BACKEND_SECRET: &str = "backend-secret-for-kill9-tests";
const ADMIN_SECRET: &str = "admin-secret-for-kill9-tests";
const JWT_KID: &str = "kill9-jwks-kid";
const JWT_SECRET: &str = "kill9-jwt-secret-for-integration";

const READY_TIMEOUT: Duration = Duration::from_secs(30);
const DELIVERY_DEADLINE: Duration = Duration::from_secs(20);

// ── schema ──────────────────────────────────────────────────────────────────

fn parts_schema() -> jazz_tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("parts")
                .column("file_id", ColumnType::Text)
                .column("idx", ColumnType::Integer)
                .column("data", ColumnType::Bytea),
        )
        .build()
}

// ── JWT / JWKS plumbing ─────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
struct JwtClaims {
    sub: String,
    claims: JsonValue,
    exp: u64,
}

fn make_jwt(sub: &str) -> String {
    let claims = JwtClaims {
        sub: sub.to_string(),
        claims: json!({"role": "user"}),
        exp: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock drift")
            .as_secs()
            + 3600,
    };
    let mut header = Header::new(Algorithm::HS256);
    header.kid = Some(JWT_KID.to_string());
    encode(
        &header,
        &claims,
        &EncodingKey::from_secret(JWT_SECRET.as_bytes()),
    )
    .expect("encode jwt")
}

struct JwksServer {
    addr: std::net::SocketAddr,
    task: tokio::task::JoinHandle<()>,
}

impl JwksServer {
    async fn start() -> Self {
        let app = Router::new().route("/jwks", get(jwks_handler));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind jwks server");
        let addr = listener.local_addr().expect("jwks local addr");
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve jwks");
        });
        Self { addr, task }
    }

    fn endpoint(&self) -> String {
        format!("http://{}/jwks", self.addr)
    }
}

impl Drop for JwksServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn jwks_handler() -> Json<JsonValue> {
    let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(JWT_SECRET.as_bytes());
    Json(json!({
        "keys": [
            { "kty": "oct", "kid": JWT_KID, "alg": "HS256", "k": encoded }
        ]
    }))
}

// ── out-of-process server (SIGKILL-able) ────────────────────────────────────

struct ServerProcess {
    process: Child,
    port: u16,
    bound_port_file: PathBuf,
    client: reqwest::Client,
}

impl ServerProcess {
    async fn start(port: u16, data_dir: &Path, jwks_endpoint: &str) -> Self {
        let bound_port_file = data_dir.join("bound-port");
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_jazz-tools"));
        cmd.args([
            "server",
            APP_ID_STR,
            "--port",
            &port.to_string(),
            "--data-dir",
            data_dir.to_str().expect("data dir path"),
        ])
        .env("JAZZ_JWKS_URL", jwks_endpoint)
        .env("JAZZ_BACKEND_SECRET", BACKEND_SECRET)
        .env("JAZZ_ADMIN_SECRET", ADMIN_SECRET)
        .env("JAZZ_BOUND_PORT_FILE", &bound_port_file)
        .stdout(Stdio::piped());
        if std::env::var("JAZZ_TEST_SERVER_LOGS").is_ok() {
            cmd.stderr(Stdio::inherit());
        } else {
            cmd.stderr(Stdio::piped());
        }

        let process = cmd.spawn().expect("spawn jazz-tools server");
        let mut server = Self {
            process,
            port,
            bound_port_file,
            client: reqwest::Client::new(),
        };
        server.wait_ready().await;
        server
    }

    fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    /// SIGKILL — no shutdown hook runs, mirroring an OOM kill.
    fn kill_hard(mut self) -> u16 {
        let port = self.port;
        let _ = self.process.kill();
        let _ = self.process.wait();
        port
    }

    async fn wait_ready(&mut self) {
        for _ in 0..200 {
            self.maybe_update_bound_port();
            if let Some(status) = self.process.try_wait().expect("poll jazz-tools server") {
                panic!(
                    "jazz-tools server exited before becoming ready: {status}{}",
                    self.process_output_summary()
                );
            }
            if self.port != 0 {
                let health_url = format!("{}/health", self.base_url());
                if let Ok(response) = self.client.get(&health_url).send().await
                    && response.status().is_success()
                {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!(
            "jazz-tools server did not become ready within 20 seconds{}",
            self.process_output_summary()
        );
    }

    fn maybe_update_bound_port(&mut self) {
        let Ok(contents) = std::fs::read_to_string(&self.bound_port_file) else {
            return;
        };
        let Ok(port) = contents.trim().parse::<u16>() else {
            return;
        };
        self.port = port;
    }

    fn process_output_summary(&mut self) -> String {
        let stdout = take_pipe_text(&mut self.process.stdout);
        let stderr = take_pipe_text(&mut self.process.stderr);
        if stdout.is_empty() && stderr.is_empty() {
            return String::new();
        }
        format!(
            "\nstdout:\n{}\nstderr:\n{}",
            if stdout.is_empty() { "<empty>" } else { stdout.trim() },
            if stderr.is_empty() { "<empty>" } else { stderr.trim() },
        )
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        if self.process.try_wait().ok().flatten().is_some() {
            return;
        }
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

fn take_pipe_text<T: Read>(pipe: &mut Option<T>) -> String {
    let Some(mut pipe) = pipe.take() else {
        return String::new();
    };
    let mut text = String::new();
    let _ = pipe.read_to_string(&mut text);
    text
}

// ── clients ─────────────────────────────────────────────────────────────────

fn app_id() -> AppId {
    AppId::from_string(APP_ID_STR).expect("parse app id")
}

async fn publish_schema(server: &ServerProcess) {
    let response = reqwest::Client::new()
        .post(format!(
            "{}/apps/{}/admin/schemas",
            server.base_url(),
            APP_ID_STR
        ))
        .header("X-Jazz-Admin-Secret", ADMIN_SECRET)
        .json(&json!({ "schema": parts_schema(), "permissions": null }))
        .send()
        .await
        .expect("publish schema");
    assert!(
        response.status().is_success() || response.status() == reqwest::StatusCode::CONFLICT,
        "schema publish failed: {}",
        response.status()
    );
    publish_allow_all_permissions(&server.base_url(), app_id(), ADMIN_SECRET, &parts_schema())
        .await;
}

async fn make_client(server: &ServerProcess, user_id: &str) -> JazzClient {
    let context = AppContext {
        app_id: app_id(),
        client_id: None,
        schema: parts_schema(),
        server_url: server.base_url(),
        data_dir: TempDir::new().expect("client dir").keep(),
        // Persistent, like a device: the client's own store must survive its
        // server disappearing and re-appearing.
        storage: ClientStorage::Persistent,
        jwt_token: Some(make_jwt(user_id)),
        backend_secret: None,
        admin_secret: None,
        sync_tracer: None,
    };
    let client = JazzClient::connect(context).await.expect("connect client");
    wait_for_ready(&client, user_id).await;
    client
}

async fn wait_for_ready(client: &JazzClient, who: &str) {
    let query = QueryBuilder::new("parts").build();
    let deadline = tokio::time::Instant::now() + READY_TIMEOUT;
    loop {
        if tokio::time::Instant::now() > deadline {
            panic!("{who}: EdgeServer readiness timed out");
        }
        if let Ok(Ok(_)) = tokio::time::timeout(
            Duration::from_secs(8),
            client.query(query.clone(), Some(DurabilityTier::EdgeServer)),
        )
        .await
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn insert_part(client: &JazzClient, file_id: &str, idx: i32) -> ObjectId {
    let payload: Vec<u8> = (0..64).map(|byte| (byte as u8).wrapping_mul(7)).collect();
    let (id, _, batch_id) = client
        .insert(
            "parts",
            row_input!("file_id" => file_id, "idx" => idx, "data" => payload),
        )
        .expect("insert part");
    client
        .wait_for_batch(batch_id, DurabilityTier::EdgeServer)
        .await
        .expect("part reaches the server");
    id
}

async fn expect_added(
    stream: &mut jazz_tools::SubscriptionStream,
    expected: &BTreeSet<ObjectId>,
    what: &str,
) {
    let mut seen: BTreeSet<ObjectId> = BTreeSet::new();
    let deadline = tokio::time::Instant::now() + DELIVERY_DEADLINE;
    while !expected.is_subset(&seen) {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            let missing: Vec<_> = expected.difference(&seen).collect();
            panic!("{what}: live subscription never delivered {missing:?} (saw {seen:?})");
        }
        let delta = tokio::time::timeout(remaining, stream.next())
            .await
            .unwrap_or_else(|_| {
                let missing: Vec<_> = expected.difference(&seen).collect();
                panic!("{what}: timed out waiting for {missing:?} (saw {seen:?})")
            })
            .unwrap_or_else(|| panic!("{what}: subscription stream closed early"));
        for added in &delta.added {
            seen.insert(added.id);
        }
    }
}

// ── the test ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn live_subscriptions_survive_sigkill_restart() {
    let jwks = JwksServer::start().await;
    let data_dir = TempDir::new().expect("server data dir");

    let server1 = ServerProcess::start(0, data_dir.path(), &jwks.endpoint()).await;
    publish_schema(&server1).await;

    let writer = make_client(&server1, "kill9-writer").await;
    let reader = make_client(&server1, "kill9-reader").await;

    // The two production subscription shapes, registered before any data.
    let eq_query = QueryBuilder::new("parts")
        .filter_eq("file_id", Value::Text("file-under-test".into()))
        .build();
    let list_query = QueryBuilder::new("parts")
        .filter_eq("file_id", Value::Text("file-under-test".into()))
        .order_by_desc("idx")
        .limit(50)
        .build();
    let mut eq_sub = reader.subscribe(eq_query.clone()).await.expect("eq sub");
    let mut list_sub = reader.subscribe(list_query.clone()).await.expect("list sub");

    let mut before: BTreeSet<ObjectId> = BTreeSet::new();
    for idx in 0..4 {
        before.insert(insert_part(&writer, "file-under-test", idx).await);
        insert_part(&writer, "other-file", idx).await;
    }
    expect_added(&mut eq_sub, &before, "pre-kill eq subscription").await;
    expect_added(&mut list_sub, &before, "pre-kill list subscription").await;

    // ── SIGKILL, then restart on the same dir and port ─────────────────────
    let port = server1.kill_hard();
    let server2 = ServerProcess::start(port, data_dir.path(), &jwks.endpoint()).await;

    wait_for_ready(&writer, "writer after restart").await;
    wait_for_ready(&reader, "reader after restart").await;

    let mut after: BTreeSet<ObjectId> = BTreeSet::new();
    for idx in 10..13 {
        after.insert(insert_part(&writer, "file-under-test", idx).await);
    }

    // The field failure: these two assertions are exactly what production
    // subscriptions stopped doing after the ungraceful restart.
    expect_added(&mut eq_sub, &after, "post-kill eq subscription").await;
    expect_added(&mut list_sub, &after, "post-kill list subscription").await;

    // A fresh subscription over the recovered store sees everything.
    let mut all = before.clone();
    all.extend(&after);
    let mut fresh = reader.subscribe(eq_query).await.expect("fresh sub");
    expect_added(&mut fresh, &all, "fresh subscription over recovered store").await;

    writer.shutdown().await.expect("shutdown writer");
    reader.shutdown().await.expect("shutdown reader");
    drop(server2);
}

/// Fire-and-forget insert: local-first commit, no durability wait. Mirrors the
/// app's chunked upload (`insert` + move on) and any write made while the
/// server is unreachable.
fn insert_part_nowait(client: &JazzClient, file_id: &str, idx: i32) -> ObjectId {
    let payload: Vec<u8> = (0..64).map(|byte| (byte as u8).wrapping_mul(3)).collect();
    let (id, _, _) = client
        .insert(
            "parts",
            row_input!("file_id" => file_id, "idx" => idx, "data" => payload),
        )
        .expect("insert part (nowait)");
    id
}

/// SIGKILL lands MID-INGEST, with unconfirmed batches in flight, and more
/// writes are committed locally while the server is dead. After the restart
/// every row the writer ever committed must reach the reader's live
/// subscription — this is the client's unconfirmed-batch replay contract.
///
/// Field shape (2026-08-01, linsa): the server was OOM-killed during write
/// storms, and a message sent from a client during the outage never became
/// visible to peers after recovery.
#[tokio::test]
async fn sigkill_mid_ingest_replays_unconfirmed_batches() {
    let jwks = JwksServer::start().await;
    let data_dir = TempDir::new().expect("server data dir");

    let server1 = ServerProcess::start(0, data_dir.path(), &jwks.endpoint()).await;
    publish_schema(&server1).await;

    let writer = make_client(&server1, "midkill-writer").await;
    let reader = make_client(&server1, "midkill-reader").await;

    let eq_query = QueryBuilder::new("parts")
        .filter_eq("file_id", Value::Text("file-under-test".into()))
        .build();
    let mut sub = reader.subscribe(eq_query).await.expect("subscribe");

    let mut all: BTreeSet<ObjectId> = BTreeSet::new();

    // Burst with no durability waits — some of these are mid-flight when the
    // server dies.
    for idx in 0..15 {
        all.insert(insert_part_nowait(&writer, "file-under-test", idx));
    }
    let port = server1.kill_hard();

    // Writes while the server is DEAD: local-first commits that only the
    // client's replay can ever deliver.
    for idx in 15..25 {
        all.insert(insert_part_nowait(&writer, "file-under-test", idx));
    }

    let server2 = ServerProcess::start(port, data_dir.path(), &jwks.endpoint()).await;
    wait_for_ready(&writer, "writer after mid-ingest kill").await;
    wait_for_ready(&reader, "reader after mid-ingest kill").await;

    // Post-restart traffic on top, then the full-set assertion.
    for idx in 25..30 {
        all.insert(insert_part_nowait(&writer, "file-under-test", idx));
    }

    expect_added(
        &mut sub,
        &all,
        "live subscription after mid-ingest SIGKILL (incl. outage-window writes)",
    )
    .await;

    writer.shutdown().await.expect("shutdown writer");
    reader.shutdown().await.expect("shutdown reader");
    drop(server2);
}
