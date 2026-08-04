//! Memory profile of app-like subscriptions on the REAL linsa schema.
//!
//! Fixture `fixtures/linsa-schema.json` is the production wire schema (with
//! the merged policy bundle) dumped via `serializeRuntimeSchema` — the exact
//! bytes the linsa backend feeds the engine. The scenario mirrors the
//! measured production census (19 live subscriptions: 9×messages, 4×chats,
//! 2×users, 2×media_assets, singles) and, per the field lead, a HOT users
//! row carrying 6000 presence-heartbeat batches of history — the suspected
//! per-batch (not per-row) cost amplifier behind the ~1 GB device step.
//!
//! This is a measurement probe: it prints per-phase live bytes; the only
//! hard assertions are survival and release-on-reap.

#![cfg(feature = "test")]

mod support;

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use jazz_tools::object::ObjectId;
use jazz_tools::query_manager::settle_cost::{LIVE_SUBQUERY_INSTANCES, LIVE_SUBQUERY_NODES};
use jazz_tools::query_manager::types::ColumnType;
use jazz_tools::row_input;
use jazz_tools::server::JazzServer;
use jazz_tools::sync_manager::DurabilityTier;
use jazz_tools::{JazzClient, QueryBuilder, Schema, Value};
use support::{connect_ready_client, connect_ready_user};

struct TrackingAllocator;

static LIVE_BYTES: AtomicU64 = AtomicU64::new(0);
static PEAK_BYTES: AtomicU64 = AtomicU64::new(0);
/// Cumulative allocated bytes — CHURN, not residency.
///
/// Live bytes are blind to the cost the per-write phase below exists to catch:
/// work that allocates and frees inside one settle leaves residency flat while
/// burning CPU. That is exactly the shape of the include-instance walk.
static TOTAL_ALLOCATED: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            TOTAL_ALLOCATED.fetch_add(layout.size() as u64, Ordering::Relaxed);
            let live = LIVE_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed)
                + layout.size() as u64;
            PEAK_BYTES.fetch_max(live, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        LIVE_BYTES.fetch_sub(layout.size() as u64, Ordering::Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            TOTAL_ALLOCATED.fetch_add(new_size as u64, Ordering::Relaxed);
            LIVE_BYTES.fetch_sub(layout.size() as u64, Ordering::Relaxed);
            let live = LIVE_BYTES.fetch_add(new_size as u64, Ordering::Relaxed) + new_size as u64;
            PEAK_BYTES.fetch_max(live, Ordering::Relaxed);
        }
        new_ptr
    }
}

#[global_allocator]
static GLOBAL: TrackingAllocator = TrackingAllocator;

fn live_mb() -> f64 {
    LIVE_BYTES.load(Ordering::Relaxed) as f64 / 1048576.0
}

fn peak_mb() -> f64 {
    PEAK_BYTES.load(Ordering::Relaxed) as f64 / 1048576.0
}

fn total_allocated() -> u64 {
    TOTAL_ALLOCATED.load(Ordering::Relaxed)
}

fn heartbeats() -> usize {
    std::env::var("HEARTBEATS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(6000)
}
const CHATS: usize = 5;
const MESSAGES_PER_CHAT: usize = 15;

fn linsa_schema() -> Schema {
    // The dump is the runtime-schema envelope; the engine table map lives at
    // schema.<any table>._schema (the TS layer stores the full map on every
    // table handle). NOTE: this dump carries columns only — no policy bundle —
    // so this profile measures schema width + history + subscription shape
    // with row policies OFF (PermissiveLocal visibility).
    let envelope: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/linsa-schema.json"))
            .expect("envelope must parse");
    serde_json::from_value(envelope["schema"]["users"]["_schema"].clone())
        .expect("production wire schema must parse")
}

/// Fill every required (non-nullable) column that `provided` does not cover
/// with a type-appropriate placeholder, so inserts satisfy the real schema.
fn fill_required(
    schema: &Schema,
    table: &str,
    provided: Vec<(String, Value)>,
) -> std::collections::HashMap<String, Value> {
    let table_schema = schema
        .get(&jazz_tools::query_manager::types::TableName::new(table))
        .unwrap_or_else(|| panic!("table {table} in schema"));
    let mut map: std::collections::HashMap<String, Value> = provided.into_iter().collect();
    for column in table_schema.columns.columns.iter() {
        let name = column.name.as_str();
        if name == "id" || map.contains_key(name) || column.nullable || column.default.is_some() {
            continue;
        }
        let value = match &column.column_type {
            ColumnType::Text => Value::Text("mock".into()),
            ColumnType::Enum { variants } => {
                Value::Text(variants.first().cloned().unwrap_or_default())
            }
            ColumnType::Integer => Value::Integer(1),
            ColumnType::BigInt => Value::BigInt(1),
            ColumnType::Double => Value::Double(1.0),
            ColumnType::Boolean => Value::Boolean(false),
            ColumnType::Timestamp => Value::Timestamp(1),
            ColumnType::Uuid => Value::Uuid(ObjectId::new()),
            ColumnType::Bytea => Value::Bytea(vec![0u8; 4]),
            ColumnType::Array { .. } => Value::Array(Vec::new()),
            other => panic!("unhandled required column {table}.{name}: {other:?}"),
        };
        map.insert(name.to_string(), value);
    }
    map
}

fn insert_filled(
    client: &JazzClient,
    schema: &Schema,
    table: &str,
    provided: Vec<(String, Value)>,
) -> ObjectId {
    let (id, _, _) = client
        .insert(table, fill_required(schema, table, provided))
        .unwrap_or_else(|e| panic!("insert into {table}: {e:?}"));
    id
}

/// The live-instance gauge next to the residency a phase added.
///
/// The production `jazz::settle_cost` line reports ~23 800 include-instance
/// EVALUATIONS per settle and the server holds ~1.5 GB; dividing one by the
/// other only yields bytes per instance if evaluations and live instances are
/// the same population. `live_instances` is the gauge that removes the
/// assumption, and printing it beside the phase's own MiB delta is the local
/// analogue of that division — with the caveat that the delta covers the whole
/// subscription graph (flat scans, sync scopes, row caches), not only the
/// includes, so the per-instance figure it implies is an UPPER bound. The
/// isolated per-instance measurement is `tests/include_instance_bytes.rs`.
fn report_include_census(phase: &str, phase_mb: f64) {
    let instances = LIVE_SUBQUERY_INSTANCES.load(Ordering::Relaxed);
    let nodes = LIVE_SUBQUERY_NODES.load(Ordering::Relaxed);
    let per_instance_kib = if instances == 0 {
        0.0
    } else {
        phase_mb * 1024.0 / instances as f64
    };
    eprintln!(
        "{phase} include census: {instances} live subgraph instances across {nodes} include \
         nodes; phase delta {phase_mb:.1} MiB => {per_instance_kib:.1} KiB per instance \
         (upper bound: the delta is the whole graph, not only includes)"
    );
}

/// The 19-subscription graph the production census measured, per client.
async fn open_app_graph(
    client: &JazzClient,
    user_id: ObjectId,
    chat_ids: &[ObjectId],
) -> Vec<jazz_tools::SubscriptionStream> {
    let mut subs = Vec::new();

    // users ×2: own row + directory
    subs.push(
        client
            .subscribe(
                QueryBuilder::new("users")
                    .filter_eq("id", Value::Uuid(user_id))
                    .limit(1)
                    .build(),
            )
            .await
            .expect("sub users self"),
    );
    subs.push(
        client
            .subscribe(QueryBuilder::new("users").build())
            .await
            .expect("sub users all"),
    );

    // chats ×4: three by-id + the visible list
    for chat_id in chat_ids.iter().take(3) {
        subs.push(
            client
                .subscribe(
                    QueryBuilder::new("chats")
                        .filter_eq("id", Value::Uuid(*chat_id))
                        .limit(1)
                        .build(),
                )
                .await
                .expect("sub chat by id"),
        );
    }
    // The chat list resolves its participants inline, so this one is
    // include-bearing too: one subgraph instance per visible chat.
    subs.push(
        client
            .subscribe(
                QueryBuilder::new("chats")
                    .with_array("members", |members| {
                        members
                            .from("chat_members")
                            .correlate("chatId", "chats.id")
                            .filter_eq("isBanned", Value::Boolean(false))
                    })
                    .build(),
            )
            .await
            .expect("sub chats list with members include"),
    );

    // messages ×9: five threads (limit 50) + four previews (limit 1).
    //
    // The first two threads carry the app's real INCLUDE shape: the mobile
    // thread view reads each message together with its attachments, and each
    // attachment together with the media variants that back the thumbnail.
    // That is one array-subquery node per thread with a live subgraph instance
    // per message, and a nested node inside it — the only shape in this
    // profile that instantiates cached subgraphs at all. Without it the
    // profile measures a graph made entirely of flat scans and is structurally
    // blind to per-instance costs (v13-2 shipped past it for exactly that
    // reason).
    for chat_id in chat_ids.iter().take(2) {
        subs.push(
            client
                .subscribe(
                    QueryBuilder::new("messages")
                        .filter_eq("chatId", Value::Uuid(*chat_id))
                        .filter_eq("isDeleted", Value::Boolean(false))
                        .order_by_desc("createdAtMs")
                        .limit(50)
                        .with_array("attachments", |attachments| {
                            attachments
                                .from("message_attachments")
                                .correlate("messageId", "messages.id")
                                .order_by("position")
                                .with_array("variants", |variants| {
                                    variants
                                        .from("media_asset_variants")
                                        .correlate(
                                            "mediaAssetId",
                                            "message_attachments.mediaAssetId",
                                        )
                                        .order_by("createdAtMs")
                                })
                        })
                        .build(),
                )
                .await
                .expect("sub thread with attachments include"),
        );
    }
    for chat_id in chat_ids.iter().skip(2).take(3) {
        subs.push(
            client
                .subscribe(
                    QueryBuilder::new("messages")
                        .filter_eq("chatId", Value::Uuid(*chat_id))
                        .filter_eq("isDeleted", Value::Boolean(false))
                        .order_by_desc("createdAtMs")
                        .limit(50)
                        .build(),
                )
                .await
                .expect("sub thread"),
        );
    }
    for chat_id in chat_ids.iter().take(4) {
        subs.push(
            client
                .subscribe(
                    QueryBuilder::new("messages")
                        .filter_eq("chatId", Value::Uuid(*chat_id))
                        .filter_eq("isDeleted", Value::Boolean(false))
                        .order_by_desc("createdAtMs")
                        .limit(1)
                        .build(),
                )
                .await
                .expect("sub preview"),
        );
    }

    // media_assets ×2, singles ×4
    subs.push(
        client
            .subscribe(QueryBuilder::new("media_assets").build())
            .await
            .expect("sub media"),
    );
    subs.push(
        client
            .subscribe(
                QueryBuilder::new("media_assets")
                    .filter_eq("ownerUserId", Value::Uuid(user_id))
                    .build(),
            )
            .await
            .expect("sub media own"),
    );
    subs.push(
        client
            .subscribe(
                QueryBuilder::new("tasks")
                    .filter_eq("ownerUserId", Value::Uuid(user_id))
                    .build(),
            )
            .await
            .expect("sub tasks"),
    );
    subs.push(
        client
            .subscribe(
                QueryBuilder::new("chat_members")
                    .filter_eq("userId", Value::Uuid(user_id))
                    .build(),
            )
            .await
            .expect("sub memberships"),
    );
    subs.push(
        client
            .subscribe(
                QueryBuilder::new("chat_drafts")
                    .filter_eq("userId", Value::Uuid(user_id))
                    .build(),
            )
            .await
            .expect("sub drafts"),
    );
    subs.push(
        client
            .subscribe(
                QueryBuilder::new("auth_pending_state")
                    .filter_eq("claimedBySessionUserId", Value::Text(user_id.to_string()))
                    .build(),
            )
            .await
            .expect("sub auth pending"),
    );

    subs
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn app_graph_on_real_schema_with_hot_history_row() {
    let schema = linsa_schema();
    eprintln!(
        "schema loaded: {} tables; live {:.1} MiB",
        schema.keys().count(),
        live_mb()
    );

    let server = JazzServer::builder()
        .with_schema(schema.clone())
        .with_rocksdb_storage()
        .start()
        .await;
    let ready = Duration::from_secs(60);
    let writer = connect_ready_client(&server, &schema, "writer", "users", ready).await;

    // ── seed ────────────────────────────────────────────────────────────────
    let alice = insert_filled(
        &writer,
        &schema,
        "users",
        vec![
            ("firstName".into(), Value::Text("Alice".into())),
            ("isActive".into(), Value::Boolean(true)),
            ("onlineTimeMs".into(), Value::Integer(0)),
        ],
    );
    let bob = insert_filled(
        &writer,
        &schema,
        "users",
        vec![
            ("firstName".into(), Value::Text("Bob".into())),
            ("isActive".into(), Value::Boolean(true)),
        ],
    );

    let mut chat_ids = Vec::new();
    let mut message_ids = Vec::new();
    let mut attachment_asset_ids = Vec::new();
    for _ in 0..CHATS {
        let chat = insert_filled(
            &writer,
            &schema,
            "chats",
            vec![
                ("kind".into(), Value::Text("direct".into())),
                ("createdByUserId".into(), Value::Uuid(alice)),
            ],
        );
        for user in [alice, bob] {
            insert_filled(
                &writer,
                &schema,
                "chat_members",
                vec![
                    ("chatId".into(), Value::Uuid(chat)),
                    ("userId".into(), Value::Uuid(user)),
                    ("role".into(), Value::Text("member".into())),
                    ("isBanned".into(), Value::Boolean(false)),
                ],
            );
        }
        for i in 0..MESSAGES_PER_CHAT {
            let message = insert_filled(
                &writer,
                &schema,
                "messages",
                vec![
                    ("chatId".into(), Value::Uuid(chat)),
                    ("senderKind".into(), Value::Text("user".into())),
                    ("senderUserId".into(), Value::Uuid(alice)),
                    ("createdAtMs".into(), Value::Timestamp(1000 + i as u64)),
                    ("isDeleted".into(), Value::Boolean(false)),
                ],
            );
            message_ids.push(message);

            // Every third message carries a media attachment with two
            // variants, so the thread includes hold non-empty arrays at both
            // levels. An include over empty arrays would settle without ever
            // touching the inner scans and would measure nothing.
            if i % 3 != 0 {
                continue;
            }
            let asset = insert_filled(
                &writer,
                &schema,
                "media_assets",
                vec![("ownerUserId".into(), Value::Uuid(alice))],
            );
            insert_filled(
                &writer,
                &schema,
                "message_attachments",
                vec![
                    ("messageId".into(), Value::Uuid(message)),
                    ("mediaAssetId".into(), Value::Uuid(asset)),
                    ("position".into(), Value::Integer(0)),
                ],
            );
            for variant in 0..2 {
                insert_filled(
                    &writer,
                    &schema,
                    "media_asset_variants",
                    vec![
                        ("mediaAssetId".into(), Value::Uuid(asset)),
                        ("createdAtMs".into(), Value::Timestamp(1000 + variant)),
                    ],
                );
            }
            attachment_asset_ids.push(asset);
        }
        chat_ids.push(chat);
    }
    for _ in 0..3 {
        insert_filled(
            &writer,
            &schema,
            "media_assets",
            vec![("ownerUserId".into(), Value::Uuid(alice))],
        );
    }
    let (_, _, seed_batch) = writer
        .insert(
            "tasks",
            fill_required(
                &schema,
                "tasks",
                vec![
                    ("ownerUserId".into(), Value::Uuid(alice)),
                    ("status".into(), Value::Text("open".into())),
                    ("isDeleted".into(), Value::Boolean(false)),
                ],
            ),
        )
        .expect("insert task");
    writer
        .wait_for_batch(seed_batch, DurabilityTier::EdgeServer)
        .await
        .expect("seed durable");
    eprintln!("phase A (seeded, no history): live {:.1} MiB", live_mb());

    // ── 6000 presence heartbeats on the hot users row ──────────────────────
    let mut last_hb = None;
    let heartbeat_count = heartbeats();
    for i in 0..heartbeat_count {
        let batch = writer
            .update(
                alice,
                vec![
                    ("onlineTimeMs".to_string(), Value::Integer(i as i32)),
                    (
                        "onlineTimeUpdatedAtMs".to_string(),
                        Value::Timestamp(2000 + i as u64),
                    ),
                ],
            )
            .expect("heartbeat update");
        last_hb = Some(batch);
    }
    writer
        .wait_for_batch(last_hb.expect("heartbeats"), DurabilityTier::EdgeServer)
        .await
        .expect("heartbeats durable");
    tokio::time::sleep(Duration::from_secs(2)).await;
    eprintln!(
        "phase B (+{heartbeat_count} heartbeat batches on users row): live {:.1} MiB",
        live_mb()
    );
    eprintln!(
        "phase B history fastpath counters: hits {} fallbacks {}",
        jazz_tools::row_histories::HISTORY_FASTPATH_HITS.load(Ordering::Relaxed),
        jazz_tools::row_histories::HISTORY_FASTPATH_FALLBACKS.load(Ordering::Relaxed),
    );
    eprintln!(
        "phase B patch fastpath counters: hits {} fallbacks {}",
        jazz_tools::row_histories::PATCH_FASTPATH_HITS.load(Ordering::Relaxed),
        jazz_tools::row_histories::PATCH_FASTPATH_FALLBACKS.load(Ordering::Relaxed),
    );

    // Hot-path scan tripwire (history-fastpaths §6): serving the phase C/D
    // subscription graphs — every row has a visible entry — must not walk row
    // history, neither through tier-gated reads nor through provenance
    // fallbacks. Snapshot here, assert zero growth after phase D.
    let tier_scans_before_subs =
        jazz_tools::row_histories::QUERY_TIER_READ_HISTORY_SCANS.load(Ordering::Relaxed);
    let provenance_scans_before_subs =
        jazz_tools::row_histories::QUERY_PROVENANCE_HISTORY_SCANS.load(Ordering::Relaxed);

    // ── device 1: alice's app graph ────────────────────────────────────────
    let alice_client =
        connect_ready_user(&server, &schema, &alice.to_string(), "users", ready).await;
    let before_alice = live_mb();
    let alice_subs = open_app_graph(&alice_client, alice, &chat_ids).await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    eprintln!(
        "phase C (alice: {} subs): live {:.1} MiB (+{:.1}), peak {:.1} MiB",
        alice_subs.len(),
        live_mb(),
        live_mb() - before_alice,
        peak_mb()
    );
    report_include_census("phase C", live_mb() - before_alice);

    // Visibility check so an empty-result graph cannot masquerade as cheap.
    let visible = alice_client
        .query(QueryBuilder::new("chats").build(), None)
        .await
        .map(|rows| rows.len())
        .unwrap_or(0);
    eprintln!("alice sees {visible} chats (0 would mean policy-denied graph)");

    // Same guard for the include shapes: an include whose arrays are all empty
    // instantiates no inner scans and would make the whole graph look cheap
    // for the wrong reason.
    let thread_rows = alice_client
        .query(
            QueryBuilder::new("messages")
                .filter_eq("chatId", Value::Uuid(chat_ids[0]))
                .with_array("attachments", |attachments| {
                    attachments
                        .from("message_attachments")
                        .correlate("messageId", "messages.id")
                        .with_array("variants", |variants| {
                            variants
                                .from("media_asset_variants")
                                .correlate("mediaAssetId", "message_attachments.mediaAssetId")
                        })
                })
                .build(),
            None,
        )
        .await
        .expect("query the include-bearing thread shape");
    let non_empty_includes = thread_rows
        .iter()
        .filter(|(_, values)| {
            values
                .iter()
                .any(|value| matches!(value, Value::Array(items) if !items.is_empty()))
        })
        .count();
    eprintln!(
        "alice's thread include: {} rows, {non_empty_includes} carrying a non-empty array",
        thread_rows.len(),
    );
    assert!(
        non_empty_includes > 0,
        "the include-bearing subscriptions instantiated no inner rows — the profile \
         would be measuring a graph of flat scans again"
    );

    // ── device 2: bob's identical graph — the per-device marginal ──────────
    let bob_client = connect_ready_user(&server, &schema, &bob.to_string(), "users", ready).await;
    let before_bob = live_mb();
    let bob_subs = open_app_graph(&bob_client, bob, &chat_ids).await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    eprintln!(
        "phase D (bob: {} subs): live {:.1} MiB (+{:.1} per extra device)",
        bob_subs.len(),
        live_mb(),
        live_mb() - before_bob
    );
    report_include_census("phase D", live_mb() - before_bob);

    let tier_scans_after_subs =
        jazz_tools::row_histories::QUERY_TIER_READ_HISTORY_SCANS.load(Ordering::Relaxed);
    let provenance_scans_after_subs =
        jazz_tools::row_histories::QUERY_PROVENANCE_HISTORY_SCANS.load(Ordering::Relaxed);
    eprintln!(
        "phase C/D query-serving scan tripwires: tier-read {} provenance {}",
        tier_scans_after_subs - tier_scans_before_subs,
        provenance_scans_after_subs - provenance_scans_before_subs,
    );
    assert_eq!(
        tier_scans_after_subs, tier_scans_before_subs,
        "tier-gated query reads walked row history during the subscription phases"
    );
    assert_eq!(
        provenance_scans_after_subs, provenance_scans_before_subs,
        "provenance lookups walked row history during the subscription phases"
    );

    // ── phase D2: steady-state write cost with the include graph live ───────
    //
    // Phases A-E measure RESIDENCY. This one measures CHURN per write, which
    // is the axis the v13-2 include regression lived on: it left live bytes
    // flat and burned CPU re-walking cached subgraph instances. Two write
    // shapes, both taken from the live app, on the same settled graph:
    //
    //   * a presence heartbeat on the hot `users` row — the ten-second tick
    //     every device emits, and a table NO include in this graph reads. Its
    //     cost must be independent of how many include instances are live.
    //   * a new `media_asset_variants` row — one row into the innermost table
    //     of the nested thread include, the shape that actually has to reach
    //     one instance.
    //
    // Reported, not asserted: the numbers are the point, and a residency
    // profile is the wrong place to pin a throughput budget. The hard gate on
    // this axis is `tests/include_instance_flatness.rs`.
    let probe_writes = 20;
    let hot_asset = *attachment_asset_ids
        .first()
        .expect("seed produced at least one attachment asset");
    eprintln!(
        "phase D2 probe: {} messages / {} attachment assets seeded, {} live subs per device",
        message_ids.len(),
        attachment_asset_ids.len(),
        alice_subs.len(),
    );

    let heartbeat_before = total_allocated();
    let settle_before = jazz_tools::query_manager::settle_cost::SettleCounts::snapshot();
    let mut last_probe = None;
    for i in 0..probe_writes {
        last_probe = Some(
            writer
                .update(
                    alice,
                    vec![
                        (
                            "onlineTimeMs".to_string(),
                            Value::Integer(1_000_000 + i as i32),
                        ),
                        (
                            "onlineTimeUpdatedAtMs".to_string(),
                            Value::Timestamp(9_000_000 + i as u64),
                        ),
                    ],
                )
                .expect("probe heartbeat"),
        );
    }
    writer
        .wait_for_batch(
            last_probe.expect("probe heartbeats"),
            DurabilityTier::EdgeServer,
        )
        .await
        .expect("probe heartbeats durable");
    tokio::time::sleep(Duration::from_secs(2)).await;
    let heartbeat_bytes = total_allocated() - heartbeat_before;

    let variant_before = total_allocated();
    for i in 0..probe_writes {
        insert_filled(
            &writer,
            &schema,
            "media_asset_variants",
            vec![
                ("mediaAssetId".into(), Value::Uuid(hot_asset)),
                ("createdAtMs".into(), Value::Timestamp(9_000_000 + i as u64)),
            ],
        );
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    let variant_bytes = total_allocated() - variant_before;

    eprintln!(
        "phase D2 per-write churn: users heartbeat {} B/write (no include reads `users`), \
         nested-include inner row {} B/write; live {:.1} MiB",
        heartbeat_bytes / probe_writes as u64,
        variant_bytes / probe_writes as u64,
        live_mb(),
    );
    eprintln!(
        "phase D2 scan tripwires: tier-read {} provenance {}",
        jazz_tools::row_histories::QUERY_TIER_READ_HISTORY_SCANS.load(Ordering::Relaxed)
            - tier_scans_after_subs,
        jazz_tools::row_histories::QUERY_PROVENANCE_HISTORY_SCANS.load(Ordering::Relaxed)
            - provenance_scans_after_subs,
    );

    // Evaluations against the instance population that produced them — the
    // ratio the production `settle_cost` line could not report before the
    // `live_instances` gauge existed. Neither number bounds the other in
    // general: a clean instance is skipped (evals < instances) and an instance
    // touched by both `process_with_context` and `reevaluate_all` in one pass
    // is counted twice (evals > instances). Reported, never asserted: this
    // process runs a server and three clients on shared counters.
    let settle =
        jazz_tools::query_manager::settle_cost::SettleCounts::snapshot().since(settle_before);
    let probe_total = 2 * probe_writes as u64;
    eprintln!(
        "phase D2 include accounting: {} instance evals over {probe_total} writes ({:.0} per \
         write) against a standing population of {} live instances in {} include nodes; \
         {} instantiations, {} plan compiles",
        settle.instance_evals,
        settle.instance_evals as f64 / probe_total as f64,
        settle.live_instances,
        settle.live_instance_nodes,
        settle.subquery_instantiations,
        settle.plan_compiles,
    );

    // ── teardown: drop both devices, sweep, must release ───────────────────
    drop(alice_subs);
    drop(bob_subs);
    drop(alice_client);
    drop(bob_client);
    tokio::time::sleep(Duration::from_secs(2)).await;
    server.set_client_ttl(Duration::ZERO).await;
    let reaped = server.run_sweep_once().await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    eprintln!(
        "phase E (dropped + reaped {}): live {:.1} MiB",
        reaped.len(),
        live_mb()
    );

    writer.shutdown().await.expect("shutdown writer");
    server.shutdown().await;

    // The batchId->rows index must serve every lookup; the last-resort
    // full-store history scan firing even once here means the index is broken
    // for exactly the write pattern this profile exercises — and it would
    // dominate every cost the fast paths remove.
    assert_eq!(
        jazz_tools::runtime_core::LOCAL_BATCH_FULL_SCANS.load(Ordering::Relaxed),
        0,
        "last-resort full-store batch scan fired during the profile"
    );
}
