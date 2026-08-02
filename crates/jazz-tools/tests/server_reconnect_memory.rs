//! Memory budget for server-side session reconnect cycles.
//!
//! Field failure (2026-08-02, linsa): the standalone sync server ballooned
//! ~900 MB of anonymous memory PER app relaunch of a single device (fresh
//! client_id each launch, catch-up over a 129 MB store), hitting the 3 GiB
//! container limit after three relaunches and dying. `docker stats` cycles:
//! 979 → 1874 → 2767 → 2957 MB across consecutive simulator relaunches.
//!
//! Measured composition of that balloon (this test reproduces both terms):
//!
//! 1. TRANSIENT: a catch-up materializes the full subscription payload in
//!    the per-connection dispatch queue plus encode/decode copies — ~2.8×
//!    the payload live at once (blob-heavy stores make this hundreds of MB).
//!    In production those bursts become allocator-retained RSS between
//!    relaunches. Real fix is windowed snapshot delivery; until then the
//!    peak budget guards against the multiplier growing.
//! 2. RETAINED: every abandoned session (fresh client_id per app launch —
//!    the old one never resumes) parks its subscription graphs and sync
//!    bookkeeping (+10 MiB/session here, store-scale in the field) until
//!    the TTL sweep reaps it. `remove_client` does free everything — the
//!    sweep assertions pin that guarantee, and `--client-ttl-secs` exists
//!    so relaunch-heavy deployments keep the parking window short.
//!
//! The schema is deliberately fat (any stray full-schema clone would be
//! loudly visible), select policies are member-gated + INHERITS like linsa's
//! chat permissions, and the reader connects → catch-up-subscribes →
//! vanishes N times like a relaunched device.

#![cfg(feature = "test")]

mod support;

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures::StreamExt as _;
use jazz_tools::object::ObjectId;
use jazz_tools::query_manager::policy::Operation;
use jazz_tools::query_manager::types::{permissions, policy_expr as pe};
use jazz_tools::row_input;
use jazz_tools::server::JazzServer;
use jazz_tools::sync_manager::DurabilityTier;
use jazz_tools::{ColumnType, QueryBuilder, SchemaBuilder, TableSchema, Value};
use support::{connect_ready_client, connect_ready_user};

// ── tracking allocator: live bytes + high-water mark ───────────────────────

struct TrackingAllocator;

static LIVE_BYTES: AtomicU64 = AtomicU64::new(0);
static PEAK_BYTES: AtomicU64 = AtomicU64::new(0);

fn bump_live(size: u64) {
    let live = LIVE_BYTES.fetch_add(size, Ordering::Relaxed) + size;
    PEAK_BYTES.fetch_max(live, Ordering::Relaxed);
}

fn drop_live(size: u64) {
    LIVE_BYTES.fetch_sub(size, Ordering::Relaxed);
}

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            bump_live(layout.size() as u64);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        drop_live(layout.size() as u64);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            drop_live(layout.size() as u64);
            bump_live(new_size as u64);
        }
        new_ptr
    }
}

#[global_allocator]
static GLOBAL: TrackingAllocator = TrackingAllocator;

fn live_now() -> u64 {
    LIVE_BYTES.load(Ordering::Relaxed)
}

/// Reset the high-water mark to the current live level.
fn reset_peak() {
    PEAK_BYTES.store(live_now(), Ordering::Relaxed);
}

fn peak_now() -> u64 {
    PEAK_BYTES.load(Ordering::Relaxed)
}

const MIB: u64 = 1 << 20;

// ── fat linsa-like schema ───────────────────────────────────────────────────

/// Padding tables sized so one full schema clone is megabytes of small
/// allocations — cheap to build once, ruinous when cloned per policy graph.
const PAD_TABLES: usize = 120;
const PAD_COLUMNS: usize = 32;

const CHATS: usize = 20;
const MESSAGES_PER_CHAT: usize = 20;
/// Media blobs: chunked file parts like linsa's blob-in-jazz media pipeline.
/// 640 × 64 KiB ≈ 40 MiB of payload — the field store was 129 MB, mostly
/// file_parts, and the whole store re-streams on every fresh client_id.
const FILE_PARTS_PER_CHAT: usize = 32;
const FILE_PART_BYTES: usize = 64 * 1024;
const RECONNECT_CYCLES: usize = 4;

/// Chat visibility: session user must be listed in the chat's `members`
/// array. Message visibility: INHERITS the parent chat's SELECT policy via
/// the `chat_id` FK — the shape the passing `inherited_policies` suite uses,
/// and the one that drives `PolicyGraph::for_inherits` per row server-side.
fn linsa_like_schema() -> jazz_tools::Schema {
    let mut builder = SchemaBuilder::new()
        .table(
            TableSchema::builder("chats")
                .column("name", ColumnType::Text)
                .column(
                    "members",
                    ColumnType::Array {
                        element: Box::new(ColumnType::Text),
                    },
                )
                .policies(permissions(|p| {
                    p.allow_insert().always();
                    p.allow_update().always();
                    p.allow_read()
                        .where_(pe::contains("members", pe::session("user_id")));
                })),
        )
        .table(
            TableSchema::builder("messages")
                .nullable_fk_column("chat_id", "chats")
                .column("body", ColumnType::Text)
                .column("created_at", ColumnType::Integer)
                .policies(permissions(|p| {
                    p.allow_insert().always();
                    p.allow_read().where_(pe::all_of([
                        pe::is_not_null("chat_id"),
                        pe::allowed_to(Operation::Select, "chat_id"),
                    ]));
                })),
        )
        .table(
            TableSchema::builder("file_parts")
                .nullable_fk_column("chat_id", "chats")
                .column("part_index", ColumnType::Integer)
                .column("data", ColumnType::Bytea)
                .policies(permissions(|p| {
                    p.allow_insert().always();
                    p.allow_read().where_(pe::all_of([
                        pe::is_not_null("chat_id"),
                        pe::allowed_to(Operation::Select, "chat_id"),
                    ]));
                })),
        );
    for table_index in 0..PAD_TABLES {
        let mut table = TableSchema::builder(&format!(
            "padding_table_with_a_deliberately_long_name_{table_index:03}"
        ));
        for column_index in 0..PAD_COLUMNS {
            table = table.column(
                &format!("padding_column_with_a_deliberately_long_name_{column_index:03}"),
                ColumnType::Text,
            );
        }
        table = table.policies(permissions(|p| {
            p.allow_insert().always();
            p.allow_read().always();
        }));
        builder = builder.table(table);
    }
    builder.build()
}

// ── budgets ─────────────────────────────────────────────────────────────────

/// Peak live-bytes delta allowed within one reconnect cycle (connect,
/// catch-up over ~40 MiB of policy-gated rows and blob parts, disconnect).
///
/// Calibration on this suite's rig (2026-08-02): +112 MiB measured — the
/// catch-up currently materializes the full subscription payload in the
/// per-connection queue plus encode/decode copies (~2.8× payload). The
/// budget guards against regressions (extra copies, encoding blow-ups);
/// ratchet it down when windowed snapshot delivery lands.
const CYCLE_PEAK_BUDGET_BYTES: u64 = 160 * MIB;

/// Live-bytes growth allowed between the end of cycle 1 and the end of the
/// last cycle. Sessions park in `disconnect_candidates` for the client TTL,
/// so some per-session state legitimately lingers (+10 MiB/session measured)
/// — but it must not be hundreds of megabytes per relaunch.
const RETAINED_GROWTH_BUDGET_BYTES: u64 = 64 * MIB;

/// Live bytes the server may still hold over the single-session baseline
/// after ALL abandoned sessions were reaped. This is the "reap actually
/// frees" guarantee the production TTL sweep relies on.
const RETAINED_RESIDUAL_AFTER_SWEEP_BUDGET_BYTES: u64 = 8 * MIB;

// ── the test ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn session_reconnect_cycles_stay_within_memory_budget() {
    let schema = linsa_like_schema();
    let server = JazzServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;

    let ready_timeout = Duration::from_secs(30);
    let writer = connect_ready_client(&server, &schema, "writer", "chats", ready_timeout).await;

    // Seed: chats, reader memberships, messages — all before the first reader
    // connects, so every reconnect cycle performs a pure catch-up like a
    // relaunched device.
    let mut chat_ids: BTreeSet<ObjectId> = BTreeSet::new();
    let mut last_batch = None;
    for chat_index in 0..CHATS {
        let (chat_id, _, batch) = writer
            .insert(
                "chats",
                row_input!(
                    "name" => format!("chat {chat_index}"),
                    "members" => Value::Array(vec!["reader".into(), "writer".into()]),
                ),
            )
            .expect("insert chat");
        last_batch = Some(batch);
        for message_index in 0..MESSAGES_PER_CHAT {
            let (_, _, message_batch) = writer
                .insert(
                    "messages",
                    row_input!(
                        "chat_id" => Value::Uuid(chat_id),
                        "body" => format!("message {message_index} in chat {chat_index}"),
                        "created_at" => message_index as i32,
                    ),
                )
                .expect("insert message");
            last_batch = Some(message_batch);
        }
        for part_index in 0..FILE_PARTS_PER_CHAT {
            let payload = vec![(part_index % 251) as u8; FILE_PART_BYTES];
            let (_, _, part_batch) = writer
                .insert(
                    "file_parts",
                    row_input!(
                        "chat_id" => Value::Uuid(chat_id),
                        "part_index" => part_index as i32,
                        "data" => Value::Bytea(payload),
                    ),
                )
                .expect("insert file part");
            last_batch = Some(part_batch);
        }
        chat_ids.insert(chat_id);
    }
    writer
        .wait_for_batch(
            last_batch.expect("seeded batches"),
            DurabilityTier::EdgeServer,
        )
        .await
        .expect("seed reaches the server");

    // ── measured phase: reconnect cycles ────────────────────────────────────
    let mut cycle_end_live: Vec<u64> = Vec::new();
    let mut cycle_peaks: Vec<u64> = Vec::new();

    for cycle in 0..RECONNECT_CYCLES {
        let live_before = live_now();
        reset_peak();

        // Fresh client per cycle — like an app relaunch minting a new
        // client_id — with a catch-up include-tree subscription.
        let reader = connect_ready_user(&server, &schema, "reader", "chats", ready_timeout).await;
        eprintln!(
            "cycle {cycle} post-connect: peak +{} MiB live +{} MiB",
            peak_now().saturating_sub(live_before) / MIB,
            live_now().saturating_sub(live_before) / MIB,
        );

        let include_query = QueryBuilder::new("chats")
            .with_array("messages", |sub| {
                sub.from("messages").correlate("chat_id", "chats.id")
            })
            .build();
        let mut sub = reader
            .subscribe(include_query)
            .await
            .expect("subscribe chats include tree");

        // Media pull — the app's media cache subscribes to blob parts.
        let parts_query = QueryBuilder::new("file_parts").build();
        let mut parts_sub = reader
            .subscribe(parts_query)
            .await
            .expect("subscribe file parts");

        eprintln!(
            "cycle {cycle} post-subscribe: peak +{} MiB live +{} MiB",
            peak_now().saturating_sub(live_before) / MIB,
            live_now().saturating_sub(live_before) / MIB,
        );

        let expected_parts = CHATS * FILE_PARTS_PER_CHAT;
        let mut seen: BTreeSet<ObjectId> = BTreeSet::new();
        let mut seen_parts: BTreeSet<ObjectId> = BTreeSet::new();
        let mut next_report = expected_parts / 4;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        while !chat_ids.is_subset(&seen) || seen_parts.len() < expected_parts {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            assert!(
                !remaining.is_zero(),
                "cycle {cycle}: catch-up never completed \
                 (chats {} of {}, parts {} of {expected_parts})",
                seen.len(),
                chat_ids.len(),
                seen_parts.len(),
            );
            tokio::select! {
                delta = sub.next() => {
                    let delta = delta.unwrap_or_else(|| {
                        panic!("cycle {cycle}: chats stream closed early")
                    });
                    for added in &delta.added {
                        seen.insert(added.id);
                    }
                }
                delta = parts_sub.next() => {
                    let delta = delta.unwrap_or_else(|| {
                        panic!("cycle {cycle}: parts stream closed early")
                    });
                    for added in &delta.added {
                        seen_parts.insert(added.id);
                    }
                    if seen_parts.len() >= next_report {
                        eprintln!(
                            "cycle {cycle} drain {}/{expected_parts} parts: \
                             peak +{} MiB live +{} MiB",
                            seen_parts.len(),
                            peak_now().saturating_sub(live_before) / MIB,
                            live_now().saturating_sub(live_before) / MIB,
                        );
                        next_report += expected_parts / 4;
                    }
                }
                _ = tokio::time::sleep(remaining) => {}
            }
        }

        // Abrupt disconnect — a relaunched app never says goodbye. The server
        // parks the session as a disconnect candidate for the client TTL,
        // exactly like the field scenario (fresh client_id per relaunch, old
        // session still resident).
        drop(sub);
        drop(parts_sub);
        drop(reader);
        // Let disconnect processing and deferred drops settle.
        tokio::time::sleep(Duration::from_millis(500)).await;

        let live_after = live_now();
        let peak_delta = peak_now().saturating_sub(live_before);
        let retained_delta = live_after.saturating_sub(live_before);
        eprintln!(
            "cycle {cycle}: peak +{} MiB, retained +{} MiB (live {} MiB)",
            peak_delta / MIB,
            retained_delta / MIB,
            live_after / MIB,
        );
        cycle_peaks.push(peak_delta);
        cycle_end_live.push(live_after);
    }

    let worst_peak = cycle_peaks.iter().copied().max().unwrap_or(0);
    let retained_growth = cycle_end_live
        .last()
        .copied()
        .unwrap_or(0)
        .saturating_sub(cycle_end_live.first().copied().unwrap_or(0));

    eprintln!(
        "worst cycle peak +{} MiB, retained growth cycle1→cycle{} +{} MiB",
        worst_peak / MIB,
        RECONNECT_CYCLES,
        retained_growth / MIB,
    );

    // ── reap phase: dead sessions must actually release their memory ───────
    // Every cycle abandoned its session; with TTL zero one sweep must reap
    // them all and return the parked per-session state (subscription graphs,
    // schema contexts, outbox remnants).
    let live_before_sweep = live_now();
    server.set_client_ttl(Duration::ZERO).await;
    let reaped = server.run_sweep_once().await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let live_after_sweep = live_now();
    let released = live_before_sweep.saturating_sub(live_after_sweep);
    eprintln!(
        "sweep reaped {} clients, released {} MiB (live {} MiB)",
        reaped.len(),
        released / MIB,
        live_after_sweep / MIB,
    );
    assert!(
        reaped.len() >= RECONNECT_CYCLES - 1,
        "sweep reaped only {} of {} abandoned sessions",
        reaped.len(),
        RECONNECT_CYCLES,
    );
    // The reap must return at least the cross-cycle growth: after it, live
    // may exceed the first cycle's end by only a small slack.
    let residual = live_after_sweep.saturating_sub(cycle_end_live.first().copied().unwrap_or(0));
    assert!(
        residual < RETAINED_RESIDUAL_AFTER_SWEEP_BUDGET_BYTES,
        "after reaping all abandoned sessions the server still holds +{} MiB \
         over the single-session baseline — per-session state leaks past \
         remove_client (budget {} MiB)",
        residual / MIB,
        RETAINED_RESIDUAL_AFTER_SWEEP_BUDGET_BYTES / MIB,
    );

    assert!(
        worst_peak < CYCLE_PEAK_BUDGET_BYTES,
        "a reconnect cycle peaked +{} MiB of live allocations — the policy \
         evaluation path is cloning the schema per policy graph again \
         (budget {} MiB)",
        worst_peak / MIB,
        CYCLE_PEAK_BUDGET_BYTES / MIB,
    );
    assert!(
        retained_growth < RETAINED_GROWTH_BUDGET_BYTES,
        "server retained +{} MiB across {} reconnect cycles of the same \
         device — per-session state is not being released (budget {} MiB)",
        retained_growth / MIB,
        RECONNECT_CYCLES,
        RETAINED_GROWTH_BUDGET_BYTES / MIB,
    );

    writer.shutdown().await.expect("shutdown writer");
    server.shutdown().await;
}
