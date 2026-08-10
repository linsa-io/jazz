# Defects found in jazz while running it in production

We run a local-first messenger on a fork of `garden-co/jazz` (`linsa-io/jazz`, branch
`linsa`), diverged at `e84d84a6` on 2026-07-27. Everything below is a defect in **upstream
code**, found by running it under a real workload, with a fix and a measurement. Fixes to
code we added ourselves are deliberately excluded — this is not a changelog of our fork.

Every entry gives what goes wrong, the fix, the fork commit, what covers it, and a number.

## Track record

Two defects were reported before the fork and closed by the jazz team quickly. This
document is the next batch, all found after `e84d84a6`, so none of it overlaps.

| issue                                                  | filed      | closed     | what                                                                                                                          |
| ------------------------------------------------------ | ---------- | ---------- | ----------------------------------------------------------------------------------------------------------------------------- |
| [#1081](https://github.com/garden-co/jazz/issues/1081) | 2026-07-11 | 2026-07-21 | `local_batch_rows` falling through to `scan_history_row_batches` — multi-second reconnect freeze from author-side row history |
| [#1120](https://github.com/garden-co/jazz/issues/1120) | 2026-07-22 | 2026-07-24 | one `.include()` makes every subsequent write pay a full non-incremental re-settle (85× write cost, ~O(rows²), clean DB)      |

---

## 1. A forced resend omits the row's metadata, and the peer discards what it cannot locate

**Severity: data loss.** A row written while a peer is offline never reaches it, and no
relaunch heals it — only a server restart, a store wipe, or a TTL reap.

`include_metadata` is derived from the per-client `sent_metadata` set
(`sync_manager/sync_logic.rs`), and `queue_row_to_client` inserts into that set when the row
is **enqueued**. During an offline window the enqueue happens, and the payload is then
dropped at the stream layer for a client with no connection — `prepare_payload`
(`server/mod.rs`) returns an empty vec and the result is discarded. The bookkeeping is never
rolled back.

On reconnect the row is re-queued with `force_resend`, which correctly bypasses the delivery
gate — but `include_metadata` still trusts the poisoned set, so the row ships with
`metadata: None`. The peer cannot resolve its table, has no locator to fall back on, and
discards it at `row_metadata_from_payload` — **silently**, with no log line between "sent"
and "missing".

That one set explains all three ways it heals: a reap drops `sent_metadata` with the rest of
`ClientState`, a store wipe changes the client id, and a restart loses it with the process.

```rust
let include_metadata = force_resend || !client.sent_metadata.contains(&object_id);
```

Fix `b32db09c`. Covered by `tests/offline_reap_delivery.rs` (a reproduction plus a
reaped-peer gate) and `tests/offline_delivery_edges.rs` — six cases including a long gap
(**25 of 26 rows lost** before the fix), both subscriptions of a returning peer, and
exactly-once delivery. The silent discard is now logged.

Two conditions must hold together, which is why it hides: the peer returns with the same
wire client id (persisted with its store, so only a reinstall changes it), and it is not
reaped — so a short `--client-ttl-secs` accidentally masks it.

## 2. Per-peer delivery tracking grows without bound

`sent_batch_ids` gained one id per delivered batch for as long as a row stayed in scope:
~16 B per batch per peer, so a 12k-batch presence row cost **~188 KiB per connected peer**,
indefinitely.

`record_delivery` now inserts the delivered id and drops its direct parents — a
domination-only frontier cursor. Ids are only ever forgotten, never invented, so pruning can
only under-claim, and a dedup miss merely re-sends a batch the receiver already holds and
applies idempotently. The `ParentNotFound` over-claim class is structurally impossible.

Fix `20580ad3`. Pinned by an ancestor-DFS termination test: **zero storage loads after a
512-deep chain**.

## 3. A subquery recompiles its graph for every outer row

`ArraySubqueryNode` compiles a fresh `QueryGraph` per outer row, and `reevaluate_all()`
re-evaluates every instance whenever the inner table changes.
`SubgraphTemplate::instantiate` documents this as the "recompile per binding approach for
simplicity".

Measured against a real store with a chat client and a backend attached: **one incoming row
update produced ~147 full `try_compile_with_schema_context` calls — ~186/s on an otherwise
idle server, with the process pinned at 98% CPU.** A symbolized profile put `graph::compile`
and `hash_row_descriptor` above execution and row decoding.

Keeping the settled `SubgraphInstance` per (outer row, element index) and re-settling it is
sound: the compiled shape is identical across re-evaluations, only the correlation binding
differs, and results are read from `current_output_tuples()` — full state, not a delta.

Fix `8a5730f4`. **Compilations to zero, CPU to single digits.**

## 4. Include dirtiness is broadcast to every instance

A write marked every cached subquery instance, so one write cost O(live outer rows). On the
live server this **doubled cold-start CPU** and turned a ten-second presence heartbeat into
a CPU burst; `Vec<TupleElement>::clone` under that collect was worth **−12.4%** alone.

Two changes: buffer the marks and apply them when an instance is next evaluated
(`98eca871`), and route dirt by correlation value through two indices instead of
broadcasting (`14ad429c`). Reads that went **204 → 4004 (19.6×) become 8 → 8**; bytes that
went 20.2× become flat.

Both bugs escaped every existing gate because those varied history depth only. The new gate
varies instance count, and marking is pinned flat: **1.03× at 1000 instances vs 50**.

## 5. Every write-tick rescans the full index of each dirty table

Per subscription scan node, per dirty table, per tick — then diffs against the previous
membership, so write cost scaled with index size rather than with what changed.

Fix `b33832e1`: incremental scans from row-precise dirty marks. Covered by a randomized
differential test (300-step insert/update/delete against a model, filtered and unfiltered
live subscriptions) and an interleaved full-dirty handoff test.

## 6. Blob payloads cross both language bridges one byte at a time

The most expensive defect we found, and it is in the transport on **both** bindings.

`ValueHuman::Bytea` is a plain `Vec<u8>` with no `serde_bytes`
(`query_manager/types/value.rs`), so `serde_json` renders a megabyte as an array of a
million `Number`s.

On **node**, napi then sets that array into JS one element at a time — roughly two million
FFI calls per MiB — for query results, for subscription deltas, and for the row `insert`
echoes, although JS supplied those bytes microseconds earlier. On **React Native**, the
delta goes out through `serde_json::to_string`, so the same megabyte becomes **3,743,770
characters of JSON text** that the device parses into a million-element array and then walks
once per byte to rebuild a `Uint8Array`.

Measured on the node binding with no server and no network in the loop, 16 MiB:

|          | before              | after                   |
| -------- | ------------------- | ----------------------- |
| write    | 1645 ms (9.7 MiB/s) | **247 ms (64.9 MiB/s)** |
| read     | 1604 ms (10 MiB/s)  | **5 ms (3100 MiB/s)**   |
| user CPU | 3138 ms             | **252 ms**              |

User CPU was 97% of wall time with no server involved — the boundary was the entire cost.

Fixes: `Bytea` crosses as a `Buffer` via `BufferSlice::from_data`, a zero-copy hand-off whose
finalizer frees through Rust's allocator (`9d669b14` for query results and write echoes,
`cca31eab` for subscription deltas). On RN, blobs travel beside the JSON as a sidecar with
`{"type":"BlobRef","value":<idx>}` in their place (`4f8b6147`) — the read-direction twin of
the write-side transport we added earlier (`5859e680`, which measured **3.57 characters per
byte** for the hex-in-JSON encoding it replaced). The JS decoder needed no change: it already
returns an incoming `Uint8Array` untouched and only falls back to a per-byte loop for arrays.

Also `088e07a5`: the JS hex encoder allocated two strings per byte and measured **~3.7×
slower on device** for megabyte payloads.

Gates: a shape assertion that `insert`, `query` and deltas never hand back a JS array (it
asserts a shape, not a duration, so it cannot flake); a size gate on the RN delta that keeps
both inline forms measured so the gap stays visible; and `ffi-value.test.ts` over the full
byte range, chunk-seam sizes and a 1 MiB round-trip.

## 7. Schema and descriptor structures are cloned through subquery compilation

Compilation cloned the `Schema` and `SchemaContext` per subquery, and descriptor column lists
per node.

Fixes `afd4f9f7` and `6ad129f8` share them via `Arc`. **462 MiB → 324 MiB** on the amplified
rig, with an integration test pinning that recursive nodes hold the same shared handles.

## 8. Row history rebuilds from scratch on a serial write

A visible batch whose parent set equals the entire previous branch frontier dominates every
old tip, so the next entry is derivable from the previous entry plus the incoming row — but
the path took `load_branch_history` and a full rebuild regardless.

Fix `826dcaae` adds an O(1) serial-write fast path with five guards, any miss falling back to
the full path, and a runtime kill switch. Companion `81f65bde` does the same for batch-state
patches and re-applies.

**`linsa_schema_profile HEARTBEATS=12000` (RocksDB, debug): 3995s → 34s.** At depth 4000 in
release: tip tier-confirmation **786 µs → 14 µs**, stage+publish 881 → 541 µs/op.

Byte-exactness is proved by a randomized differential oracle running dual-mode — fast path on
and off — per backend, plus seven fixtures pinning tier-sparse chains, tier-hole divergence,
frontier coverage and the decline cases.

---

## 9. Every transactional write reads the whole store to build a payload nobody reads

**Severity: the app freezes on every write once the store holds large rows.**

`RuntimeCore::sealed_batch_submission` (`runtime_core/writes.rs:459`) calls
`Storage::capture_family_visible_frontier` for every `Transactional` batch, and that helper
(`storage/storage_trait.rs:1271`) scans every visible raw table with an empty prefix and
decodes every row it finds. The result is compatibility payload: PR #920 removed the
validation that consumed it — transactional conflicts are decided from the staged rows' own
parents in `SyncManager::validate_transactional_parent_frontiers`
(`sync_manager/inbox.rs:151`), one targeted lookup per row — and left the capture in place,
commented for removal "with the next storage-format break". Nothing has read it since.

While rows are small this is invisible. Our messenger stores file attachments as 1 MiB rows,
and the cost became the dominant term the moment a user sent a file.

Measured on a device store of 741 rows, of which 780 raw values exceed 500 KB (768.3 MB of
1 MiB attachment parts, 17.4 MB everything else): **every settle pass read 768.38 MB in 2207
operations** — every blob row, both its visible and its history copy, exactly once, byte for
byte, pass after pass. 148 of 469 settle passes in one minute of ordinary use, ~1000 ms each,
~113 GB read in that minute. This defect accounts for the visible half, 384.2 MB; defect 10
accounts for the other.

**Reproduced on pristine upstream.** The gate below was checked out into a clean
`origin/main` worktree at `e84d84a6` — not one line of this fork — and fails there
identically: "sealing one row carried a frontier of 25 members with 25 unrelated rows in the
store". So the behaviour is upstream's, not something our changes induce. One honest limit:
the trigger is `BatchMode::Transactional`, which the APPLICATION chooses. An app that never
writes transactionally never pays this. Ours does, on every message.

The fix removes the capture; the field stays on the wire and in storage, empty. The gate is
`runtime_core/tests/sealed_batch_cost.rs`, which asserts SHAPE rather than duration or bytes —
the captured frontier must be bounded by the batch, never by the store. It fails on the
unfixed code with "sealing one row carried a frontier of 25 members with 25 unrelated rows in
the store". Shape rather than bytes because `MemoryStorage` overrides the capture with an
in-memory walk that costs nothing to traverse: a bytes-read assertion passes there while the
real backends bleed.

One pre-existing test, `rc_missing_batch_fate_retransmits_original_captured_frontier`, pinned
the old behaviour. Its real contract is that a retransmission replays what was sealed instead
of re-deriving it; with an empty frontier that would pass vacuously, so it now seeds an
old-format submission itself and demands it back verbatim.

**After both fixes, on the same device and the same scenario: settle passes over 100 MB went
148 of 469 → 0, passes over 500 ms 148 → 0, worst pass 1000 ms / 768 MB → 178 ms / 0.27 MB.**

## 10. An index scan reads — and can admit — rows belonging to other tables

**Severity: cost proportional to unrelated data, plus a correctness hazard.**

`IndexScanNode::apply_local_overlay_rows` (`query_manager/graph_nodes/index_scan.rs:257`)
walks `QueryManager::pending_local_row_batches` and loads each entry's full row bytes. That
map is process-global and table-blind (`query_manager/manager.rs:541`), the loop filters only
on branch, and the resolver it calls ignores the table name it is handed —
`load_history_row_batch_row_bytes_with_storage` takes `_table` and addresses purely by row id
(`storage/mod.rs:2133`). So a scan over one table reads the megabyte payloads of rows in
another, and where the condition happens to match, `new_ids.insert(row_id)` admits a foreign
row into that index's id set.

The map also gates the cheap path: while it is non-empty, `IndexScanNode` takes the full
rescan branch for every dirty settle in every qualifying subscription
(`index_scan.rs:344-353`). And it drains only when a NON-local update for the same object
arrives with `confirmed_tier == GlobalServer` (`manager.rs:1930`), so rows written locally and
never echoed back that way pin it for the life of the process. That is why restarting the app
"fixes" the freeze while the data on disk is untouched — the map lives only in memory. It is
also why the defect is asymmetric between peers: the device that UPLOADS accumulates the
entries; the device that downloads receives the same rows as non-local updates and does not.

Measured: ~390 unconfirmed 1 MiB parts in the map, **384.2 MB read per settle pass**, on a
subscription that loaded 7 rows and emitted none.

**Evidence is weaker here than for defect 9, and stated as such.** All three functions
involved — `apply_local_overlay_rows`, `load_history_row_batch_row_bytes_with_storage` and
`common_case_exact_history_row_table_locator` — are byte-identical to `origin/main`
(`e84d84a6`), verified by diffing them function by function. But we have NOT reproduced it
on a pristine upstream tree: populating the overlay needs a live scenario with an
unconfirmed local write, which a unit test does not reach. What can be said without a run:
the incremental fast path that gates this walk (`index_scan.rs:344-353`) exists only in this
fork — upstream always takes the full-scan branch — so upstream cannot be less exposed than
we are.

The fix skips overlay entries whose row locator names a different table than the one being
scanned, keeping the resolver's fallback for rows that have no locator yet. Fixing the shared
resolver instead was considered and rejected: its table parameter is ignored deliberately and
it serves many callers, so a strict check there risks silently dropping rows — the failure
mode this document already contains two entries about.

**After the fix, with the overlay demonstrably populated (peak 73 entries), reads per overlay
entry went from ~2 MB to 2.8 KB.**

---

## 11. A seal whose rows died in transit loops the authority into full-store scans

Two-phase uploads have no recovery path when the row payloads are lost on a dying
connection but the `SealBatch` survives onto the next one. The authority persists the
sealed submission, then `try_accept_completed_sealed_batch_from_client` finds no declared
rows and returns **silently** — the sealer is never told anything, so it retries the seal
indefinitely. Every retry (and every later reconciliation over the orphan: reconnect
pending-set derivation, re-received rejected fates via `mark_local_batch_rows_rejected`)
lands in `local_batch_rows`, misses all four member sources, and pays the "last-resort"
full-store history scan.

Measured in production (2026-08-09, ~430 MB store): one diverged client held a core at
100% for 38+ minutes — one ~3.5 s full scan per retry, the runtime serialized behind the
`RuntimeCore` mutex the whole time (a second worker sat blocked in
`push_sync_inbox_batch` from the websocket handler, so no other client could even park a
message). The store had accumulated **609** such orphan submissions in three days of
ordinary mobile traffic; a restart sweep walked them all, one full scan each. Captured
end-to-end with gdb: `apply_received_batch_fate → mark_local_batch_rows_rejected →
local_batch_rows → scan_local_batch_rows → scan_history_row_batches`, reading 1 MiB
media blocks with a per-row hex branch-name decode.

The protocol gap is the defect: there is no path back from "the seal outlived
its rows". The scan amplification below is the same fallback as defect 13
being reached repeatedly; the cache we added for it is hardening, not a claim
that the fallback is wrong.

Fix shipped in this fork, two halves:

- **Answer the sealer.** The silent return now queues `BatchFate::Missing` to the sealing
  client — the existing Missing semantics ("pends retransmission") make its fate handler
  retransmit rows + seal, closing the two-phase loop. Not persisted: the fate-request
  path already synthesizes Missing for unknown batches, and a stored Missing would
  wrongly outlive the rows' arrival.
- **Never scan twice for the same void.** `local_batch_rows` keeps an in-memory set of
  batch ids whose full scan already answered "no rows"; repeats answer from it.
  Invalidated when a row batch (or seal) with that id is pushed into the inbox or a
  local write tracks the batch. After a restart the first question pays one scan and
  re-learns.

Gate: `a_seal_without_rows_must_not_loop_full_store_scans` (drops the row payload,
delivers the seal, retries it, then derives the pending set twice — asserts the Missing
answer, at most one scan, and cache reuse across derivations).

---

## 12. The browser binding still hands blobs over one byte at a time

Defect 9's napi fix (blobs as bytes rather than as an array of a million Numbers) was never
applied to `jazz-wasm`, and nothing measured it: the crate has two Rust tests and had no
TypeScript coverage of the boundary at all.

`WasmRuntime::query` and the `insert()` echo both marshal through
`serde_wasm_bindgen::Serializer` (crates/jazz-wasm/src/runtime.rs), where
`ValueHuman::Bytea` is a bare `Vec<u8>` with no `serde_bytes` — so it serialises as a JS
Array. Subscriptions escape it: on wasm32 `make_subscription_callback` goes through
`native_subscription_delta_to_js`, which packs whole encoded rows into a single
`Uint8Array`.

The surviving cost therefore lands on the WRITE path, which is exactly where a browser app
meets it: a chunked upload calls `insert()` once per 1 MiB part and every call echoes that
megabyte back as an array. Measured at the binding, 16 rows of 1 MiB: **write 8.3 MB/s,
read 29.7 MB/s**. Measured independently from the application on top of it (a Vue client
writing `file_parts`): **8.0-8.5 MB/s upload, flat from 2 MB to the 100 MB cap** — the same
number from both sides of the boundary, and ~12 s of uninterrupted main-thread work for a
100 MB attachment.

Gates: `packages/jazz-tools/src/runtime/blob-marshalling.wasm.test.ts`. The two crossings
that still marshal per byte are `it.fails`, so they pass while the defect stands and fail
the moment the Rust side starts handing bytes; the subscription case is an ordinary gate
that keeps the fast path fast. Both bindings now share `testing/blob-fixtures.ts`, so each
boundary is asked the identical question.

---

## 13. A policy denial reaches a fallback that cannot succeed for it

**Not a claim that the fallback is wrong.** `local_batch_rows`'s full-store
scan is deliberate and documented as a last resort for a batch whose
`batchId->rows` index was lost. The claim here is narrower: one caller reaches
it where it can never pay off, and reaching it is peer-triggered.

`mark_local_batch_rows_rejected` resolves a rejected batch's rows through the
scanning lookup. For a write **this node did not author**, a rejection means
nothing landed here — the row was refused before storage — so the four
point-lookup sources miss and the scan then walks every table's history to
find rows that cannot exist. Checked in the incident store: **zero** of its
1049 rejected batches appear anywhere in its history, across all 1998 row
locators.

Because policy evaluates per incoming write, a peer buys one walk of the whole
store per denied write, under the `RuntimeCore` mutex, so the rest of the
runtime stops for its duration. Measured in production 2026-08-10: a client
whose chains predate this store wrote presence heartbeats onto a `users` row
the server cannot see. The server classifies such a write by
`if old_content.is_some() || !row.parents.is_empty() { Update } else { Insert }`
(`sync_manager/inbox.rs`), so the one condition — no visible old content —
produced 934 `Insert denied by policy on table users` and 114 `Update denied
by USING policy on table users - no old content`. 264 distinct batches in 26
minutes, one core pinned at 100%, sync stalled.

Fix: the rejection path uses `local_batch_rows_tracked_only` — only what this
node already tracks.

**The tradeoff, stated plainly.** The scan's one possible payoff is a batch
whose rows are in storage while _all_ its bookkeeping (sealed submission,
cached record, persisted record, row index) is gone; the rejection path can no
longer recover that case. We believe it is unreachable — bookkeeping is pruned
only at settlement, and a settled batch is terminal, so a later rejection for
it would contradict the settlement — but that is an argument, not a
measurement, and it is the thing to re-examine if a rejected local batch is
ever seen keeping visible rows.

Gate: `a_policy_rejected_write_costs_no_full_store_scan` — six policy-denied
writes, six rejected fates, zero full-store scans (6/6 before the fix).

---

## 14. Resolving a batch's ancestors copies the whole row history, twice

`history_rows_visible_before_batch` (upstream `sync_manager/inbox.rs`, from
"Fix replay idempotency for stored history rows", refactored 2026-07-13)
resolves which of a row's visible history entries precede an incoming batch.
It indexed the candidates by **cloning every one of them** into a map — though
the walk reads nothing but each candidate's `parents` — and then cloned the
selection out again, into a fresh vector. The selection is a subset of a
vector the function already owns by value, so both copies are avoidable.

Cost per incoming batch is therefore two copies of the row's whole visible
history, payloads and parent vectors included. In a linear history — the
normal shape, and what a presence row grows into — every entry is an ancestor,
so neither copy shrinks with the selection.

Measured in production 2026-08-10: a `users` row grown to 2541 history entries
by presence heartbeats, taking ~52 unappliable writes a second from one
client, put a core at 100% with a stack dump landing repeatedly in
`smallvec::grow` under this function — roughly a quarter of a million row
copies a second.

Fix: index by reference, then `retain` the caller's own vector in place. No
row is copied at all.

Gate: `resolving_ancestors_does_not_clone_the_whole_history` — a 200-entry
linear history whose every entry is an ancestor; asserts the returned vector
is the caller's own allocation (same pointer, same capacity). Verified red
against the upstream shape and green with the fix.

---

## 15. A parentless write reads the row's entire history to prove nothing

`pre_batch_visible_row` prepares the pre-batch content every incoming write is
policy-checked against. A batch with ONE parent takes a point-lookup fast path
there. A batch with NO parents does not: it falls through to
`scan_history_row_batches`, reading every version the row has.

A parentless batch has no ancestry to resolve, so that read decides exactly one
thing — whether every visible version sits on the incoming batch's branch. When
it does, `history_rows_visible_before_batch` disables its fallback and answers
`None`, and the read was spent proving that. The branch registry answers the
same question in two point lookups.

Not a corner case: the server classifies a write with no parents and no visible
old content as an insert (`sync_manager/inbox.rs`), so every write from a
client whose chain this store does not share arrives this way. Production
2026-08-10 collected 1442 `Insert denied by policy on table users`; the row
they landed on had grown to 2541 versions on presence heartbeats, so each
attempt read all 2541 with the runtime mutex held, in bursts of hundreds a
minute.

Fix: when the store has exactly one branch and it is the incoming batch's
branch, a parentless batch returns `None` without the read.

Gate: `a_parentless_write_does_not_read_the_whole_row_history` — a 40-version
row, one parentless write, asserts zero whole-history reads (one before the
fix, verified by removing it).

**Scope of the fix, stated plainly.** It covers the single-branch store. A
store with several branches still pays the read for parentless writes;
answering "which branches does THIS row span" cheaply needs a storage
primitive that does not exist yet.

---

## 16. A seal over delivered rows can never be matched, and the answer for that is unbounded

Two independent halves, and the second one is ours.

**The declaration cannot match.** `RowHistoryEntry::content_digest()` covers `parents`,
and `scope_delivery_row` clears `parents` before a row goes out to a peer. A peer that
seals rows it _received_ rather than authored therefore declares digests computed over the
stripped form, while the authority holds the full form and computes a different digest.
Both sides are behaving exactly as written; the identity is simply not the same identity on
the two sides of the wire. No retry can converge, because retrying re-derives the same two
digests.

**The answer for an unmatched declaration is an instruction.** `BatchFate::Missing` is not
advice: on the peer it drives `retransmit_local_batch_to_servers`, and
`force_row_batch_to_servers` deliberately clears the sent-metadata bookkeeping so nothing
suppresses the resend. Rows and seal come back, the seal is unmatchable again, and the
answer buys the work that produces the next question. The cycle's only limit is how fast
the authority answers.

That second half arrived with this fork's own fix for defect 11, which replaced silence
with `Missing`. Silence was worse in its own way — the sealer retried forever with no path
forward — but the replacement had no bound, and on 2026-08-10 a diverged client held a
production core at 100% with sync dead behind it. The lesson is narrow and worth stating:
an answer whose handler generates the next question needs a bound before it needs anything
else.

**The answer has two emitters, and bounding one bounds nothing.** Besides the seal that
cannot complete, `respond_to_batch_fate_request` synthesises `Missing` for any batch with
no stored fate — and the replay short-circuit queues a fate request for _every replayed
row_. So the rows a `Missing` asks for each buy another `Missing`, with no seal involved.
A first version of this fix bounded only the seal emitter; its gates passed because they
sent seals and never a replayed row.

**Fix shipped in this fork, three parts.**

- `content_digest_ignoring_parents()`, accepted **alongside** the full digest at the two
  sites that match declared members (`declared_rows_for_submission`, and the rejected-batch
  membership check in `apply_row_updated`). Delivered rows can now settle. The full digest
  is still accepted, so nothing that matched before stops matching. Membership is only the
  question of which stored rows a seal is about; whether they may commit is still answered
  from their real stored parents, gated by a test that declares a stale frontier with the
  blind digest and still expects `transaction_conflict`.
- One answer policy, `may_tell_client_a_batch_is_missing`, through which **both** emitters
  pass — keyed on the fate being `Missing` rather than on where it came from, because a
  node with an upstream can hold a _stored_ one and it drives retransmission just the same. Three limits, bounding different things: a rate limit
  (`MISSING_ANSWER_MIN_INTERVAL_MICROS`, 5 s per client per batch) is what a burst runs
  into first and is the one that makes "a peer cannot set our workrate" true; the cap and
  the grace (`MAX_MISSING_ANSWERS` / `MISSING_ANSWER_GIVE_UP_AFTER_MICROS`, aliases of the
  redelivery constants) are the give-up policy, both halves load-bearing for the reasons
  stated where they were first introduced. Silence retracts nothing — the submission stays,
  no fate is invented (a `Rejected` would destroy the peer's row and the graft tool is
  offline-only) — and the budget is connection-scoped, re-armed by the
  handshake itself, because nothing else observes a new socket: a reconnect does not always
  mint a new client (`ensure_client_with_session` updates it in place, and the server pulls
  a reconnecting client back out of the disconnect candidates rather than reaping it), and
  the session does not mark one either, since the same user presents the same session
  value.
- The replay short-circuit in `apply_row_updated` is decided _before_ the inputs to the
  check it skips are prepared, so absorbing a replay costs one history read instead of two.
  This is what the loop's traffic actually spends its time on once the answer is bounded.

**The first answer for a batch is free, and the tracking cap is what protects that.** A
genuinely interrupted upload must be repaired without first waiting out an interval, so a
fresh batch id is the cheapest thing a peer can buy an answer with — and it can mint them
endlessly. Room is therefore made only out of a batch that is no longer being repaired:
one already given up on, or one nobody has asked about in a give-up window. Evicting a
live budget instead would hand back the free answer for a batch already being throttled,
which is the alternation hole moved into the id space; evicting _only_ the given-up ones
would lock out a client that named many batches once and then went quiet, because
silencing takes sustained interest. Oldest-created first, which is O(1) and cannot stall —
the head either keeps being asked about and goes silent, or stops and goes dormant. A fate
request is likewise answered, and registers interest, only up to the cap, capped inside
`respond_to_batch_fate_request` so every caller inherits it: nothing limited how many
batches one frame may name, and 64 MiB of ids is about four million of them.

**The budget deliberately remembers nothing about what the peer declared.** An earlier
version kept the declaration so that a _different_ one could re-arm, which reads as
fairness and is exactly the hole: a peer alternating two declarations, or perturbing one
member per round, resets the budget every round and the bound never engages. A declaration
that can be matched never reaches this path, so re-arming on a changed one buys nothing
else. It also removed a peer-controlled allocation — member counts are not capped, so a
remembered declaration is memory a client chooses the size of.

Gates: `delivered_row_reseal.rs`; `transaction_sealing.rs`'s blind-digest conflict case;
and `missing_answer_bound.rs` — a burst inside one window draws one answer, an
unanswerable seal is answered 40/40 within the grace and 0/40 past it, replayed rows alone
(the production shape: a `User` client through the inbox, not a `Peer` through
`process_from_client`) stop drawing answers, alternating declarations buy nothing,
cycling fresh batch ids stops buying answers, one oversized request is answered only up to
the cap, making room never takes a live budget, a client at the tracking cap can still be
told about a new batch, and a new connection — not merely a new session — asks again, that
last one gated twice: once on the hook and once, in `runtime_core`, on the registration a
handshake actually goes through. Each was falsified by disabling the mechanism it claims
to test.

**Residuals, stated rather than fixed.** A replayed `SealBatch` persists the submission and
then deletes it again when the fate is already settled — two storage writes per replayed
seal, client-driven, no amplification. And `an_exact_replay_costs_one_history_read` counts
`load_history_row_batch` only; the sealed path's `load_history_row_batch_for_schema_hash`
is not instrumented, so that gate proves the reorder rather than the whole replay cost.

## Notes on method

Every number above is a measurement, not an estimate, each taken with one variable changed
against a control. Where a measurement later proved wrong we re-ran it rather than adjusting
the conclusion — for defect 1, two mechanisms were written, reviewed, implemented and
discarded before probing showed what the code actually did.

Two instrument lessons that cost us days, in case they save you the same:

- `ps -o rss=` does not count compressed pages on macOS. The same process read **5 MiB by
  `ps` and 779 MB by `phys_footprint`** at the same instant.
- dhat names the allocation site, not the owner, and cannot see C++ at all — RocksDB's
  memtables were invisible to it while dominating the process's resident set.

Happy to open individual issues for any of these, with the reproductions attached.
