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
