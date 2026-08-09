import { describe, expect, it } from "vitest";
import {
  BLOB_BYTES,
  blobPayload,
  blobQuery,
  blobRow,
  blobSchema,
  cell,
} from "./testing/blob-fixtures.js";
import { createWasmRuntime, hasJazzWasmBuild } from "./testing/wasm-runtime-test-utils.js";

/**
 * How a megabyte of `Bytea` crosses the Rust↔JS boundary in the BROWSER binding.
 *
 * The napi twin of this file (`blob-marshalling.bench.test.ts`) exists because that
 * boundary once handed a blob over as an array of a million Numbers, one
 * `napi_set_element` per byte: 18 MiB/s with the client pinned at 227% CPU while the
 * server idled. The fix was entirely on the Rust side of napi, so it says nothing about
 * this side — jazz-wasm marshals through `serde_wasm_bindgen` instead, where the same
 * mistake has the same shape (a `Vec<u8>` with no `serde_bytes` serialises as a JS Array)
 * and would be just as invisible until someone sent a real file.
 *
 * Web had no coverage here at all: two Rust tests in the crate, none in TypeScript, and
 * the app that runs on it moves files through `file_parts` rows of exactly BLOB_BYTES.
 *
 * WHAT THIS FOUND, measured 2026-08-10 on linsa-v16.1: the browser binding has the defect
 * napi had. `query()` and the `insert()` echo both go through `serde_wasm_bindgen`
 * (runtime.rs `Serializer::new()`), where `ValueHuman::Bytea` is a bare `Vec<u8>` and
 * serialises as a JS Array of a million Numbers. Subscriptions do NOT: on wasm32 they take
 * `native_subscription_delta_to_js`, which packs encoded rows into one `Uint8Array`.
 *
 * So the surviving cost lands on the WRITE path, which is where the app meets it — the web
 * uploader calls `db.insert()` per 1 MiB part and every call echoes that megabyte back as
 * an array. Measured at the binding: write 8.3 MB/s, read 29.7 MB/s. Measured from the app
 * (Linsa.Web `pnpm test:perf`): 8.0-8.5 MB/s upload, flat to the 100 MB cap. Same number,
 * both sides of the boundary.
 *
 * The two crossings that still marshal per byte are marked `it.fails`: they pass while the
 * defect stands and start failing the moment someone fixes the Rust side, which is the
 * signal to flip them back to `it` and keep them as ordinary gates.
 *
 * The gates assert SHAPE, never a duration, so they cannot flake on a loaded machine.
 * `JAZZ_BLOB_BENCH=1` adds the timings.
 */

const BENCH_ROWS = 16;
const RUN_BENCH = process.env.JAZZ_BLOB_BENCH === "1";

/** What the binding handed back for the `data` column, before any JS normalisation. */
function byteaFromWireRow(row: unknown): unknown {
  const data = cell(row, 0);
  expect(data.type).toBe("Bytea");
  return data.value;
}

describe.skipIf(!hasJazzWasmBuild())("Bytea across the wasm boundary", () => {
  // KNOWN DEFECT (see the file header): flip to `it` when the Rust side hands bytes.
  it.fails("hands a blob back as bytes, not as an array of numbers", async () => {
    const schema = blobSchema();
    const runtime = await createWasmRuntime(schema, { appId: "wasm-blob-marshalling" });

    // The write-side crossing: the binding echoes the whole row back, blob included,
    // although JS supplied those bytes a microsecond ago.
    const echoed = byteaFromWireRow(runtime.insert("blobs", blobRow(1, "gate") as never));
    expect(
      Array.isArray(echoed),
      "insert() echoed the blob as a JS array — that is a per-byte crossing",
    ).toBe(false);
    expect(ArrayBuffer.isView(echoed)).toBe(true);
    expect((echoed as Uint8Array).byteLength).toBe(BLOB_BYTES);

    // The read crossing.
    const rows = (await runtime.query(blobQuery(schema), null, null, null)) as unknown[];
    expect(rows.length).toBeGreaterThan(0);
    const read = byteaFromWireRow(rows[0]);
    expect(
      Array.isArray(read),
      "query() returned the blob as a JS array — this is the per-byte read path",
    ).toBe(false);
    expect(ArrayBuffer.isView(read)).toBe(true);
    expect((read as Uint8Array).byteLength).toBe(BLOB_BYTES);
  });

  // KNOWN DEFECT, same cause: nested blobs cross as arrays too.
  it.fails("keeps the wrapper shape while handing nested blobs over as bytes", async () => {
    const schema = blobSchema();
    const runtime = await createWasmRuntime(schema, { appId: "wasm-blob-nested" });

    // An empty blob alongside a populated one: zero-length is its own case for a view over
    // wasm memory, and nothing else exercises it.
    const inserted = runtime.insert("blobs", {
      data: { type: "Bytea" as const, value: new Uint8Array(0) },
      label: { type: "Text" as const, value: "nested" },
      parts: {
        type: "Array" as const,
        value: [
          { type: "Bytea" as const, value: new Uint8Array([1, 2, 3]) },
          { type: "Bytea" as const, value: new Uint8Array(0) },
        ],
      },
      note: { type: "Null" as const, value: null },
    } as never);

    const top = cell(inserted, 0);
    expect(top.type).toBe("Bytea");
    expect(ArrayBuffer.isView(top.value)).toBe(true);
    expect((top.value as Uint8Array).length).toBe(0);

    const parts = cell(inserted, 2);
    expect(parts.type).toBe("Array");
    const elements = parts.value as { type?: string; value?: unknown }[];
    expect(elements).toHaveLength(2);
    for (const element of elements) {
      expect(element.type).toBe("Bytea");
      expect(
        ArrayBuffer.isView(element.value),
        "a blob nested in an Array still crossed as numbers",
      ).toBe(true);
    }
    expect(Array.from(elements[0]!.value as Uint8Array)).toEqual([1, 2, 3]);

    // A null column must keep whatever serde produced for it.
    expect(cell(inserted, 3).type).toBe("Null");
  });

  it(
    "ships subscription deltas as encoded bytes, never as per-value objects",
    { timeout: 20_000 },
    async () => {
      // The path the web app actually reads files on: `useAll` subscribes, it does not
      // query. On wasm32 `make_subscription_callback` always goes through
      // `native_subscription_delta_to_js`, which packs whole encoded rows into one
      // `Uint8Array` — so a blob never becomes JS values on this path at all, and
      // `decodeNativeTypedDelta` unpacks it on the JS side.
      //
      // This is the FAST path, and the gate exists to keep it that way: if a future change
      // routes deltas back through serde, blobs silently become arrays of numbers here and
      // every image in the app pays a per-byte crossing again.
      const schema = blobSchema();
      const runtime = await createWasmRuntime(schema, { appId: "wasm-blob-subscription" });

      runtime.insert("blobs", blobRow(3, "subscribed") as never);

      const delta = await new Promise<Record<string, unknown>>((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error("no delta arrived")), 10_000);
        const handle = runtime.createSubscription(blobQuery(schema), null, null, null);
        runtime.executeSubscription(handle, (received: unknown) => {
          const value = received as Record<string, unknown> | null;
          if (value != null && (value.addedCount as number) > 0) {
            clearTimeout(timer);
            resolve(value);
          }
        });
      });

      expect(delta.addedCount).toBe(1);
      expect(
        ArrayBuffer.isView(delta.added),
        "the delta stopped arriving as packed bytes — blobs are crossing as JS values again",
      ).toBe(true);
      // The packed buffer must actually carry the megabyte: 16-byte id + 4-byte index +
      // 4-byte length + the encoded row itself.
      expect((delta.added as Uint8Array).byteLength).toBeGreaterThan(BLOB_BYTES);
    },
  );
});

describe.skipIf(!hasJazzWasmBuild() || !RUN_BENCH)("Bytea throughput across wasm", () => {
  it("reports MB/s for write and read", { timeout: 120_000 }, async () => {
    const schema = blobSchema();
    const runtime = await createWasmRuntime(schema, { appId: "wasm-blob-bench" });

    const writeStart = performance.now();
    for (let i = 0; i < BENCH_ROWS; i++) {
      runtime.insert("blobs", {
        data: { type: "Bytea" as const, value: blobPayload(i) },
        label: { type: "Text" as const, value: `row-${i}` },
      } as never);
    }
    const writeMs = performance.now() - writeStart;

    const readStart = performance.now();
    const rows = (await runtime.query(blobQuery(schema), null, null, null)) as unknown[];
    const readMs = performance.now() - readStart;
    expect(rows).toHaveLength(BENCH_ROWS);

    const mb = (BENCH_ROWS * BLOB_BYTES) / (1024 * 1024);
    // eslint-disable-next-line no-console -- the numbers are the point of this block
    console.log(
      `wasm Bytea: write ${(mb / (writeMs / 1000)).toFixed(1)} MB/s, ` +
        `read ${(mb / (readMs / 1000)).toFixed(1)} MB/s (${mb} MB in ${BENCH_ROWS} rows)`,
    );
  });
});
