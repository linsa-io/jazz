import { describe, expect, it } from "vitest";
import type { WasmSchema } from "../drivers/types.js";
import { translateQuery } from "./query-adapter.js";
import { createNapiRuntime, hasJazzNapiBuild } from "./testing/napi-runtime-test-utils.js";

/**
 * How a megabyte of `Bytea` crosses the Rust↔JS boundary.
 *
 * `ValueHuman::Bytea` is a plain `Vec<u8>` with no `serde_bytes`
 * (`crates/jazz-tools/src/query_manager/types/value.rs`), so `serde_json` renders a blob as
 * an array of a million Numbers and napi then sets it into a JS array one element at a
 * time. Measured against a node client uploading 1 MiB rows: 18 MiB/s with the client at
 * 227% CPU while the server idled at 5%, its leaf frames all `napi_set_element` /
 * `Object::AddDataProperty` / `FastElementsAccessor::AddImpl`.
 *
 * The JS side already prefers the fast path — `toByteArray` in `row-transformer.ts` returns
 * an incoming `Uint8Array` untouched and only falls back to a per-byte loop for arrays, and
 * its own comment records that React Native takes that loop for every blob read. So the fix
 * is entirely on the Rust side of the boundary, and the gate below is what says whether it
 * landed.
 *
 * The gate asserts a SHAPE, not a duration, so it cannot flake on a loaded machine. The
 * timings live behind `JAZZ_BLOB_BENCH=1`.
 */

const BLOB_BYTES = 1024 * 1024;
const BENCH_ROWS = 16;
const RUN_BENCH = process.env.JAZZ_BLOB_BENCH === "1";

function blobSchema(): WasmSchema {
  return {
    blobs: {
      columns: [
        { name: "data", column_type: { type: "Bytea" as const }, nullable: false },
        { name: "label", column_type: { type: "Text" as const }, nullable: false },
      ],
    },
  } as unknown as WasmSchema;
}

function blobQuery(schema: WasmSchema): string {
  return translateQuery(
    JSON.stringify({ table: "blobs", conditions: [], includes: {}, orderBy: [] }),
    schema,
  );
}

function payload(seed: number): Uint8Array {
  const bytes = new Uint8Array(BLOB_BYTES);
  bytes.fill(seed % 251);
  return bytes;
}

function blobRow(seed: number, label: string) {
  return {
    data: { type: "Bytea" as const, value: payload(seed) },
    label: { type: "Text" as const, value: label },
  };
}

/** What the binding handed back for the `data` column, before any JS normalisation. */
function byteaFromWireRow(row: unknown): unknown {
  const values = (row as { values?: unknown[] })?.values;
  if (!Array.isArray(values)) {
    throw new Error(`unexpected wire row shape: ${JSON.stringify(row)?.slice(0, 200)}`);
  }
  const cell = values[0] as { type?: string; value?: unknown };
  expect(cell?.type).toBe("Bytea");
  return cell?.value;
}

describe.skipIf(!hasJazzNapiBuild())("Bytea across the napi boundary", () => {
  it("hands a blob back as bytes, not as an array of numbers", async () => {
    const schema = blobSchema();
    const runtime = await createNapiRuntime(schema, { appId: "blob-marshalling-gate" });

    const inserted = runtime.insert("blobs", blobRow(1, "gate") as never);

    // The insert echo is the write-side crossing: the binding sends the whole row back,
    // blob included, although JS supplied those bytes a microsecond ago.
    const echoed = byteaFromWireRow(inserted);
    expect(
      Array.isArray(echoed),
      "insert() echoed the blob as a JS array — one napi call per byte",
    ).toBe(false);
    expect(ArrayBuffer.isView(echoed)).toBe(true);

    // The read crossing, which is the one shared with React Native.
    const rows = (await runtime.query(blobQuery(schema), null, null, null)) as unknown[];
    expect(rows.length).toBeGreaterThan(0);
    const read = byteaFromWireRow(rows[0]);
    expect(
      Array.isArray(read),
      "query() returned the blob as a JS array — this is the per-byte read path",
    ).toBe(false);
    expect(ArrayBuffer.isView(read)).toBe(true);
  });
});

describe.skipIf(!RUN_BENCH || !hasJazzNapiBuild())("Bytea throughput across the boundary", () => {
  it("reports MiB/s and CPU for a write and a read of the same blobs", async () => {
    const schema = blobSchema();
    const runtime = await createNapiRuntime(schema, { appId: "blob-marshalling-bench" });

    const before = process.cpuUsage();
    const writeStarted = performance.now();
    for (let index = 0; index < BENCH_ROWS; index += 1) {
      runtime.insert("blobs", blobRow(index, `row-${index}`) as never);
    }
    const writeMs = performance.now() - writeStarted;

    const readStarted = performance.now();
    await runtime.query(blobQuery(schema), null, null, null);
    const readMs = performance.now() - readStarted;
    const cpu = process.cpuUsage(before);

    const mib = (BENCH_ROWS * BLOB_BYTES) / 1048576;
    console.info(
      JSON.stringify(
        {
          rows: BENCH_ROWS,
          mib,
          write: { ms: Math.round(writeMs), mibPerSec: +(mib / (writeMs / 1000)).toFixed(1) },
          read: { ms: Math.round(readMs), mibPerSec: +(mib / (readMs / 1000)).toFixed(1) },
          // Wall time hides where it went: this path is synchronous on the JS thread, so
          // user CPU close to wall time means the boundary is the cost, not waiting.
          cpu: { userMs: Math.round(cpu.user / 1000), systemMs: Math.round(cpu.system / 1000) },
        },
        null,
        2,
      ),
    );
    expect(writeMs).toBeGreaterThan(0);
  });
});
