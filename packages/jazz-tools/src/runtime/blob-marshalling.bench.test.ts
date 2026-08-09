import { describe, expect, it } from "vitest";
import { BLOB_BYTES, blobQuery, blobRow, blobSchema, cell } from "./testing/blob-fixtures.js";
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

const BENCH_ROWS = 16;
const RUN_BENCH = process.env.JAZZ_BLOB_BENCH === "1";

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

describe.skipIf(!hasJazzNapiBuild())("Bytea nested inside other values", () => {
  it("keeps the wrapper shape while handing nested blobs over as bytes", async () => {
    const schema = blobSchema();
    const runtime = await createNapiRuntime(schema, { appId: "blob-marshalling-nested" });

    // An empty blob alongside a populated one: zero-length is its own case for an
    // external buffer, and nothing else exercises it.
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

    // The wrapper for a null column must keep whatever serde produced for it: this
    // branch is delegated, and the assertion is here so a future rewrite that stops
    // delegating has to keep it.
    expect(cell(inserted, 3).type).toBe("Null");

    const rows = (await runtime.query(blobQuery(schema), null, null, null)) as unknown[];
    const readParts = cell(rows[0], 2);
    expect(readParts.type).toBe("Array");
    for (const element of readParts.value as { value?: unknown }[]) {
      expect(ArrayBuffer.isView(element.value)).toBe(true);
    }
  });
});

describe.skipIf(!hasJazzNapiBuild())("Bytea through a subscription", () => {
  it(
    "delivers blobs in a delta as bytes, not as an array of numbers",
    { timeout: 20_000 },
    async () => {
      // This is the path the app actually reads blobs on — `media-file-cache` subscribes
      // to a window of `file_parts` rather than querying — and it is a different code
      // path from `query()`: deltas go out through a ThreadsafeFunction. Fixing the query
      // path did nothing for it, which is why this case exists separately.
      const schema = blobSchema();
      const runtime = await createNapiRuntime(schema, { appId: "blob-marshalling-subscription" });

      runtime.insert("blobs", blobRow(3, "subscribed") as never);

      const firstDelta = await new Promise<unknown[]>((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error("no delta arrived")), 10_000);
        const handle = runtime.createSubscription(blobQuery(schema), null, null, null);
        // The threadsafe function calls back node-style, `(err, value)`.
        runtime.executeSubscription(handle, (error: unknown, delta: unknown) => {
          if (error != null) {
            reject(error instanceof Error ? error : new Error(String(error)));
            return;
          }
          if (Array.isArray(delta) && delta.length > 0) {
            clearTimeout(timer);
            resolve(delta);
          }
        });
      });

      const added = firstDelta.find((change) => (change as { kind?: number }).kind === 0) as
        | { row?: { values?: { type?: string; value?: unknown }[] } }
        | undefined;
      expect(added?.row, "the delta carried no added row to inspect").toBeDefined();

      const data = added!.row!.values![0]!;
      expect(data.type).toBe("Bytea");
      expect(
        Array.isArray(data.value),
        "a subscription delta shipped the blob as a JS array — this is the read path the app uses",
      ).toBe(false);
      expect(ArrayBuffer.isView(data.value)).toBe(true);
    },
  );
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
