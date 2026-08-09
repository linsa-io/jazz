import type { WasmSchema } from "../../drivers/types.js";
import { translateQuery } from "../query-adapter.js";

/**
 * The blob table both binding gates use.
 *
 * Shared so `blob-marshalling.bench.test.ts` (napi) and `blob-marshalling.wasm.test.ts`
 * ask the SAME question of each boundary — the two bindings marshal values through
 * completely different machinery (napi's `Object::AddDataProperty` vs
 * `serde_wasm_bindgen`), and a fix on one side says nothing about the other.
 */
export function blobSchema(): WasmSchema {
  return {
    blobs: {
      columns: [
        { name: "data", column_type: { type: "Bytea" as const }, nullable: false },
        { name: "label", column_type: { type: "Text" as const }, nullable: false },
        // A blob nested one level down: the wrapper for `Array` is restated by hand on the
        // napi side so it can recurse, and that restatement is what can silently drift.
        {
          name: "parts",
          column_type: { type: "Array" as const, element: { type: "Bytea" as const } },
          nullable: true,
        },
        { name: "note", column_type: { type: "Text" as const }, nullable: true },
      ],
    },
  } as unknown as WasmSchema;
}

export function blobQuery(schema: WasmSchema): string {
  return translateQuery(
    JSON.stringify({ table: "blobs", conditions: [], includes: {}, orderBy: [] }),
    schema,
  );
}

/** One megabyte, the size a `file_parts` row carries in the app. */
export const BLOB_BYTES = 1024 * 1024;

export function blobPayload(seed: number, bytes: number = BLOB_BYTES): Uint8Array {
  const buf = new Uint8Array(bytes);
  buf.fill(seed % 251);
  return buf;
}

export function blobRow(seed: number, label: string) {
  return {
    data: { type: "Bytea" as const, value: blobPayload(seed) },
    label: { type: "Text" as const, value: label },
  };
}

/** One column cell of a wire row, before any JS-side normalisation. */
export function cell(row: unknown, index: number): { type?: string; value?: unknown } {
  const values = (row as { values?: unknown[] })?.values;
  if (!Array.isArray(values)) {
    throw new Error(`unexpected wire row shape: ${JSON.stringify(row)?.slice(0, 200)}`);
  }
  return values[index] as { type?: string; value?: unknown };
}
