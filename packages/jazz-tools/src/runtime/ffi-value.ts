import type { FFIRecord, FFIRow, Value } from "../drivers/types.js";

type JsonFFIValue =
  | { type: "Integer"; value: number }
  | { type: "BigInt"; value: number }
  | { type: "Double"; value: number }
  | { type: "Boolean"; value: boolean }
  | { type: "Text"; value: string }
  | { type: "Timestamp"; value: number }
  | { type: "Uuid"; value: string }
  | { type: "Bytea"; value: string }
  | { type: "Array"; value: JsonFFIValue[] }
  | { type: "Row"; value: { id?: string; values: JsonFFIValue[] } }
  | { type: "Null" };

type JsonFFIRecord = Record<string, JsonFFIValue>;
type JsonFFIRow = {
  id: string;
  values: JsonFFIValue[];
};

// Hex encoding sits on the hot path of every Bytea write on React Native (a 1 MiB file
// chunk is a million iterations), so it is table-driven. The previous per-byte
// `toString(16).padStart(2)` allocated two strings per byte and measured ~3.7x slower
// on-device for megabyte payloads.
const HEX_BYTE: string[] = Array.from({ length: 256 }, (_, byte) =>
  byte.toString(16).padStart(2, "0"),
);

// Join in bounded chunks so a multi-megabyte payload never holds one giant array of
// per-byte string pointers alive all at once.
const HEX_JOIN_CHUNK = 8192;

function encodeHex(bytes: Uint8Array): string {
  const length = bytes.length;
  const codes = new Array<string>(Math.min(length, HEX_JOIN_CHUNK));
  if (length <= HEX_JOIN_CHUNK) {
    for (let i = 0; i < length; i++) {
      codes[i] = HEX_BYTE[bytes[i]!]!;
    }
    return codes.join("");
  }
  const parts: string[] = [];
  for (let base = 0; base < length; base += HEX_JOIN_CHUNK) {
    const end = Math.min(base + HEX_JOIN_CHUNK, length);
    let n = 0;
    for (let i = base; i < end; i++) {
      codes[n++] = HEX_BYTE[bytes[i]!]!;
    }
    codes.length = n;
    parts.push(codes.join(""));
  }
  return parts.join("");
}

// Nibble value by char code, -1 for non-hex. Indexed by code unit (max 'f' = 102).
const HEX_NIBBLE: Int8Array = (() => {
  const table = new Int8Array(103).fill(-1);
  for (let i = 0; i <= 9; i++) table[0x30 + i] = i;
  for (let i = 0; i < 6; i++) {
    table[0x41 + i] = 10 + i;
    table[0x61 + i] = 10 + i;
  }
  return table;
})();

function decodeHex(value: string): Uint8Array {
  if (value.length % 2 !== 0) {
    throw new Error("Invalid Bytea hex payload: expected an even-length string");
  }

  const bytes = new Uint8Array(value.length / 2);
  for (let i = 0; i < value.length; i += 2) {
    const hi = HEX_NIBBLE[value.charCodeAt(i)] ?? -1;
    const lo = HEX_NIBBLE[value.charCodeAt(i + 1)] ?? -1;
    if (hi < 0 || lo < 0) {
      throw new Error("Invalid Bytea hex payload: expected only hexadecimal characters");
    }
    bytes[i / 2] = (hi << 4) | lo;
  }
  return bytes;
}

function encodeJsonFFIValue(value: Value): JsonFFIValue {
  switch (value.type) {
    case "Bytea":
      return { type: "Bytea", value: encodeHex(value.value) };
    case "Array":
      return { type: "Array", value: value.value.map((entry) => encodeJsonFFIValue(entry)) };
    case "Row":
      return {
        type: "Row",
        value: {
          id: value.value.id,
          values: value.value.values.map((entry) => encodeJsonFFIValue(entry)),
        },
      };
    case "Integer":
    case "BigInt":
    case "Double":
    case "Boolean":
    case "Text":
    case "Timestamp":
    case "Uuid":
    case "Null":
      return { ...value };
  }
}

function decodeJsonFFIValue(value: JsonFFIValue): Value {
  switch (value.type) {
    case "Bytea":
      return { type: "Bytea", value: decodeHex(value.value) };
    case "Array":
      return { type: "Array", value: value.value.map((entry) => decodeJsonFFIValue(entry)) };
    case "Row":
      return {
        type: "Row",
        value: {
          id: value.value.id,
          values: value.value.values.map((entry) => decodeJsonFFIValue(entry)),
        },
      };
    case "Integer":
    case "BigInt":
    case "Double":
    case "Boolean":
    case "Text":
    case "Timestamp":
    case "Uuid":
    case "Null":
      return { ...value };
  }
}

export function encodeFFIRecordToJson(values: FFIRecord): string {
  const jsonValues: JsonFFIRecord = Object.fromEntries(
    Object.entries(values).map(([key, value]) => [key, encodeJsonFFIValue(value)]),
  );
  return JSON.stringify(jsonValues);
}

export function decodeFFIRowFromJson(json: string): FFIRow {
  const parsed = JSON.parse(json) as JsonFFIRow;
  return {
    id: parsed.id,
    values: parsed.values.map((value) => decodeJsonFFIValue(value)),
  };
}

// ─── Blob-sidecar codec ──────────────────────────────────────────────────────
//
// The `*_with_blobs` native methods take Bytea payloads as raw byte buffers next to the
// JSON, with `{"type":"BlobRef","value":<index>}` markers inside it. This avoids hex
// encoding megabytes into the JSON on the way in — and on the way out the native side
// echoes the same BlobRef for any returned Bytea that is byte-identical to an input blob,
// so large payloads are never serialized back either.

type BlobRefJsonValue = JsonFFIValue | { type: "BlobRef"; value: number };

export interface EncodedFFIRecordWithBlobs {
  json: string;
  blobs: Uint8Array[];
}

function encodeJsonFFIValueWithBlobs(value: Value, blobs: Uint8Array[]): BlobRefJsonValue {
  switch (value.type) {
    case "Bytea": {
      const index = blobs.push(value.value) - 1;
      return { type: "BlobRef", value: index };
    }
    case "Array":
      return {
        type: "Array",
        value: value.value.map((entry) =>
          encodeJsonFFIValueWithBlobs(entry, blobs),
        ) as JsonFFIValue[],
      };
    case "Row":
      return {
        type: "Row",
        value: {
          id: value.value.id,
          values: value.value.values.map((entry) =>
            encodeJsonFFIValueWithBlobs(entry, blobs),
          ) as JsonFFIValue[],
        },
      };
    default:
      return encodeJsonFFIValue(value);
  }
}

export function encodeFFIRecordWithBlobs(values: FFIRecord): EncodedFFIRecordWithBlobs {
  const blobs: Uint8Array[] = [];
  const jsonValues: Record<string, BlobRefJsonValue> = Object.fromEntries(
    Object.entries(values).map(([key, value]) => [key, encodeJsonFFIValueWithBlobs(value, blobs)]),
  );
  return { json: JSON.stringify(jsonValues), blobs };
}

/**
 * Resolve a parsed `*_with_blobs` return payload in place: `BlobRef` markers become the
 * very Uint8Array the caller passed in (zero copies), and hex Bytea — possible when the
 * runtime returns bytes that did not come from this call's input, e.g. on restore —
 * becomes decoded bytes. Non-Bytea values already have the legacy wire shape and are left
 * untouched, so the resolved row is exactly what the legacy methods would have produced.
 */
export function resolveReturnedFFIValuesWithBlobs(
  values: unknown[],
  blobs: readonly Uint8Array[],
): void {
  for (let i = 0; i < values.length; i++) {
    const entry = values[i] as { type?: string; value?: unknown } | null;
    if (entry === null || typeof entry !== "object") continue;
    switch (entry.type) {
      case "BlobRef": {
        const index = entry.value;
        if (typeof index !== "number" || blobs[index] === undefined) {
          throw new Error(`Invalid BlobRef ${String(index)} in returned row`);
        }
        values[i] = { type: "Bytea", value: blobs[index] };
        break;
      }
      case "Bytea": {
        if (typeof entry.value === "string") {
          values[i] = { type: "Bytea", value: decodeHex(entry.value) };
        }
        break;
      }
      case "Array": {
        if (Array.isArray(entry.value)) {
          resolveReturnedFFIValuesWithBlobs(entry.value, blobs);
        }
        break;
      }
      case "Row": {
        const row = entry.value as { values?: unknown[] } | null;
        if (row !== null && typeof row === "object" && Array.isArray(row.values)) {
          resolveReturnedFFIValuesWithBlobs(row.values, blobs);
        }
        break;
      }
      default:
        break;
    }
  }
}
