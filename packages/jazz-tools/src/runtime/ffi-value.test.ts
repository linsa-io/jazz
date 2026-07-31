import { describe, expect, it } from "vitest";

import type { FFIRow } from "../drivers/types.js";
import { decodeFFIRowFromJson, encodeFFIRecordToJson } from "./ffi-value.js";

// The hex codec is table-driven for speed; these tests pin its behavior at the
// boundaries the fast path could get wrong: full byte range, chunk-join seams,
// case-insensitive decode, and rejection of malformed payloads.

function encodedHex(bytes: Uint8Array): string {
  const json = JSON.parse(encodeFFIRecordToJson({ data: { type: "Bytea", value: bytes } })) as {
    data: { type: "Bytea"; value: string };
  };
  return json.data.value;
}

function decodedBytes(hex: string): Uint8Array {
  const row = decodeFFIRowFromJson(
    JSON.stringify({ id: "row-1", values: [{ type: "Bytea", value: hex }] }),
  ) as FFIRow;
  const value = row.values[0]!;
  if (value.type !== "Bytea") {
    throw new Error(`expected Bytea, got ${value.type}`);
  }
  return value.value;
}

describe("ffi-value hex codec", () => {
  it("encodes every byte value to two lowercase hex digits", () => {
    const all = new Uint8Array(256);
    for (let i = 0; i < 256; i++) all[i] = i;
    const hex = encodedHex(all);
    expect(hex).toHaveLength(512);
    expect(hex.startsWith("000102")).toBe(true);
    expect(hex.endsWith("fdfeff")).toBe(true);
    expect(hex).toBe(hex.toLowerCase());
  });

  it("round-trips a payload larger than the join chunk", () => {
    // 1 MiB, patterned so a mis-seamed chunk join cannot cancel out.
    const big = new Uint8Array(1024 * 1024);
    for (let i = 0; i < big.length; i++) big[i] = (i * 31 + (i >> 8)) & 255;
    const back = decodedBytes(encodedHex(big));
    expect(back).toHaveLength(big.length);
    expect(Buffer.from(back).equals(Buffer.from(big))).toBe(true);
  });

  it("round-trips a payload exactly at and around the join chunk boundary", () => {
    for (const size of [8191, 8192, 8193]) {
      const bytes = new Uint8Array(size).map((_, i) => (i * 7) & 255);
      const back = decodedBytes(encodedHex(bytes));
      expect(Buffer.from(back).equals(Buffer.from(bytes))).toBe(true);
    }
  });

  it("round-trips the empty payload", () => {
    expect(encodedHex(new Uint8Array(0))).toBe("");
    expect(decodedBytes("")).toHaveLength(0);
  });

  it("decodes uppercase hex", () => {
    expect(Array.from(decodedBytes("0AFF"))).toEqual([0x0a, 0xff]);
  });

  it("rejects odd-length payloads", () => {
    expect(() => decodedBytes("abc")).toThrow(/even-length/);
  });

  it("rejects non-hex characters, including ones past the lookup range", () => {
    for (const bad of ["zz", "0g", "0€", "0\u{1F600}".slice(0, 2)]) {
      expect(() => decodedBytes(bad)).toThrow(/hexadecimal/);
    }
  });
});
