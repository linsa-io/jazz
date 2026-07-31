import { describe, expect, it, vi } from "vitest";
import { JazzRnRuntimeAdapter, type JazzRnRuntimeBinding } from "./jazz-rn-runtime-adapter.js";
import { decodeFFIRowFromJson, encodeFFIRecordToJson } from "../runtime/ffi-value.js";

function createBinding(overrides: Partial<JazzRnRuntimeBinding> = {}): JazzRnRuntimeBinding {
  const commitBatch = overrides.commitBatch ?? vi.fn();
  return {
    batchedTick: vi.fn(),
    close: vi.fn(),
    connect: vi.fn(),
    disconnect: vi.fn(),
    updateAuth: vi.fn(),
    onAuthFailure: vi.fn(),
    onMutationError: vi.fn(),
    createSubscription: vi.fn(() => 9n),
    delete_: vi.fn((_objectId, writeContextJson) =>
      JSON.stringify({ batchId: writeContextJson ? "batch-delete-2" : "batch-delete-1" }),
    ),
    executeSubscription: vi.fn(),
    getSchemaHash: vi.fn(() => "schema-hash"),
    waitForBatch: vi.fn(async () => undefined),
    beginBatch: vi.fn((batchMode) => `batch-${batchMode}`),
    rollbackBatch: vi.fn(() => true),
    insert: vi.fn((_table, _valuesJson, writeContextJson) =>
      JSON.stringify({
        id: "row-1",
        values: [],
        batchId: writeContextJson ? "batch-2" : "batch-1",
      }),
    ),
    restore: vi.fn((_table, _objectId, _valuesJson, writeContextJson) =>
      JSON.stringify({
        id: "row-1",
        values: [],
        batchId: writeContextJson ? "batch-restore-2" : "batch-restore-1",
      }),
    ),
    onBatchedTickNeeded: vi.fn(),
    query: vi.fn(() => Promise.resolve(JSON.stringify([{ id: "row-1", values: [] }]))),
    unsubscribe: vi.fn(),
    update: vi.fn((_objectId, _valuesJson, writeContextJson) =>
      JSON.stringify({ batchId: writeContextJson ? "batch-update-2" : "batch-update-1" }),
    ),
    upsert: vi.fn((_table, _objectId, _valuesJson, writeContextJson) =>
      JSON.stringify({ batchId: writeContextJson ? "batch-upsert-2" : "batch-upsert-1" }),
    ),
    ...overrides,
    commitBatch,
  };
}

describe("JazzRnRuntimeAdapter", () => {
  it("defers batched tick execution to avoid re-entrancy", async () => {
    const binding = createBinding();
    new JazzRnRuntimeAdapter(binding, {});

    const onBatchedTickNeeded = binding.onBatchedTickNeeded as ReturnType<typeof vi.fn>;
    const callbackObject = onBatchedTickNeeded.mock.calls[0]![0];

    callbackObject.requestBatchedTick();
    expect(binding.batchedTick).not.toHaveBeenCalled();

    await Promise.resolve();
    expect(binding.batchedTick).toHaveBeenCalledTimes(1);
  });

  it("serializes mutation payloads and parses query responses", async () => {
    const binding = createBinding();
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    expect(adapter.beginBatch("transactional")).toBe("batch-transactional");
    expect(adapter.rollbackBatch("batch-transactional")).toBe(true);

    const row = adapter.insert("todos", { title: { type: "Text", value: "milk" } });
    expect(row).toEqual({ id: "row-1", values: [], batchId: "batch-1" });
    expect(binding.insert).toHaveBeenCalledWith(
      "todos",
      JSON.stringify({ title: { type: "Text", value: "milk" } }),
      undefined,
      undefined,
    );

    const restored = adapter.restore("todos", "row-1", { title: { type: "Text", value: "eggs" } });
    expect(restored).toEqual({ id: "row-1", values: [], batchId: "batch-restore-1" });
    expect(binding.restore).toHaveBeenCalledWith(
      "todos",
      "row-1",
      JSON.stringify({ title: { type: "Text", value: "eggs" } }),
      undefined,
    );

    adapter.update("row-1", { done: { type: "Boolean", value: true } });
    expect(binding.update).toHaveBeenCalledWith(
      "row-1",
      JSON.stringify({ done: { type: "Boolean", value: true } }),
      undefined,
    );

    adapter.upsert("todos", "row-1", { done: { type: "Boolean", value: true } });
    expect(binding.upsert).toHaveBeenCalledWith(
      "todos",
      "row-1",
      JSON.stringify({ done: { type: "Boolean", value: true } }),
      undefined,
    );

    adapter.delete("row-1");
    expect(binding.delete_).toHaveBeenCalledWith("row-1", undefined);

    await expect(adapter.query("{}", null, null)).resolves.toEqual([{ id: "row-1", values: [] }]);
  });

  it("yields the JS event loop while a query is in flight", async () => {
    // Simulates a slow native query: the binding returns a Promise that we
    // resolve only from a setTimeout(0), i.e. after the event loop has had a
    // chance to run other tasks. A correct adapter awaits the binding's
    // Promise, so the timeout fires, resolves the binding, and the query
    // returns. A blocking adapter (e.g. one that parses the result without
    // awaiting) never yields and the query never settles.
    let resolveBinding!: (value: string) => void;
    const queryMock = vi.fn(
      () =>
        new Promise<string>((resolve) => {
          resolveBinding = resolve;
        }),
    );
    const binding = createBinding({ query: queryMock });
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    let otherJsDidRun = false;
    const queryPromise = adapter.query("{}", null, null);
    setTimeout(() => {
      otherJsDidRun = true;
      resolveBinding(JSON.stringify([{ id: "row-1", values: [] }]));
    }, 0);

    await expect(queryPromise).resolves.toEqual([{ id: "row-1", values: [] }]);
    expect(otherJsDidRun).toBe(true);
  });

  it("encodes Bytea mutations with an explicit FFI transport shape", () => {
    const binding = createBinding();
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    adapter.insert("files", {
      data: { type: "Bytea", value: new Uint8Array([0x01, 0x02, 0xff]) },
    });

    expect(binding.insert).toHaveBeenCalledWith(
      "files",
      JSON.stringify({
        data: { type: "Bytea", value: "0102ff" },
      }),
      undefined,
      undefined,
    );
  });

  it("routes inserts through the blob sidecar when the binding supports it", () => {
    const payload = new Uint8Array(1024).fill(0xab);
    const insertWithBlobs = vi.fn((_table, valuesJson, _blobs, _wc, _oid) => {
      expect(JSON.parse(valuesJson)).toEqual({
        idx: { type: "Integer", value: 3 },
        data: { type: "BlobRef", value: 0 },
      });
      return JSON.stringify({
        id: "row-1",
        values: [
          { type: "Integer", value: 3 },
          { type: "BlobRef", value: 0 },
        ],
        batchId: "batch-1",
      });
    });
    const binding = createBinding({ insertWithBlobs });
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    const row = adapter.insert("file_parts", {
      idx: { type: "Integer", value: 3 },
      data: { type: "Bytea", value: payload },
    });

    expect(insertWithBlobs).toHaveBeenCalledTimes(1);
    expect(binding.insert).not.toHaveBeenCalled();
    // The blob crosses as the very same underlying buffer — no copies, no hex.
    const sentBlobs = insertWithBlobs.mock.calls[0]![2] as ArrayBuffer[];
    expect(sentBlobs).toHaveLength(1);
    expect(sentBlobs[0]).toBe(payload.buffer);
    // And the echoed BlobRef resolves back to the original Uint8Array.
    expect(row.values[1]).toEqual({ type: "Bytea", value: payload });
    expect((row.values[1] as { value: Uint8Array }).value).toBe(payload);
  });

  it("slices partial Uint8Array views instead of leaking their whole buffer", () => {
    const backing = new Uint8Array([1, 2, 3, 4, 5, 6]);
    const view = backing.subarray(2, 5);
    const insertWithBlobs = vi.fn(
      (_table: string, _valuesJson: string, _blobs: ArrayBuffer[], _wc?: string, _oid?: string) =>
        JSON.stringify({ id: "row-1", values: [], batchId: "batch-1" }),
    );
    const binding = createBinding({ insertWithBlobs });
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    adapter.insert("files", { data: { type: "Bytea", value: view } });

    const sent = insertWithBlobs.mock.calls[0]![2] as unknown as ArrayBuffer[];
    expect(sent[0]!.byteLength).toBe(3);
    expect(Array.from(new Uint8Array(sent[0]!))).toEqual([3, 4, 5]);
    expect(sent[0]).not.toBe(backing.buffer);
  });

  it("indexes multiple blobs in traversal order, including nested ones", () => {
    const a = new Uint8Array([1]);
    const b = new Uint8Array([2, 2]);
    const insertWithBlobs = vi.fn(
      (_table: string, valuesJson: string, _blobs: ArrayBuffer[], _wc?: string, _oid?: string) => {
        expect(JSON.parse(valuesJson)).toEqual({
          data: { type: "BlobRef", value: 0 },
          chunks: { type: "Array", value: [{ type: "BlobRef", value: 1 }] },
        });
        return JSON.stringify({ id: "row-1", values: [], batchId: "batch-1" });
      },
    );
    const binding = createBinding({ insertWithBlobs });
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    adapter.insert("files", {
      data: { type: "Bytea", value: a },
      chunks: { type: "Array", value: [{ type: "Bytea", value: b }] },
    });

    expect(insertWithBlobs.mock.calls[0]![2]).toEqual([a.buffer, b.buffer]);
  });

  it("decodes hex Bytea in a blob-path return that did not come from the input", () => {
    // Restore can resurrect columns absent from the input; those come back as hex.
    const restoreWithBlobs = vi.fn(() =>
      JSON.stringify({
        id: "row-1",
        values: [{ type: "Bytea", value: "0aff" }],
        batchId: "batch-restore-1",
      }),
    );
    const binding = createBinding({ restoreWithBlobs });
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    const row = adapter.restore("files", "row-1", {});
    expect(row.values[0]).toEqual({ type: "Bytea", value: new Uint8Array([0x0a, 0xff]) });
  });

  it("routes update and upsert through their blob variants when available", () => {
    const payload = new Uint8Array([9, 9, 9]);
    const updateWithBlobs = vi.fn(
      (_objectId: string, _valuesJson: string, _blobs: ArrayBuffer[], _wc?: string) =>
        JSON.stringify({ batchId: "batch-update-1" }),
    );
    const upsertWithBlobs = vi.fn(() => JSON.stringify({ batchId: "batch-upsert-1" }));
    const binding = createBinding({ updateWithBlobs, upsertWithBlobs });
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    adapter.update("row-1", { data: { type: "Bytea", value: payload } });
    adapter.upsert("files", "row-1", { data: { type: "Bytea", value: payload } });

    expect(updateWithBlobs).toHaveBeenCalledTimes(1);
    expect(upsertWithBlobs).toHaveBeenCalledTimes(1);
    expect(binding.update).not.toHaveBeenCalled();
    expect(binding.upsert).not.toHaveBeenCalled();
    expect(updateWithBlobs.mock.calls[0]![1]).toBe(
      JSON.stringify({ data: { type: "BlobRef", value: 0 } }),
    );
    expect(updateWithBlobs.mock.calls[0]![2]).toEqual([payload.buffer]);
  });

  it("throws on an out-of-range BlobRef in a blob-path return", () => {
    const insertWithBlobs = vi.fn(() =>
      JSON.stringify({
        id: "row-1",
        values: [{ type: "BlobRef", value: 5 }],
        batchId: "batch-1",
      }),
    );
    const binding = createBinding({ insertWithBlobs });
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    expect(() => adapter.insert("files", {})).toThrow(/Invalid BlobRef/);
  });

  it("round-trips Bytea values through the RN FFI JSON codec", () => {
    const encoded = JSON.parse(
      encodeFFIRecordToJson({
        data: { type: "Bytea", value: new Uint8Array([0x01, 0x02, 0xff]) },
        chunks: {
          type: "Array",
          value: [{ type: "Bytea", value: new Uint8Array([0x0a, 0x0b]) }],
        },
        nested: {
          type: "Row",
          value: {
            id: "nested-row",
            values: [{ type: "Bytea", value: new Uint8Array([0x7f]) }],
          },
        },
      }),
    ) as Record<string, unknown>;

    expect(encoded).toEqual({
      data: { type: "Bytea", value: "0102ff" },
      chunks: {
        type: "Array",
        value: [{ type: "Bytea", value: "0a0b" }],
      },
      nested: {
        type: "Row",
        value: {
          id: "nested-row",
          values: [{ type: "Bytea", value: "7f" }],
        },
      },
    });

    const decoded = decodeFFIRowFromJson(
      JSON.stringify({
        id: "row-1",
        values: [encoded.data, encoded.chunks, encoded.nested],
      }),
    );

    const [data, chunks, nested] = decoded.values;
    expect(decoded.id).toBe("row-1");
    expect(data).toEqual({ type: "Bytea", value: new Uint8Array([0x01, 0x02, 0xff]) });
    expect(chunks).toEqual({
      type: "Array",
      value: [{ type: "Bytea", value: new Uint8Array([0x0a, 0x0b]) }],
    });
    expect(nested).toEqual({
      type: "Row",
      value: {
        id: "nested-row",
        values: [{ type: "Bytea", value: new Uint8Array([0x7f]) }],
      },
    });
  });

  it("serializes write context payloads through collapsed mutation methods", async () => {
    const binding = createBinding();
    const adapter = new JazzRnRuntimeAdapter(binding, {});
    const writeContextJson = JSON.stringify({
      session: { user_id: "alice", claims: {} },
      attribution: "alice",
    });

    const row = adapter.insert(
      "todos",
      { title: { type: "Text", value: "milk" } },
      writeContextJson,
    );
    expect(row).toEqual({ id: "row-1", values: [], batchId: "batch-2" });
    expect(binding.insert).toHaveBeenCalledWith(
      "todos",
      JSON.stringify({ title: { type: "Text", value: "milk" } }),
      writeContextJson,
      undefined,
    );

    const restored = adapter.restore(
      "todos",
      "row-1",
      { title: { type: "Text", value: "eggs" } },
      writeContextJson,
    );
    expect(restored).toEqual({ id: "row-1", values: [], batchId: "batch-restore-2" });
    expect(binding.restore).toHaveBeenCalledWith(
      "todos",
      "row-1",
      JSON.stringify({ title: { type: "Text", value: "eggs" } }),
      writeContextJson,
    );

    adapter.update("row-1", { done: { type: "Boolean", value: true } }, writeContextJson);
    expect(binding.update).toHaveBeenCalledWith(
      "row-1",
      JSON.stringify({ done: { type: "Boolean", value: true } }),
      writeContextJson,
    );

    adapter.delete("row-1", writeContextJson);
    expect(binding.delete_).toHaveBeenCalledWith("row-1", writeContextJson);
  });

  it("bridges 2-phase createSubscription + executeSubscription with handle conversion", () => {
    const binding = createBinding();
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    const handle = adapter.createSubscription("{}", null, null);
    expect(handle).toBe(9);
    expect(binding.createSubscription).toHaveBeenCalledWith("{}", undefined, undefined);

    const onUpdate = vi.fn();
    adapter.executeSubscription(handle, onUpdate);

    const executeMock = binding.executeSubscription as ReturnType<typeof vi.fn>;
    expect(executeMock).toHaveBeenCalledTimes(1);
    expect(executeMock.mock.calls[0]![0]).toBe(9n);

    const callbackObject = executeMock.mock.calls[0]![1];
    callbackObject.onUpdate('{"added":[],"removed":[],"updated":[],"pending":false}');
    expect(onUpdate).toHaveBeenCalledWith({
      added: [],
      removed: [],
      updated: [],
      pending: false,
    });

    adapter.unsubscribe(handle);
    expect(binding.unsubscribe).toHaveBeenCalledWith(9n);
  });

  it("swallows exceptions thrown by subscription callbacks crossing the native boundary", () => {
    const binding = createBinding();
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    const onUpdate = vi.fn(() => {
      throw new Error("sub boom");
    });
    const handle = adapter.createSubscription("{}", null, null);
    adapter.executeSubscription(handle, onUpdate);
    const executeMock = binding.executeSubscription as ReturnType<typeof vi.fn>;
    const subscriptionCallback = executeMock.mock.calls[0]![1];
    expect(() => subscriptionCallback.onUpdate("[]")).not.toThrow();
  });

  it("passes canonical subscription tuple updates through unchanged", () => {
    const binding = createBinding();
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    const onUpdate = vi.fn();
    const handle = adapter.createSubscription("{}", null, null);
    adapter.executeSubscription(handle, onUpdate);
    const executeMock = binding.executeSubscription as ReturnType<typeof vi.fn>;
    const subscriptionCallback = executeMock.mock.calls[0]![1];

    subscriptionCallback.onUpdate(
      JSON.stringify({
        added: [],
        removed: [],
        updated: [
          [
            { id: "row-u", values: [{ type: "Text", value: "before" }] },
            { id: "row-u", values: [{ type: "Text", value: "after" }] },
          ],
        ],
        pending: false,
      }),
    );

    expect(onUpdate).toHaveBeenCalledWith({
      added: [],
      removed: [],
      updated: [
        [
          { id: "row-u", values: [{ type: "Text", value: "before" }] },
          { id: "row-u", values: [{ type: "Text", value: "after" }] },
        ],
      ],
      pending: false,
    });
  });

  it("wraps Jazz RN errors with error name and cause", async () => {
    const runtimeError = {
      tag: "Runtime",
      inner: {
        message: "indexed value too large",
      },
    };
    const binding = createBinding({
      insert: vi.fn(() => {
        throw runtimeError;
      }),
      query: vi.fn(() => Promise.reject(runtimeError)),
      update: vi.fn(() => {
        throw runtimeError;
      }),
      delete_: vi.fn(() => {
        throw runtimeError;
      }),
    });
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    const insertError = (() => {
      try {
        adapter.insert("todos", {});
        return null;
      } catch (error) {
        return error;
      }
    })();
    expect(insertError).toBeInstanceOf(Error);
    expect((insertError as Error).name).toBe("JazzRnRuntimeError");
    expect((insertError as Error).message).toBe("indexed value too large");
    expect((insertError as Error & { cause?: unknown }).cause).toBe(runtimeError);
    expect((insertError as Error & { tag?: unknown }).tag).toBe("Runtime");

    const queryError = await adapter.query("{}", null, null).catch((error: unknown) => error);
    expect(queryError).toBeInstanceOf(Error);
    expect((queryError as Error).name).toBe("JazzRnRuntimeError");
    expect((queryError as Error).message).toBe("indexed value too large");
    expect((queryError as Error & { cause?: unknown }).cause).toBe(runtimeError);
    expect((queryError as Error & { tag?: unknown }).tag).toBe("Runtime");

    const updateError = (() => {
      try {
        adapter.update("row-1", { done: { type: "Boolean", value: true } });
        return null;
      } catch (error) {
        return error;
      }
    })();
    expect(updateError).toBeInstanceOf(Error);
    expect((updateError as Error).name).toBe("JazzRnRuntimeError");
    expect((updateError as Error).message).toBe("indexed value too large");
    expect((updateError as Error & { cause?: unknown }).cause).toBe(runtimeError);
    expect((updateError as Error & { tag?: unknown }).tag).toBe("Runtime");

    const deleteError = (() => {
      try {
        adapter.delete("row-1");
        return null;
      } catch (error) {
        return error;
      }
    })();
    expect(deleteError).toBeInstanceOf(Error);
    expect((deleteError as Error).name).toBe("JazzRnRuntimeError");
    expect((deleteError as Error).message).toBe("indexed value too large");
    expect((deleteError as Error & { cause?: unknown }).cause).toBe(runtimeError);
    expect((deleteError as Error & { tag?: unknown }).tag).toBe("Runtime");
  });

  it("does not wrap non-Jazz errors", () => {
    const binding = createBinding({
      insert: vi.fn(() => {
        throw new Error("plain failure");
      }),
    });
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    const error = (() => {
      try {
        adapter.insert("todos", {});
        return null;
      } catch (caught) {
        return caught;
      }
    })();

    expect(error).toBeInstanceOf(Error);
    expect((error as Error).name).toBe("Error");
    expect((error as Error).message).toBe("plain failure");
    expect((error as Error & { tag?: unknown }).tag).toBeUndefined();
  });

  it("derives error name for non-runtime Jazz tags", () => {
    const schemaError = {
      tag: "Schema",
      inner: {
        message: "schema mismatch",
      },
    };
    const binding = createBinding({
      insert: vi.fn(() => {
        throw schemaError;
      }),
    });
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    const error = (() => {
      try {
        adapter.insert("todos", {});
        return null;
      } catch (caught) {
        return caught;
      }
    })();

    expect(error).toBeInstanceOf(Error);
    expect((error as Error).name).toBe("JazzRnSchemaError");
    expect((error as Error).message).toBe("schema mismatch");
    expect((error as Error & { tag?: unknown }).tag).toBe("Schema");
    expect((error as Error & { cause?: unknown }).cause).toBe(schemaError);
  });

  it("no-ops close after close", () => {
    const binding = createBinding();
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    adapter.close();
    adapter.close();

    expect(binding.close).toHaveBeenCalledTimes(1);
  });

  it("forwards updateAuth JSON payload to the native binding", () => {
    const updateAuth = vi.fn();
    const binding = createBinding({ updateAuth });
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    adapter.updateAuth(JSON.stringify({ jwt_token: "refreshed" }));

    expect(updateAuth).toHaveBeenCalledWith(JSON.stringify({ jwt_token: "refreshed" }));
  });

  it("registers onAuthFailure callback with the native binding and invokes it on failure", () => {
    let captured: { onFailure: (reason: string) => void } | null = null;
    const onAuthFailure = vi.fn((cb: { onFailure: (reason: string) => void }) => {
      captured = cb;
    });
    const binding = createBinding({ onAuthFailure });
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    const listener = vi.fn();
    adapter.onAuthFailure(listener);

    expect(onAuthFailure).toHaveBeenCalledTimes(1);
    expect(captured).not.toBeNull();
    expect(captured!.onFailure).toBeInstanceOf(Function);

    captured!.onFailure("token expired");
    expect(listener).toHaveBeenCalledWith("token expired");
  });

  it("bridges mutation error callback and batch sealing", () => {
    let capturedMutationError: { onError: (eventJson: string) => void } | null = null;
    const binding = createBinding({
      onMutationError: vi.fn((callback: { onError: (eventJson: string) => void }) => {
        capturedMutationError = callback;
      }),
      commitBatch: vi.fn(),
    });
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    const mutationErrorListener = vi.fn();
    adapter.onMutationError(mutationErrorListener);
    capturedMutationError!.onError(
      JSON.stringify({
        code: "WriteRejected",
        reason: "nope",
        batch: {
          batchId: "batch-1",
          latestSettlement: {
            kind: "rejected",
            code: "WriteRejected",
            reason: "nope",
          },
        },
      }),
    );
    adapter.commitBatch("batch-1");

    expect(binding.onMutationError).toHaveBeenCalledTimes(1);
    expect(mutationErrorListener).toHaveBeenCalledWith({
      code: "WriteRejected",
      reason: "nope",
      batch: {
        batchId: "batch-1",
        latestSettlement: {
          kind: "rejected",
          code: "WriteRejected",
          reason: "nope",
        },
      },
    });
    expect(binding.commitBatch).toHaveBeenCalledWith("batch-1");
  });
});
