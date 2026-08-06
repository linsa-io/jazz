#!/usr/bin/env node
// The uniffi bindings under `crates/jazz-rn/src/generated/` are COMMITTED, not build
// output. Change an FFI signature in the Rust and forget to regenerate, and nothing
// fails: the Rust tests pass, the JS adapter tests pass, and the mismatch only shows up
// on a device, as a checksum error on a black screen — which is what shipped in
// linsa-v14 when `SubscriptionCallback::on_update` grew a blob sidecar.
//
// So regenerate into a scratch directory and compare. The generator reads the signatures
// out of a compiled library, but a HOST build is enough — no iOS cross-compile, no
// xcframework — and `cargo clippy --workspace` in this same hook has already built it.
//
// Only the TypeScript is compared: `cpp/` is not tracked, and the C++ shim never names
// individual callbacks.

import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, readdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = fileURLToPath(new URL("../..", import.meta.url));
const crate = join(repoRoot, "crates/jazz-rn");
const committedDir = join(crate, "src/generated");

function run(command, args, options = {}) {
  return execFileSync(command, args, {
    cwd: repoRoot,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
    ...options,
  });
}

function libraryPath() {
  // `cargo build --message-format` would be exact, but the hook has already built the
  // workspace; look for the artifact the host target produces.
  const candidates = ["libjazz_rn.dylib", "libjazz_rn.so", "jazz_rn.dll"];
  for (const dir of ["debug", "release"]) {
    for (const name of candidates) {
      const candidate = join(repoRoot, "target", dir, name);
      try {
        readFileSync(candidate);
        return candidate;
      } catch {
        // keep looking
      }
    }
  }
  return null;
}

let library = libraryPath();
if (library === null) {
  // Nothing built yet — build just this crate, quietly.
  run("cargo", ["build", "-p", "jazz-rn"], { stdio: "inherit" });
  library = libraryPath();
}
if (library === null) {
  console.error("check-rn-bindings: could not find a built jazz-rn library to read.");
  process.exit(1);
}

const scratch = mkdtempSync(join(tmpdir(), "rn-bindings-"));
try {
  // `ubrn` is a local binary of the jazz-rn package, so it only resolves from there.
  run(
    "npx",
    [
      "ubrn",
      "generate",
      "jsi",
      "bindings",
      // `--library` is a boolean: it says to read definitions out of the positional
      // source, which is the dylib itself.
      "--library",
      "--ts-dir",
      scratch,
      "--cpp-dir",
      join(scratch, "cpp"),
      library,
      // `--library` mode is mutually exclusive with `--config`, and the generator
      // shells out to `cargo metadata`, so run it where the manifest lives.
    ],
    { cwd: join(crate, "rust") },
  );

  // Compare the IDENTIFIERS each file declares, as a sorted multiset.
  //
  // Byte equality is unreachable here: the generator formats with prettier, the
  // committed copies went through a chain this script cannot reproduce, and chasing it
  // means reimplementing someone's line wrapping. Normalising punctuation gets within a
  // few characters but still trips on prettier idioms like a paren around `typeof X`.
  //
  // Identifiers are the right invariant. Nothing an FFI signature change can do avoids
  // them — a new parameter brings its name, a changed type its type name, a renamed
  // method its method name — and no formatter invents or removes one. Counting rather
  // than set-comparing catches a parameter added with a name already used elsewhere.
  const identifiers = (source) =>
    (source.match(/[A-Za-z_$][A-Za-z0-9_$]*/g) ?? []).sort().join(" ");

  const drifted = readdirSync(committedDir)
    .filter((name) => name.endsWith(".ts"))
    .filter((name) => {
      let fresh;
      try {
        fresh = readFileSync(join(scratch, name), "utf8");
      } catch {
        return true; // the generated set changed shape — also drift
      }
      return identifiers(fresh) !== identifiers(readFileSync(join(committedDir, name), "utf8"));
    });

  if (drifted.length > 0) {
    console.error(
      [
        "",
        "The committed uniffi bindings no longer match the Rust FFI surface:",
        ...drifted.map((name) => `  crates/jazz-rn/src/generated/${name}`),
        "",
        "They are tracked files, so a signature change has to carry them. Run:",
        "  pnpm --filter jazz-rn ubrn:ios     # or ubrn:android",
        "and stage the result in the same commit.",
        "",
        "Shipping them stale does not fail any test — it fails at app startup with",
        "an FFI checksum mismatch and a black screen.",
        "",
      ].join("\n"),
    );
    process.exit(1);
  }
} finally {
  rmSync(scratch, { recursive: true, force: true });
}
