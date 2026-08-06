// Rewrites the manifest of a package directory just before it is packed for a release.
//
// Run as: node .github/scripts/pack-fixup.js <package-dir>
//
// A real file rather than `node -e '...'` inside the workflow: an apostrophe in a JS
// comment once closed the shell quote and bash tried to run the JavaScript, and moving it
// to a heredoc then broke the YAML block scalar because the terminator has to sit in
// column zero. A committed script has neither problem and can be reviewed on its own.
const fs = require("node:fs");

const dir = process.argv[2];
if (!dir) {
  console.error("usage: pack-fixup.js <package-dir>");
  process.exit(1);
}

const manifest = `${dir}/package.json`;
const pkg = JSON.parse(fs.readFileSync(manifest, "utf8"));

// The artifacts arrive already built. jazz-rn's `prepare` runs `bob build`, which is not
// installed in the release job, and `npm pack --ignore-scripts` did not stop npm from
// running it. Dropping these is right for a published manifest anyway: whoever installs
// the tarball has no build toolchain either.
if (pkg.scripts) {
  for (const script of ["prepare", "prepack", "prepublishOnly", "postinstall"]) {
    delete pkg.scripts[script];
  }
}

// `npm pack` keeps `workspace:*` ranges verbatim — only `pnpm publish` rewrites them — so
// the tarball was uninstallable outside the jazz monorepo: pnpm reported "no package named
// jazz-wasm is present in the workspace". Pin them to the version of this build.
for (const field of [
  "dependencies",
  "devDependencies",
  "peerDependencies",
  "optionalDependencies",
]) {
  const deps = pkg[field];
  if (!deps) continue;
  for (const [name, range] of Object.entries(deps)) {
    if (typeof range === "string" && range.startsWith("workspace:")) {
      deps[name] = pkg.version;
    }
  }
}

fs.writeFileSync(manifest, `${JSON.stringify(pkg, null, 2)}\n`);
