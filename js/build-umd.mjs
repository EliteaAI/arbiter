#!/usr/bin/env node
// build-umd.mjs
//
// Generates arbiter.umd.js from arbiter.js (the ES-module source of truth).
//
// The transform is deliberately simple and relies on arbiter.js being
// self-contained (no `import` statements) and exporting only top-level
// declarations via `export class|function|const NAME`:
//   1. collect every exported NAME,
//   2. strip the leading `export ` keyword from each declaration,
//   3. wrap the body in the UMD boilerplate,
//   4. append `return { NAME, ... };`.
//
// Run:  node build-umd.mjs   (from the directory containing arbiter.js)

import { readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const here = dirname(fileURLToPath(import.meta.url));
const SRC = join(here, "arbiter.js");
const OUT = join(here, "arbiter.umd.js");

const source = readFileSync(SRC, "utf8");

// --- Guard: this transform cannot handle `import` statements -----------------
const importLine = source.split("\n").find((l) => /^\s*import\s/.test(l));
if (importLine) {
  console.error(`build-umd: arbiter.js contains an import statement, which the UMD transform does not support:\n  ${importLine.trim()}`);
  process.exit(1);
}

// --- Collect exported names --------------------------------------------------
const EXPORT_DECL = /^export\s+(?:class|function|const|let|var)\s+([A-Za-z_$][\w$]*)/gm;
const names = [];
for (const m of source.matchAll(EXPORT_DECL)) names.push(m[1]);

// Reject `export { ... }` / `export default` — not handled by this simple pass.
if (/^export\s*\{/m.test(source) || /^export\s+default\b/m.test(source)) {
  console.error("build-umd: `export { ... }` and `export default` are not supported; use `export class|function|const NAME`.");
  process.exit(1);
}
if (names.length === 0) {
  console.error("build-umd: no exports found in arbiter.js — nothing to generate.");
  process.exit(1);
}

// --- Strip the `export ` keyword from each declaration -----------------------
let body = source.replace(/^export\s+(class|function|const|let|var)\s/gm, "$1 ");

// Indent the whole body one level so it reads naturally inside the factory.
// Safe because arbiter.js has no multi-line template literals (leading
// whitespace added per line would otherwise leak into string contents).
body = body.split("\n").map((l) => (l.length ? `  ${l}` : l)).join("\n");

const exportsBlock = `  return {\n${names.map((n) => `    ${n},`).join("\n")}\n  };`;

const output = `// arbiter.umd.js
//
// GENERATED FILE — do not edit by hand.
// Produced from arbiter.js by build-umd.mjs. Edit arbiter.js, then re-run:
//     node build-umd.mjs
//
// UMD wrapper: works as a browser global (window.Arbiter), CommonJS (require),
// or AMD (define). For \`import\` / <script type="module">, use arbiter.js.

(function (root, factory) {
  if (typeof define === "function" && define.amd) {
    define([], factory);                    // AMD
  } else if (typeof module === "object" && module.exports) {
    module.exports = factory();             // CommonJS / Node
  } else {
    root.Arbiter = factory();               // browser global
  }
}(typeof self !== "undefined" ? self : this, function () {
  "use strict";

${body}

${exportsBlock}
}));
`;

writeFileSync(OUT, output);
console.log(`build-umd: wrote ${OUT}`);
console.log(`build-umd: exported ${names.length} names: ${names.join(", ")}`);
