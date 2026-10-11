const assert = require("node:assert/strict");
const { readFile } = require("node:fs/promises");
const path = require("node:path");
const { test } = require("node:test");

async function loadReadOptions() {
  const sourcePath = path.join(__dirname, "../k6/read_options.js");
  const source = await readFile(sourcePath, "utf8");
  const moduleUrl = `data:text/javascript;base64,${Buffer.from(source).toString("base64")}`;
  return import(moduleUrl);
}

test("GET payload options match the gateway contract for both consistency modes", async () => {
  const { buildGetPayload, validateBenchmarkOptions } = await loadReadOptions();

  assert.deepEqual(validateBenchmarkOptions("STRONG", "WAL_SYNC"), {
    consistency: "STRONG",
    read_mode: "READ_YOUR_WRITES",
    max_staleness_ms: 0,
  });
  assert.deepEqual(buildGetPayload({ request_id: "strong" }, "key-1", "STRONG"), {
    ctx: { request_id: "strong" },
    key: "key-1",
    options: {
      consistency: "STRONG",
      read_mode: "READ_YOUR_WRITES",
      max_staleness_ms: 0,
    },
    head_only: false,
  });
  assert.deepEqual(buildGetPayload({ request_id: "eventual" }, "key-2", "EVENTUAL"), {
    ctx: { request_id: "eventual" },
    key: "key-2",
    options: {
      consistency: "EVENTUAL",
      read_mode: "LOW_LATENCY",
      max_staleness_ms: 0,
    },
    head_only: false,
  });
});

test("unsupported user overrides fail during init validation", async () => {
  const { buildReadOptions, validateBenchmarkOptions } = await loadReadOptions();

  assert.throws(() => buildReadOptions("EVENTUAL", "READ_YOUR_WRITES"), /READ_YOUR_WRITES is not valid for EVENTUAL consistency/);
  assert.throws(() => validateBenchmarkOptions("UNKNOWN", "WAL_SYNC"), /CONSISTENCY must be STRONG or EVENTUAL/);
  assert.throws(() => validateBenchmarkOptions("EVENTUAL", "WAL_ASYNC"), /gateway does not support "WAL_ASYNC"/);
  assert.throws(() => validateBenchmarkOptions("STRONG", "UNKNOWN"), /DURABILITY must be WAL_SYNC or QUORUM_SYNC/);
});
