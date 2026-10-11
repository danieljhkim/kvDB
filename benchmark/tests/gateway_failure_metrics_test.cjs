const assert = require("node:assert/strict");
const { readFile } = require("node:fs/promises");
const path = require("node:path");
const { test } = require("node:test");

const StatusCode = {
  OK: 1,
  NOT_FOUND: 3,
  UNAVAILABLE: 9,
  TIMEOUT: 10,
  NOT_LEADER: 12,
  SHARD_MOVED: 13,
};
const dataUrl = (source) => `data:text/javascript;base64,${Buffer.from(source).toString("base64")}`;
let loadCount = 0;

async function loadGatewayBench(operation, appStatus) {
  const metrics = {};
  class FakeClient {
    load() {}
    connect() {}
    close() {}
    invoke() {
      return { status: 0, message: { status: { code: appStatus } } };
    }
  }

  const [readOptions, authOptions, bench] = await Promise.all([
    readFile(path.join(__dirname, "../k6/read_options.js"), "utf8"),
    readFile(path.join(__dirname, "../k6/auth_options.js"), "utf8"),
    readFile(path.join(__dirname, "../k6/gateway_bench.js"), "utf8"),
  ]);
  const prelude = [
    `import { buildGetPayload, validateBenchmarkOptions } from ${JSON.stringify(dataUrl(readOptions))};`,
    `import { buildConnectParams, buildInvokeParams, buildRequestContext, resolveClientAuth } from ${JSON.stringify(dataUrl(authOptions))};`,
    "const __k6 = globalThis.__gatewayFailureMetricsHarness;",
    "const __ENV = __k6.env;",
    "const __VU = 2;",
    "const __ITER = 0;",
    "const open = () => { throw new Error('open() is not stubbed'); };",
    "const grpc = { StatusOK: 0, Client: __k6.Client };",
    "const check = () => true;",
    "const sleep = () => {};",
    "class Rate { constructor(name) { this.samples = __k6.metrics[name] = []; } add(value) { this.samples.push(value); } }",
    "const encoding = { b64encode: (value) => Buffer.from(value).toString('base64') };",
  ].join("\n");
  const body = bench.split("\n").filter((line) => !line.startsWith("import ")).join("\n");
  const moduleUrl = dataUrl(`${prelude}\n${body}\n// load ${++loadCount}`);
  const env = {
    KVDB_GRPC_SECURITY_MODE: "development-plaintext",
    KVDB_ENV: "local",
    WRITE_PCT: operation === "PUT" ? "100" : "0",
    DELETE_PCT: operation === "DELETE" ? "100" : "0",
    SLEEP_MS: "0",
  };

  globalThis.__gatewayFailureMetricsHarness = { env, metrics, Client: FakeClient };
  try {
    const module = await import(moduleUrl);
    module.default();
    return metrics;
  } finally {
    delete globalThis.__gatewayFailureMetricsHarness;
  }
}

async function runOperations(status) {
  const samples = [];
  const toleratedNotFound = [];
  for (const operation of ["PUT", "GET", "DELETE"]) {
    const metrics = await loadGatewayBench(operation, status);
    samples.push(...metrics.grpc_failures);
    toleratedNotFound.push(...metrics.tolerated_not_found);
  }
  return { samples, toleratedNotFound };
}

test("retryable unresolved application outcomes fail Put, Get, and Delete", async () => {
  for (const status of [StatusCode.NOT_LEADER, StatusCode.SHARD_MOVED, StatusCode.UNAVAILABLE, StatusCode.TIMEOUT]) {
    const { samples, toleratedNotFound } = await runOperations(status);
    assert.deepEqual(samples, [true, true, true], `status ${status}`);
    assert.deepEqual(toleratedNotFound, [false, false, false], `status ${status}`);
  }
});

test("all-UNAVAILABLE and all-TIMEOUT fixtures fail the rate threshold; all-OK passes", async () => {
  for (const status of [StatusCode.UNAVAILABLE, StatusCode.TIMEOUT]) {
    const { samples } = await runOperations(status);
    const failureRate = samples.filter(Boolean).length / samples.length;
    assert.equal(failureRate, 1, `status ${status}`);
    assert.equal(failureRate < 0.01, false, `status ${status} must fail rate<0.01`);
  }

  const { samples } = await runOperations(StatusCode.OK);
  const failureRate = samples.filter(Boolean).length / samples.length;
  assert.equal(failureRate, 0);
  assert.equal(failureRate < 0.01, true);
});

test("NOT_FOUND remains tolerated only for Get and Delete and is measured separately", async () => {
  const { samples, toleratedNotFound } = await runOperations(StatusCode.NOT_FOUND);
  assert.deepEqual(samples, [true, false, false]);
  assert.deepEqual(toleratedNotFound, [false, true, true]);
});
