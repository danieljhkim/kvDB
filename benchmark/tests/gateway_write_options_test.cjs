const assert = require("node:assert/strict");
const { readFile } = require("node:fs/promises");
const path = require("node:path");
const { test } = require("node:test");

// gateway_bench.js imports k6-only modules and reads k6 globals at init, so Node cannot load it
// as-is. The harness below replaces those with inline stubs and a fake gRPC client, then records
// every invoke so the assertions inspect the payloads the script would actually send.

const dataUrl = (source) => `data:text/javascript;base64,${Buffer.from(source).toString("base64")}`;

async function readBenchSource(name) {
  return readFile(path.join(__dirname, "../k6", name), "utf8");
}

let loadCount = 0;

async function loadGatewayBench(env) {
  const invokes = [];

  class FakeClient {
    load() {}
    connect() {}
    close() {}
    invoke(method, request) {
      invokes.push({ method, request });
      return { status: 0, message: { status: { code: 1 } } };
    }
  }

  const [readOptions, authOptions, bench] = await Promise.all([
    readBenchSource("read_options.js"),
    readBenchSource("auth_options.js"),
    readBenchSource("gateway_bench.js"),
  ]);

  const prelude = [
    `import { buildGetPayload, validateBenchmarkOptions } from ${JSON.stringify(dataUrl(readOptions))};`,
    `import { buildConnectParams, buildInvokeParams, buildRequestContext, resolveClientAuth } from ${JSON.stringify(dataUrl(authOptions))};`,
    "const __k6 = globalThis.__gatewayBenchHarness;",
    "const __ENV = __k6.env;",
    "const __VU = 2; // VU 1 iteration 0 prints debug output",
    "const __ITER = 0;",
    "const open = () => { throw new Error('open() is not stubbed'); };",
    "const grpc = { StatusOK: 0, Client: __k6.Client };",
    "const check = () => true;",
    "const sleep = () => {};",
    "class Rate { add() {} }",
    "const encoding = { b64encode: (value) => Buffer.from(value).toString('base64') };",
  ].join("\n");

  // Each load gets a unique module URL so top-level env reads run again instead of hitting the cache.
  const body = bench
    .split("\n")
    .filter((line) => !line.startsWith("import "))
    .join("\n");
  const moduleUrl = dataUrl(`${prelude}\n${body}\n// load ${++loadCount}`);

  globalThis.__gatewayBenchHarness = {
    env: {
      KVDB_GRPC_SECURITY_MODE: "development-plaintext",
      KVDB_ENV: "local",
      ...env,
    },
    Client: FakeClient,
  };
  try {
    const module = await import(moduleUrl);
    return { module, invokes };
  } finally {
    delete globalThis.__gatewayBenchHarness;
  }
}

function writesAndDeletes(invokes) {
  return invokes.filter(({ method }) => method.endsWith("/Put") || method.endsWith("/Delete"));
}

test("default preload, Put and Delete payloads omit if_version_equals", async () => {
  const preload = await loadGatewayBench({ PRELOAD: "true", PRELOAD_KEYS: "2" });
  await preload.module.setup();

  const put = await loadGatewayBench({ WRITE_PCT: "100", DELETE_PCT: "0" });
  put.module.default();

  const deletion = await loadGatewayBench({ WRITE_PCT: "0", DELETE_PCT: "100" });
  deletion.module.default();

  const sent = [...writesAndDeletes(preload.invokes), ...writesAndDeletes(put.invokes), ...writesAndDeletes(deletion.invokes)];
  assert.deepEqual(sent.map(({ method }) => method.split("/").pop()), ["Put", "Put", "Put", "Delete"]);
  for (const { request } of sent) {
    assert.deepEqual(request.options, {
      durability: "WAL_SYNC",
      require_idempotency: true,
      ttl_ms: 0,
      if_not_exists: false,
    });
    assert.equal(Object.hasOwn(request.options, "if_version_equals"), false);
  }
});

test("explicit CAS option sends the expected version, including an intentional zero", async () => {
  for (const expected of ["0", "7"]) {
    const { module, invokes } = await loadGatewayBench({
      PRELOAD: "true",
      PRELOAD_KEYS: "1",
      WRITE_PCT: "50",
      DELETE_PCT: "50",
      WRITE_IF_VERSION_EQUALS: expected,
    });
    await module.setup();
    module.default();
    module.default();

    // One preload Put, then two iterations; WRITE_PCT + DELETE_PCT covers every roll, so each is a write.
    const sent = writesAndDeletes(invokes);
    assert.equal(sent.length, 3, `WRITE_IF_VERSION_EQUALS=${expected}`);
    for (const { request } of sent) {
      assert.equal(Object.hasOwn(request.options, "if_version_equals"), true);
      assert.equal(request.options.if_version_equals, Number(expected));
    }
  }
});

test("a malformed CAS option fails during init", async () => {
  for (const value of ["-1", "abc", "1.5"]) {
    await assert.rejects(
      loadGatewayBench({ WRITE_IF_VERSION_EQUALS: value }),
      /WRITE_IF_VERSION_EQUALS must be a non-negative integer/,
      `WRITE_IF_VERSION_EQUALS=${value}`
    );
  }
});
