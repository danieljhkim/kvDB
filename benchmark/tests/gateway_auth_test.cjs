// Run with: node --test benchmark/tests/gateway_auth_test.cjs
//
// Covers gateway benchmark authentication without a cluster or a load run: the k6 script runs
// against a mocked k6/net/grpc client, and the ghz runner against a fake ghz executable.
const assert = require("node:assert/strict");
const { spawnSync } = require("node:child_process");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { test } = require("node:test");

const BENCHMARK_DIR = path.join(__dirname, "..");
const GHZ_RUNNER = path.join(BENCHMARK_DIR, "scripts/run_ghz_gateway.sh");
const PEM = "-----BEGIN TEST-----\nZmFrZQ==\n-----END TEST-----\n";

function dataUrl(source) {
  return `data:text/javascript;base64,${Buffer.from(source).toString("base64")}`;
}

function tempDir(t) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "gateway-auth-"));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  return dir;
}

function writeTlsFiles(dir) {
  const files = { ca: "ca.pem", cert: "client.crt", key: "client.key" };
  for (const name of Object.values(files)) fs.writeFileSync(path.join(dir, name), PEM);
  return {
    KVDB_CLIENT_TLS_TRUST_BUNDLE: path.join(dir, files.ca),
    KVDB_CLIENT_TLS_CERT_CHAIN: path.join(dir, files.cert),
    KVDB_CLIENT_TLS_PRIVATE_KEY: path.join(dir, files.key),
  };
}

// Mirrors InternalAuthServerInterceptor: parseDevelopmentIdentity + matchesVerifiedIdentity.
function fixtureAcceptsDevelopmentRequest(metadata, ctx) {
  const header = metadata["x-kvdb-development-identity"];
  if (!header) return "development identity is missing";
  const [role, tenant, principal] = header.split("/", 3);
  if (role !== "client" || !tenant || !principal) return "malformed development identity";
  if (ctx.tenant_id && ctx.tenant_id !== tenant) return "tenant conflict";
  if (ctx.principal && ctx.principal !== principal) return "principal conflict";
  return null;
}

async function loadAuthOptions() {
  const source = fs.readFileSync(path.join(BENCHMARK_DIR, "k6/auth_options.js"), "utf8");
  return import(dataUrl(source));
}

// Loads gateway_bench.js with k6/* imports replaced by mocks; returns the module and recorded calls.
async function loadGatewayBench(env) {
  const calls = { connect: [], invoke: [] };
  globalThis.__GATEWAY_BENCH_CALLS = calls;
  globalThis.__ENV = env;
  globalThis.__VU = 1;
  globalThis.__ITER = 0;
  globalThis.open = (file) => fs.readFileSync(file, "utf8");

  // Data URLs are cached by content, so the mock embeds a nonce to bind it to this load's calls.
  const grpcMock = `// ${Math.random()}
    const calls = globalThis.__GATEWAY_BENCH_CALLS;
    export const StatusOK = 0;
    export default { StatusOK, Client: class {
      load() {}
      connect(target, params) { calls.connect.push({ target, params }); }
      invoke(method, request, params) {
        calls.invoke.push({ method, request, params });
        return { status: StatusOK, message: { status: { code: "OK" } } };
      }
      close() {}
    } };`;
  const mocks = {
    "k6/net/grpc": dataUrl(grpcMock),
    "k6": dataUrl("export const check = () => true; export const sleep = () => {};"),
    "k6/metrics": dataUrl("export class Rate { add() {} }"),
    "k6/encoding": dataUrl('export default { b64encode: (v) => Buffer.from(v).toString("base64") };'),
    "./read_options.js": dataUrl(fs.readFileSync(path.join(BENCHMARK_DIR, "k6/read_options.js"), "utf8")),
    "./auth_options.js": dataUrl(fs.readFileSync(path.join(BENCHMARK_DIR, "k6/auth_options.js"), "utf8")),
  };
  let source = fs.readFileSync(path.join(BENCHMARK_DIR, "k6/gateway_bench.js"), "utf8");
  for (const [specifier, url] of Object.entries(mocks)) {
    source = source.replaceAll(`from "${specifier}"`, `from "${url}"`);
  }
  source += `\n// ${Math.random()}\n`;
  return { bench: await import(dataUrl(source)), calls };
}

async function runBench(env, { preload = false } = {}) {
  const { bench, calls } = await loadGatewayBench({
    PRELOAD: preload ? "true" : "false",
    PRELOAD_KEYS: "1",
    WRITE_PCT: "100",
    SLEEP_MS: "0",
    ...env,
  });
  if (preload) bench.setup();
  bench.default();
  bench.teardown();
  return calls;
}

test("auth options default to mTLS and fail closed without every TLS input", async (t) => {
  const { resolveClientAuth } = await loadAuthOptions();
  const defaults = { tenant: "dev", principal: "k6" };
  const readFile = (file) => fs.readFileSync(file, "utf8");

  assert.throws(() => resolveClientAuth({}, defaults, readFile), /KVDB_CLIENT_TLS_TRUST_BUNDLE is required/);

  const tls = writeTlsFiles(tempDir(t));
  for (const missing of Object.keys(tls)) {
    const env = { ...tls };
    delete env[missing];
    assert.throws(() => resolveClientAuth(env, defaults, readFile), new RegExp(`${missing} is required`));
  }
  assert.throws(
    () => resolveClientAuth({ ...tls, KVDB_CLIENT_TLS_PRIVATE_KEY: "/nonexistent/key" }, defaults, readFile),
    /KVDB_CLIENT_TLS_PRIVATE_KEY .* is not readable/
  );
  assert.throws(
    () => resolveClientAuth({ KVDB_GRPC_SECURITY_MODE: "plaintext" }, defaults, readFile),
    /must be mtls or development-plaintext/
  );
});

test("development-plaintext needs an explicit local-development environment", async () => {
  const { resolveClientAuth } = await loadAuthOptions();
  const defaults = { tenant: "dev", principal: "k6" };

  for (const KVDB_ENV of [undefined, "", "production"]) {
    assert.throws(
      () => resolveClientAuth({ KVDB_GRPC_SECURITY_MODE: "development-plaintext", KVDB_ENV }, defaults, () => ""),
      /requires KVDB_ENV to be local, dev, development or test/
    );
  }
  assert.throws(
    () =>
      resolveClientAuth(
        { KVDB_GRPC_SECURITY_MODE: "development-plaintext", KVDB_ENV: "local", KVDB_CLIENT_TENANT_ID: "a/b" },
        defaults,
        () => ""
      ),
    /KVDB_CLIENT_TENANT_ID must be non-empty/
  );
});

test("k6 development-plaintext sends matching identity metadata on every RPC", async () => {
  const env = { KVDB_GRPC_SECURITY_MODE: "development-plaintext", KVDB_ENV: "local" };
  const calls = await runBench(env, { preload: true });

  assert.deepEqual(calls.connect.map((c) => c.params), [{ plaintext: true }, { plaintext: true }]);
  assert.equal(calls.invoke.length, 2); // preload Put + iteration Put
  for (const { request, params } of calls.invoke) {
    assert.deepEqual(params.metadata, { "x-kvdb-development-identity": "client/dev/k6" });
    assert.equal(request.ctx.tenant_id, "dev");
    assert.equal(request.ctx.principal, "k6");
    assert.equal(fixtureAcceptsDevelopmentRequest(params.metadata, request.ctx), null);
  }
});

test("k6 identity overrides apply to both the header and the request context", async () => {
  const calls = await runBench({
    KVDB_GRPC_SECURITY_MODE: "development-plaintext",
    KVDB_ENV: "test",
    KVDB_CLIENT_TENANT_ID: "tenant-1",
    KVDB_CLIENT_PRINCIPAL: "bench-1",
  });
  const [{ request, params }] = calls.invoke;
  assert.equal(params.metadata["x-kvdb-development-identity"], "client/tenant-1/bench-1");
  assert.equal(fixtureAcceptsDevelopmentRequest(params.metadata, request.ctx), null);
});

test("k6 mTLS passes trust, certificate and key without plaintext or identity headers", async (t) => {
  const tls = writeTlsFiles(tempDir(t));
  const calls = await runBench({ ...tls });

  assert.equal(calls.connect.length, 1);
  assert.deepEqual(calls.connect[0].params, { tls: { cacerts: [PEM], cert: PEM, key: PEM } });
  assert.equal("plaintext" in calls.connect[0].params, false);
  for (const { request, params } of calls.invoke) {
    assert.deepEqual(params.metadata, {});
    // The certificate carries the identity; unconfigured ctx claims are omitted so they cannot conflict.
    assert.equal("tenant_id" in request.ctx, false);
    assert.equal("principal" in request.ctx, false);
  }
});

test("k6 mTLS sends only an explicitly configured ctx identity", async (t) => {
  const tls = writeTlsFiles(tempDir(t));
  const calls = await runBench({ ...tls, KVDB_CLIENT_TENANT_ID: "tenant-1", KVDB_CLIENT_PRINCIPAL: "bench-1" });
  assert.equal(calls.invoke[0].request.ctx.tenant_id, "tenant-1");
  assert.equal(calls.invoke[0].request.ctx.principal, "bench-1");
});

test("k6 fails during init when mTLS inputs are missing", async () => {
  await assert.rejects(loadGatewayBench({}), /KVDB_CLIENT_TLS_TRUST_BUNDLE is required/);
});

function fakeGhz(t) {
  const dir = tempDir(t);
  const bin = path.join(dir, "bin");
  fs.mkdirSync(bin);
  const log = path.join(dir, "ghz-calls.jsonl");
  fs.writeFileSync(
    path.join(bin, "ghz"),
    `#!${process.execPath}\nrequire("node:fs").appendFileSync(${JSON.stringify(log)}, JSON.stringify(process.argv.slice(2)) + "\\n");\n`,
    { mode: 0o755 }
  );
  const run = (env) => {
    const completed = spawnSync(GHZ_RUNNER, [], {
      env: {
        PATH: `${bin}${path.delimiter}${process.env.PATH}`,
        HOME: process.env.HOME,
        RESULTS_DIR: path.join(dir, "results"),
        TOTAL: "1",
        CONCURRENCY: "1",
        TARGET: "127.0.0.1:1",
        ...env,
      },
      encoding: "utf8",
    });
    const calls = fs.existsSync(log)
      ? fs.readFileSync(log, "utf8").trim().split("\n").filter(Boolean).map((line) => JSON.parse(line))
      : [];
    return { completed, calls };
  };
  return { dir, run };
}

function flagValue(args, flag) {
  const index = args.indexOf(flag);
  return index === -1 ? undefined : args[index + 1];
}

function dataPayload(args) {
  return JSON.parse(flagValue(args, "-d"));
}

test("ghz development-plaintext sends matching identity metadata", (t) => {
  const { completed, calls } = fakeGhz(t).run({ KVDB_GRPC_SECURITY_MODE: "development-plaintext", KVDB_ENV: "local" });

  assert.equal(completed.status, 0, completed.stderr);
  assert.equal(calls.length, 2); // Put then Get
  for (const args of calls) {
    assert.ok(args.includes("--insecure"));
    const metadata = JSON.parse(flagValue(args, "-m"));
    assert.deepEqual(metadata, { "x-kvdb-development-identity": "client/dev/ghz" });
    assert.equal(fixtureAcceptsDevelopmentRequest(metadata, dataPayload(args).ctx), null);
  }
});

test("ghz identity overrides apply to both the header and the request context", (t) => {
  const { completed, calls } = fakeGhz(t).run({
    KVDB_GRPC_SECURITY_MODE: "development-plaintext",
    KVDB_ENV: "dev",
    KVDB_CLIENT_TENANT_ID: "tenant-1",
    KVDB_CLIENT_PRINCIPAL: "bench-1",
  });

  assert.equal(completed.status, 0, completed.stderr);
  for (const args of calls) {
    const metadata = JSON.parse(flagValue(args, "-m"));
    assert.equal(metadata["x-kvdb-development-identity"], "client/tenant-1/bench-1");
    assert.equal(fixtureAcceptsDevelopmentRequest(metadata, dataPayload(args).ctx), null);
  }
});

test("ghz mTLS passes trust, certificate and key without --insecure", (t) => {
  const harness = fakeGhz(t);
  const tls = writeTlsFiles(harness.dir);
  const { completed, calls } = harness.run({ ...tls, KVDB_CLIENT_TLS_SERVER_NAME: "gateway.internal" });

  assert.equal(completed.status, 0, completed.stderr);
  assert.equal(calls.length, 2);
  for (const args of calls) {
    assert.equal(args.includes("--insecure"), false);
    assert.equal(flagValue(args, "-m"), undefined);
    assert.equal(flagValue(args, "--cacert"), tls.KVDB_CLIENT_TLS_TRUST_BUNDLE);
    assert.equal(flagValue(args, "--cert"), tls.KVDB_CLIENT_TLS_CERT_CHAIN);
    assert.equal(flagValue(args, "--key"), tls.KVDB_CLIENT_TLS_PRIVATE_KEY);
    assert.equal(flagValue(args, "--cname"), "gateway.internal");
    const { ctx } = dataPayload(args);
    assert.equal("tenant_id" in ctx, false);
    assert.equal("principal" in ctx, false);
  }
});

test("ghz fails closed before invoking ghz or writing results", (t) => {
  const harness = fakeGhz(t);
  const tls = writeTlsFiles(harness.dir);
  const cases = [
    [{}, /KVDB_CLIENT_TLS_TRUST_BUNDLE is required/],
    ...Object.keys(tls).map((missing) => {
      const env = { ...tls };
      delete env[missing];
      return [env, new RegExp(`${missing} is required`)];
    }),
    [{ ...tls, KVDB_CLIENT_TLS_PRIVATE_KEY: path.join(harness.dir, "absent.key") }, /is not a readable file/],
    [{ KVDB_GRPC_SECURITY_MODE: "plaintext" }, /must be mtls or development-plaintext/],
    [{ KVDB_GRPC_SECURITY_MODE: "development-plaintext" }, /requires KVDB_ENV/],
    [{ KVDB_GRPC_SECURITY_MODE: "development-plaintext", KVDB_ENV: "prod" }, /requires KVDB_ENV/],
    [
      { KVDB_GRPC_SECURITY_MODE: "development-plaintext", KVDB_ENV: "local", KVDB_CLIENT_PRINCIPAL: 'x","y' },
      /KVDB_CLIENT_PRINCIPAL must be non-empty/,
    ],
  ];
  for (const [env, message] of cases) {
    const { completed, calls } = harness.run(env);
    assert.notEqual(completed.status, 0, JSON.stringify(env));
    assert.match(completed.stderr, message);
    assert.deepEqual(calls, []);
  }
  assert.equal(fs.existsSync(path.join(harness.dir, "results")), false);
});
