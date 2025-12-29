import grpc from "k6/net/grpc";
import { check, sleep } from "k6";
import { Rate } from "k6/metrics";
import encoding from "k6/encoding";

/**
 * k6 gRPC benchmark for kvdb.gateway.KvGateway (Get/Put/Delete)
 *
 * Run from repo root:
 *   k6 run gateway_bench.js
 *
 * Common overrides:
 *   TARGET=localhost:7000 PROTO_DIR=./kv.proto/src/main/proto k6 run gateway_bench.js
 *   WRITE_PCT=30 DELETE_PCT=5 KEYSPACE=20000 VALUE_BYTES=512 k6 run gateway_bench.js
 *   PRELOAD=true PRELOAD_KEYS=5000 k6 run gateway_bench.js
 */

// --------------------
// Config (env override)
// --------------------
const TARGET = __ENV.TARGET || "localhost:7000";
// k6 resolves relative paths from this script directory (benchmark/k6/)
const PROTO_DIR = __ENV.PROTO_DIR || "../../kv.proto/src/main/proto";
const PROTO_FILE = __ENV.PROTO_FILE || "kvgateway.proto";

const WRITE_PCT = Number(__ENV.WRITE_PCT || 20);  // PUT percentage
const DELETE_PCT = Number(__ENV.DELETE_PCT || 0); // DELETE percentage (from remaining)
const KEYSPACE = Number(__ENV.KEYSPACE || 10000);
const VALUE_BYTES = Number(__ENV.VALUE_BYTES || 256);

const PRELOAD = (__ENV.PRELOAD || "false").toLowerCase() === "true";
const PRELOAD_KEYS = Number(__ENV.PRELOAD_KEYS || Math.min(KEYSPACE, 2000));

const DEADLINE_MS = Number(__ENV.DEADLINE_MS || 1500);
const SLEEP_MS = Number(__ENV.SLEEP_MS || 10);

// Consistency / durability knobs (defaults are reasonable)
const CONSISTENCY = (__ENV.CONSISTENCY || "STRONG").toUpperCase(); // STRONG | EVENTUAL
const DURABILITY = (__ENV.DURABILITY || "WAL_SYNC").toUpperCase(); // WAL_SYNC | QUORUM_SYNC | WAL_ASYNC
const REQUIRE_IDEMPOTENCY = (__ENV.REQUIRE_IDEMPOTENCY || "true").toLowerCase() === "true";

// Custom failure metric (k6 versions differ on built-in gRPC failure metric availability)
const grpc_failures = new Rate("grpc_failures");

// --------------------
// Enum numeric values (from proto)
// --------------------
const StatusCode = {
  CODE_UNSPECIFIED: 0,
  OK: 1,
  INVALID_ARGUMENT: 2,
  NOT_FOUND: 3,
  ALREADY_EXISTS: 4,
  VERSION_MISMATCH: 5,
  PRECONDITION_FAILED: 6,
  RATE_LIMITED: 7,
  PAYLOAD_TOO_LARGE: 8,
  UNAVAILABLE: 9,
  TIMEOUT: 10,
  INTERNAL: 11,
  NOT_LEADER: 12,
  SHARD_MOVED: 13,
};

function normalizeStatusCode(code) {
  if (code === null || code === undefined) return null;
  if (typeof code === "number") return code;
  if (typeof code === "string") {
    const trimmed = code.trim();
    if (StatusCode[trimmed] !== undefined) return StatusCode[trimmed];
  }
  return null;
}

function statusMessage(res) {
  const st = res?.message?.status;
  const raw = st?.code;
  const norm = normalizeStatusCode(raw);
  return `raw=${raw} (${typeof raw}) norm=${norm} msg=${st?.message} leader=${st?.leader_hint} shard=${st?.shard_id} retryAfterMs=${st?.retry_after_ms}`;
}

// --------------------
// k6 options
// --------------------
export const options = {
  scenarios: {
    gateway: {
      executor: "ramping-vus",
      startVUs: 1,
      stages: [
        { duration: "10s", target: 10 },
        { duration: "30s", target: 50 },
        { duration: "10s", target: 0 },
      ],
      gracefulRampDown: "5s",
    },
  },
  thresholds: {
    grpc_failures: ["rate<0.01"],      // < 1% failures (custom)
    grpc_req_duration: ["p(95)<75"],   // tune for local machine
  },
};

// --------------------
// Helpers
// --------------------
function randInt(maxExclusive) {
  return Math.floor(Math.random() * maxExclusive);
}

function keyString() {
  // bounded keyspace, stable distribution
  return `k-${randInt(KEYSPACE)}`;
}

// k6 gRPC expects proto `bytes` fields as base64-encoded strings.
function toBytes(s) {
  // Our keys are ASCII; base64-encoding the string is sufficient.
  return encoding.b64encode(s);
}

function makeValueBytes(n) {
  // Build deterministic binary payload and base64-encode it for proto `bytes`.
  const u8 = new Uint8Array(n);
  u8.fill(120); // 'x'
  return encoding.b64encode(u8);
}

function nowMs() {
  return Date.now();
}

function requestContext() {
  return {
    request_id: `req-${__VU}-${__ITER}-${nowMs()}`,
    tenant_id: "dev",
    principal: "k6",
    traceparent: "",
  };
}

function readOptions() {
  return {
    consistency: CONSISTENCY, // e.g. "STRONG" | "EVENTUAL"
    read_mode: "READ_YOUR_WRITES",
    max_staleness_ms: 0,
  };
}

function writeOptions() {
  return {
    durability: DURABILITY, // e.g. "WAL_SYNC" | "QUORUM_SYNC" | "WAL_ASYNC"
    require_idempotency: REQUIRE_IDEMPOTENCY,
    ttl_ms: 0,
    if_version_equals: 0,
    if_not_exists: false,
  };
}

// --------------------
// gRPC client
// --------------------
const client = new grpc.Client();
const SERVICE = "kvdb.gateway.KvGateway";
let connected = false;
// k6 requires proto loading in the init context (top-level)
client.load([PROTO_DIR], PROTO_FILE);

function connectOnce() {
  if (connected) return;
  client.connect(TARGET, { plaintext: true });
  connected = true;
}

// --------------------
// Optional preload to seed keys
// --------------------
export function setup() {
  if (!PRELOAD) return { preloaded: false };

  connectOnce();

  const value = makeValueBytes(VALUE_BYTES);
  let ok = 0;

  for (let i = 0; i < PRELOAD_KEYS; i++) {
    const key = toBytes(`k-${i}`); // deterministic seed keys
    const res = client.invoke(`${SERVICE}/Put`, {
      ctx: { request_id: `preload-${i}`, tenant_id: "dev", principal: "k6", traceparent: "" },
      key: key,
      value: value,
      options: writeOptions(),
    }, { timeout: DEADLINE_MS });

    if (res && res.status === grpc.StatusOK && normalizeStatusCode(res.message?.status?.code) === StatusCode.OK) ok++;
  }

  client.close();
  connected = false;
  return { preloaded: true, preload_ok: ok, preload_keys: PRELOAD_KEYS };
}

export default function () {
  connectOnce();

  const roll = randInt(100);
  const ks = keyString();
  const debugOnce = (__VU === 1 && __ITER === 0);

  // Weighted operation selection:
  // - PUT: WRITE_PCT
  // - DELETE: DELETE_PCT (from remaining)
  // - GET: otherwise
  if (roll < WRITE_PCT) {
    const res = client.invoke(`${SERVICE}/Put`, {
      ctx: requestContext(),
      key: toBytes(ks),
      value: makeValueBytes(VALUE_BYTES),
      options: writeOptions(),
    }, { timeout: DEADLINE_MS });

    if (debugOnce) console.log(`PUT ${statusMessage(res)}`);

    check(res, {
      "PUT grpc OK": (r) => r && r.status === grpc.StatusOK,
      "PUT app OK-ish": (r) => {
        const code = normalizeStatusCode(r?.message?.status?.code);
        // treat OK as success; tolerate retryable codes without failing the whole run
        return code === StatusCode.OK ||
          code === StatusCode.NOT_LEADER ||
          code === StatusCode.SHARD_MOVED ||
          code === StatusCode.UNAVAILABLE ||
          code === StatusCode.TIMEOUT;
      },
    });

    const putCode = normalizeStatusCode(res?.message?.status?.code);
    const putOk = res && res.status === grpc.StatusOK && (
      putCode === StatusCode.OK ||
      putCode === StatusCode.NOT_LEADER ||
      putCode === StatusCode.SHARD_MOVED ||
      putCode === StatusCode.UNAVAILABLE ||
      putCode === StatusCode.TIMEOUT
    );
    grpc_failures.add(!putOk);

  } else if (DELETE_PCT > 0 && roll < WRITE_PCT + DELETE_PCT) {
    const res = client.invoke(`${SERVICE}/Delete`, {
      ctx: requestContext(),
      key: toBytes(ks),
      options: writeOptions(),
    }, { timeout: DEADLINE_MS });

    if (debugOnce) console.log(`DELETE ${statusMessage(res)}`);

    check(res, {
      "DELETE grpc OK": (r) => r && r.status === grpc.StatusOK,
      "DELETE app OK-ish": (r) => {
        const code = normalizeStatusCode(r?.message?.status?.code);
        return code === StatusCode.OK ||
          code === StatusCode.NOT_FOUND || // deleting a missing key may be acceptable
          code === StatusCode.NOT_LEADER ||
          code === StatusCode.SHARD_MOVED ||
          code === StatusCode.UNAVAILABLE ||
          code === StatusCode.TIMEOUT;
      },
    });

    const delCode = normalizeStatusCode(res?.message?.status?.code);
    const delOk = res && res.status === grpc.StatusOK && (
      delCode === StatusCode.OK ||
      delCode === StatusCode.NOT_FOUND ||
      delCode === StatusCode.NOT_LEADER ||
      delCode === StatusCode.SHARD_MOVED ||
      delCode === StatusCode.UNAVAILABLE ||
      delCode === StatusCode.TIMEOUT
    );
    grpc_failures.add(!delOk);

  } else {
    const res = client.invoke(`${SERVICE}/Get`, {
      ctx: requestContext(), // strongly recommended for reads per proto comment
      key: toBytes(ks),
      options: readOptions(),
      head_only: false,
    }, { timeout: DEADLINE_MS });

    if (debugOnce) console.log(`GET ${statusMessage(res)}`);

    check(res, {
      "GET grpc OK": (r) => r && r.status === grpc.StatusOK,
      "GET app OK or NOT_FOUND": (r) => {
        const code = normalizeStatusCode(r?.message?.status?.code);
        return code === StatusCode.OK ||
          code === StatusCode.NOT_FOUND ||
          code === StatusCode.NOT_LEADER ||
          code === StatusCode.SHARD_MOVED ||
          code === StatusCode.UNAVAILABLE ||
          code === StatusCode.TIMEOUT;
      },
    });

    const getCode = normalizeStatusCode(res?.message?.status?.code);
    const getOk = res && res.status === grpc.StatusOK && (
      getCode === StatusCode.OK ||
      getCode === StatusCode.NOT_FOUND ||
      getCode === StatusCode.NOT_LEADER ||
      getCode === StatusCode.SHARD_MOVED ||
      getCode === StatusCode.UNAVAILABLE ||
      getCode === StatusCode.TIMEOUT
    );
    grpc_failures.add(!getOk);
  }

  sleep(SLEEP_MS / 1000.0);
}

export function teardown() {
  if (connected) {
    client.close();
    connected = false;
  }
}