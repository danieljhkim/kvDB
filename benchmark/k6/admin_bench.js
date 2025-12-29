import http from "k6/http";
import { check, sleep, group } from "k6";

export const options = {
  scenarios: {
    admin_api: {
      executor: "ramping-vus",
      startVUs: 1,
      stages: [
        { duration: "10s", target: 5 },
        { duration: "20s", target: 25 },
        { duration: "10s", target: 0 },
      ],
      gracefulRampDown: "5s",
    },
  },
  thresholds: {
    http_req_failed: ["rate<0.01"],
    http_req_duration: ["p(95)<750"],
  },
};

const BASE_URL = __ENV.ADMIN_BASE_URL || __ENV.BASE_URL || "http://localhost:8089";
const API_KEY = __ENV.K6_API_KEY || "dev-secret";

// Typically shard-init is a one-time bootstrap; leave off for load runs.
const RUN_SHARD_INIT = (__ENV.RUN_SHARD_INIT || "true").toLowerCase() === "true";

const HEADERS = {
  "Content-Type": "application/json",
  "X-Admin-Api-Key": API_KEY,
};

const N_NODES = Number(__ENV.N_NODES || 2);

// Register/health-check real nodes that are actually running locally.
// This avoids creating synthetic node IDs/ports that do not exist and would fail health checks.
const NODES = Array.from({ length: N_NODES }, (_, i) => {
  const idx = i + 1;
  const nodeId = `node-${idx}`;
  const port = Number(__ENV[`NODE_${idx}_PORT`] || (8000 + idx)); // default 8001, 8002, ...
  const host = __ENV[`NODE_${idx}_HOST`] || "localhost";
  const zone = __ENV[`NODE_${idx}_ZONE`] || "us-east-1a";
  return { nodeId, address: `${host}:${port}`, zone };
});

function url(path) {
  return `${BASE_URL}${path}`;
}

function postJson(path, body) {
  return http.post(url(path), JSON.stringify(body), { headers: HEADERS });
}

function get(path) {
  return http.get(url(path), { headers: { "X-Admin-Api-Key": API_KEY } });
}

export default function () {
  group("bootstrap (optional)", () => {
    if (!RUN_SHARD_INIT) return;

    const initBody = {
      num_shards: Number(__ENV.NUM_SHARDS || 8),
      replication_factor: Number(__ENV.REPLICATION_FACTOR || 2),
    };

    const r = postJson("/admin/config/shard-init", initBody);

    // Many systems make init idempotent and return 409 if already initialized.
    check(r, {
      "shard-init status is 2xx/409": (res) =>
        (res.status >= 200 && res.status < 300) || res.status === 409,
    });
  });

  group("register node", () => {
    // Select a real node deterministically per VU
    const node = NODES[(__VU - 1) % NODES.length];

    const registerBody = {
      node_id: node.nodeId,
      address: node.address,
      zone: node.zone,

      // Optional fields: include only if your API accepts them in snake_case
      // rack: `rack-${(__VU - 1) % 3}`,
      // status: "ALIVE",
      // last_heartbeat_ms: Date.now(),
      // capacity_hints: { disk_gb: "100", mem_gb: "16" },
    };

    const r = postJson("/admin/nodes", registerBody);
    check(r, {
      "register node status is 2xx": (res) => res.status >= 200 && res.status < 300,
      "register node latency < 750ms": (res) => res.timings.duration < 750,
    });

    // Health check the real node we just (re)registered
    const rh = get(`/admin/nodes/${encodeURIComponent(node.nodeId)}/health`);
    check(rh, {
      "node health status is 2xx": (res) => res.status >= 200 && res.status < 300,
    });
  });

  group("read-only monitoring", () => {
    const r1 = get("/admin/cluster/summary");
    check(r1, { "cluster summary 2xx": (res) => res.status >= 200 && res.status < 300 });

    const r2 = get("/admin/nodes");
    check(r2, { "list nodes 2xx": (res) => res.status >= 200 && res.status < 300 });

    const r3 = get("/admin/shards");
    check(r3, { "list shards 2xx": (res) => res.status >= 200 && res.status < 300 });
  });

  sleep(0.2);
}