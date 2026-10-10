# KvDB — Distributed Key-Value Database

![Java](https://img.shields.io/badge/Java-21+-007396?style=for-the-badge)
![gRPC](https://img.shields.io/badge/gRPC-Transport-4285F4?style=for-the-badge)
![Control Plane](https://img.shields.io/badge/Control%20Plane-Separated-5C6BC0?style=for-the-badge)

KvDB is a Redis-like distributed key-value store in Java. It separates the **control plane** (cluster metadata, held
by a Raft group of coordinators) from the **data plane** (storage nodes), and all services talk over gRPC.

Clients use the gRPC API or the non-interactive CLI in [`golang/kvcli`](golang/kvcli/README.md). There is no
interactive shell and no line protocol.

---

## Architecture

| Component | Protocol | Role |
|---|---|---|
| **Gateway** | gRPC | Client front door: shard routing, retries, local shard-map cache. |
| **Coordinator** | gRPC, Raft group | Owns the shard map, node records, shard epochs, and shard-map watch streams. |
| **Storage node** | gRPC | Hosts shard replicas, serves reads, and accepts writes only as shard leader. |
| **Admin API** | HTTP | Operator surface: node registration and shard bootstrap, forwarded to the coordinator. |

```
 Client ──gRPC──▶ Gateway ──data plane──▶ Node A │ Node B │ Node C
                     │                       │
                     ▼ watch / refresh       ▼ shard-map validation
              Coordinator (Raft group) ◀── admin mutations ── Admin API ◀──HTTP── Operator
```

### Routing

- Each key maps to a **shard**. A shard has a **replica set** and one **leader**. Writes go to the leader. Reads may
  go to the leader or a replica, depending on routing policy.
- The gateway keeps its shard-map cache fresh with **WatchShardMap** delta streams and falls back to polling while
  the stream is down.
- Storage nodes check every request against the coordinator's shard map: are they a replica, are they the leader (for
  writes), and does the request's **epoch** match the current one.
- On errors, nodes return routing hints in gRPC trailers (`x-leader-hint`, `x-shard-id`, `x-new-node-hint`). The
  gateway retries once at the hinted leader on `NOT_LEADER`, forces a refresh on `SHARD_MOVED`, and otherwise
  backs off and refreshes, with throttling.

### Coordinator invariants

The coordinator rejects invalid shard-map mutations with `INVALID_ARGUMENT` (HTTP 400 through the Admin API) and
leaves the shard map unchanged when it does:

- node addresses are `host:port` (hostname or IPv4, port 1–65535);
- replica sets are non-empty, have no duplicates, and contain only registered node IDs;
- a shard leader is a registered member of the shard's current replica set, at its current epoch.

The leader validates each command before it enters the Raft log, and every coordinator validates again at apply time.
A command that became invalid in between is applied as a no-op.

New nodes register as `ALIVE`, because a node registers only once it is serving. The health checker moves an
unreachable node to `SUSPECT` and then to `DEAD` after consecutive failed probes. The gateway routes only to `ALIVE`
nodes.

---

## APIs

- **Client → Gateway (gRPC):** `Get`, `BatchGet`, `Put`, `Delete`.
  - `BatchGet` returns exactly one result per input key, in input order.
  - It gives **no cross-key snapshot**: each key is read independently.
  - Its limits and full semantics are in [docs/api-compatibility.md](docs/api-compatibility.md#batchget-semantics-and-limits).
- **Gateway/Nodes → Coordinator (gRPC):** shard-map snapshots, delta watches, and membership and shard mutations.
- **Admin API (HTTP):**
  - `POST /admin/nodes` registers or updates a node.
  - `POST /admin/config/shard-init` bootstraps the shard map.
- **CLI ([`golang/kvcli`](golang/kvcli/README.md)):**
  - Commands are `get`, `batch-get`, `put`/`set`, `del`/`delete` and `ping`.
  - It is binary-safe (`--key-file`, `--value-file`, `--output-file`), deadline-bounded, and exits with stable
    status codes.
  - It reports an ambiguous write outcome instead of retrying it.

```bash
make go-build                       # builds golang/kvcli/kv
kv put greeting hello
kv get greeting --raw > value.bin   # stdout receives exactly the stored bytes
printf '["Z3JlZXRpbmc="]' | kv batch-get --input -
kv del greeting
kv ping
```

The CLI uses the cluster's transport policy:

- mutual TLS by default;
- `development-plaintext` only when `KVDB_ENV` is `local`, `dev`, `development` or `test`.

Its README covers credentials, flags and exit codes. To regenerate the Go bindings, run `make proto-go`.

---

## Running Locally

Normal deployments use mutually authenticated workload certificates for internal gRPC. `make run-cluster` selects
`development-plaintext` with development-only identities, so it requires `KVDB_ENV` to be `local`, `dev`,
`development` or `test`. See [SECURITY.md](SECURITY.md); the `Makefile` lists the other developer commands.

```bash
make build
export KVDB_ADMIN_SECURITY_API_KEY="$(openssl rand -hex 32)"   # Admin API fails closed without it
make run-cluster        # 3 coordinators, 2 storage nodes, gateway, Admin API
make bootstrap-cluster
make smoke-test
make stop               # stops only the processes this checkout started
```

How `run-cluster` behaves:

- It exits before starting anything if the API key is missing. Set `START_ADMIN=false` to skip the Admin API instead.
- It waits up to `STARTUP_TIMEOUT_SECONDS` (default 60) for each component to open its port.
- If a component dies or never becomes ready, it exits nonzero, names the component, and tails its log.
- Logs go to `logs/` and state to `data/` under the repository.

### Docker (durable three-coordinator cluster)

```bash
export KVDB_ADMIN_SECURITY_API_KEY="$(openssl rand -hex 32)"
export KVDB_TLS_DIR=/absolute/path/to/kvdb-tls
docker compose up --build --detach
STORAGE_NODE_ADDRS=node1:8001,node2:8002 make bootstrap-cluster
docker compose up --detach --wait --wait-timeout 180
STORAGE_NODE_ADDRS=node1:8001,node2:8002 make smoke-test
docker compose down
```

- Compose publishes the coordinator seeds on loopback only (`localhost:9001`–`9003`, the same as local processes).
  Storage-node ports stay private.
- State lives in named volumes. Use rolling restarts to keep quorum without re-bootstrapping.
- `docker compose down --volumes` wipes all state, so run it only when you mean to.
- `./scripts/docker_failover_test.sh` checks failover, rolling restarts, persistence and wipe.

Coordinator durability (record format, snapshots, fsync boundary, supported filesystems) is documented in
[docs/raft-persistence.md](docs/raft-persistence.md).

---

## Benchmarking

Results, and the BatchGet fixed-fixture baseline, are in [docs/performance.md](docs/performance.md).

```bash
make k6-gateway-bench     # Gateway (gRPC)
make ghz-gateway-bench
make k6-admin-bench       # Admin API (HTTP)
make vegeta-admin-bench
```

## License

MIT.
