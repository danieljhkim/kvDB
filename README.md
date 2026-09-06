# KvDB — Distributed Key-Value Database

![Java](https://img.shields.io/badge/Java-21+-007396?style=for-the-badge)
![Distributed Systems](https://img.shields.io/badge/Distributed%20Systems-Architecture-0B3C5D?style=for-the-badge)
![gRPC](https://img.shields.io/badge/gRPC-Transport-4285F4?style=for-the-badge)
![Control Plane](https://img.shields.io/badge/Control%20Plane-Separated-5C6BC0?style=for-the-badge)


KvDB is a Redis-like distributed key-value store implemented in Java, built around a clear separation between the **control plane** (cluster metadata) and the **data plane** (storage nodes). The system uses gRPC for service-to-service communication and is designed to evolve toward production-grade correctness (leader routing, topology epochs, retries, and consistent metadata propagation).

> Note: KvDB exposes a gRPC API. A non-interactive command line client for the
> gateway data plane ships in [`golang/kvcli`](golang/kvcli/README.md); it
> supports `get`, ordered `batch-get`, `put`/`set`, `del`/`delete`, and `ping`.
> There is no
> interactive shell and no line protocol.

---

## Architecture

KvDB is composed of four primary components:

- **Gateway (gRPC)**: Front door for clients. Performs shard routing, retries, and maintains a local shard-map cache.
- **Admin API (HTTP)**: Control-plane management surface. Bootstraps cluster state (e.g., shard initialization) and manages node membership.
- **Coordinator (gRPC / control plane)**: Owns the shard map, node records, shard epochs/versions, and streaming shard-map updates.
- **Storage Nodes (gRPC / data plane)**: Host shard replicas, serve reads, and accept writes only when they are the shard leader (or can provide a leader hint).

```
         +-----------------------------+
         |        Client (gRPC)        |
         +-------------+---------------+
                       |
         +-------------v---------------+
         |          Gateway            |
         | - Shard map cache           |
         | - Routing + retries         |
         | - Parses routing hints      |
         +------+------+---------------+
                |      \
                |       \  (data plane)
                v        v
         +------+-----+  +------+-----+  +------+-----+
         |  Node A    |  |  Node B    |  |  Node C    |
         | KV shard(s)|  | KV shard(s)|  | KV shard(s)|
         +------------+  +------------+  +------------+

                 (control plane / metadata)
         +--------------------------------------+
         |     Coordinator (Raft group)         |
         | - Shard map + epochs/versions        |
         | - Membership + status                |
         | - WatchShardMap (deltas)             |
         +--------------------------------------+
                 ^                ^
                 | watch/deltas   | admin mutations
                 | bootstrap/refresh
         +-------+----------------+-------+
         |              Admin API         |
         |   - node registration          |
         |   - shard initialization       |
         +-------------------------------+
                        ^
                        | HTTP (local ops)
                        |
                    Operator
```

---

## Key Concepts

### Shards, Replicas, and Leaders
- Keys map to a **shard** (routing is based on the shard map).
- Each shard has a **replica set** (one or more storage nodes).
- Writes are routed to the **per-shard leader**.
- Reads may be served by a leader or a replica, depending on routing policy.

### Shard Map Cache (Gateway)
The Gateway keeps a local shard map cache and keeps it fresh using a streaming watch:
- **WatchShardMap** provides **delta-based updates** to avoid full refreshes.
- On stream failures, the Gateway falls back to periodic polling until streaming resumes.

### Routing Hints (Fast Recovery)
Storage nodes return routing hints via **gRPC trailers**, allowing the Gateway to react quickly without global refreshes:
- `x-leader-hint`: preferred leader address for a shard
- `x-shard-id`: shard identifier related to the error
- `x-new-node-hint`: node address hint when shard ownership has moved

The Gateway uses these hints to:
- Retry once directly to the hinted leader for `NOT_LEADER`
- Force a shard-map refresh for `SHARD_MOVED`
- Otherwise trigger throttled refresh/backoff to avoid thrash

### Node-side Validation
Storage nodes consult the coordinator shard map to validate:
- Whether they are a **replica** of the shard
- Whether they are the **leader** for write operations
- Whether the provided **epoch** matches the shard’s current epoch (to prevent stale routing)

---

## APIs

### Client → Gateway (gRPC)
Core operations:
- `Get`
- `BatchGet`
- `Put`
- `Delete`

The Gateway is responsible for:
- Resolving the shard for a key
- Routing reads/writes to appropriate nodes
- Retrying with backoff where safe
- Interpreting routing hints from trailers

#### BatchGet semantics and limits

`BatchGet` accepts binary `keys`, shared `ReadOptions`, and a shared optional
`head_only` flag. For every accepted request, `results` contains exactly one
entry per input key in input order. Duplicate keys remain duplicate results.
Each result echoes its binary key and carries the same status, value/version
metadata, and serving-node `applied_version` as unary `Get`.

**There is no cross-key snapshot or atomic-read guarantee.** Each key is routed
and read independently. `STRONG`, `EVENTUAL`, and `head_only` therefore have
exactly the unary `Get` semantics for that item; results from different keys
may reflect different instants or shard versions.

Example request (protobuf text notation):

```protobuf
keys: "\000customer-1"
keys: "\377customer-2"
keys: "\000customer-1"  // intentionally repeated
options { consistency: STRONG }
head_only: false
ctx { request_id: "read-set-42" }
```

The default gateway bounds are configured under `limits`:

- `maxBatchEntries: 128`
- `maxBatchAggregateKeyBytes: 65536`
- `maxBatchGetConcurrency: 16`
- `maxBatchGetResponseBytes: 2097152`

Key-count, aggregate-key-size, individual-key, option, and inbound-message
violations fail request-wide validation before any storage read is dispatched.
After dispatch, found, not-found, and unavailable results can coexist. Deadline
and cancellation outcomes explicitly mark every remaining key. If a successful
item would exceed the response budget, it and all remaining items are returned
as `RESPONSE_BUDGET_EXHAUSTED`; the serialized response remains within the
configured budget. The budget must be large enough to encode one termination
outcome per admitted key, or the request is rejected before dispatch.

### Gateway/Nodes → Coordinator (gRPC)
Metadata and control plane operations:
- Shard map snapshot reads
- Shard map watch (delta streaming)
- Node/shard admin mutations (e.g., register node, init shards, set node status, set shard replicas/leader)

### Client → Gateway (command line)

`golang/kvcli` is a non-interactive client for the gateway data plane. It uses
the generated bindings for `kv.proto/src/main/proto/kvgateway.proto`; run
`make proto-go` to regenerate them with the pinned plugin versions.

```bash
make go-build                       # builds golang/kvcli/kv
kv put greeting hello               # writes, prints status and version
kv get greeting --raw > value.bin   # stdout receives exactly the stored bytes
printf '["Z3JlZXRpbmc="]' | kv batch-get --input -
kv del greeting
kv ping                             # bounded head-only reachability probe
```

Supported commands are `get`, ordered `batch-get`, `put` (alias `set`), `del`
(alias `delete`), and `ping`. Keys and values are byte strings: `--key-file`, `--value-file`, and
`--output-file` (with `-` for standard input) move arbitrary binary data,
including zero bytes and data that is not valid UTF-8. The removed interactive
line protocol is rejected explicitly: `kv connect` and `kv --interactive`
fail with an explanation rather than opening a session.

The client uses the same transport policy as the cluster
(`KVDB_GRPC_SECURITY_MODE`): mutual TLS by default, and `development-plaintext`
only when `KVDB_ENV` is `dev`, `development`, `local`, or `test`. Client
credentials come from `KVDB_CLIENT_TLS_TRUST_BUNDLE`,
`KVDB_CLIENT_TLS_CERT_CHAIN`, and `KVDB_CLIENT_TLS_PRIVATE_KEY` (or the
matching flags). Local plaintext also requires `KVDB_CLIENT_TENANT_ID` and
`KVDB_CLIENT_PRINCIPAL` (or `--tenant` and `--principal`); it sends the
development-only `client/<tenant>/<principal>` identity required by the local
gateway. Every RPC is deadline-bounded, non-OK application and
transport statuses exit nonzero with stable status names, and an ambiguous
write outcome is reported rather than retried. See
[golang/kvcli/README.md](golang/kvcli/README.md).

### Admin API (HTTP)
The Admin API provides a control-plane management surface intended for local operations and cluster bootstrapping:
- `POST /admin/nodes` — register or update a node (membership)
- `POST /admin/config/shard-init` — initialize the shard map (bootstrap)

Note: the Admin API forwards mutations to the Coordinator (Raft-backed state machine) to keep cluster metadata consistent.


---

## Running Locally

Consult the `Makefile` for common developer commands.

Internal gRPC uses mutually authenticated workload certificates in normal
deployments. `make run-cluster` explicitly selects the fail-closed
`development-plaintext` mode and supplies development-only identities; that mode
is refused unless `KVDB_ENV` is `local`, `dev`, `development`, or `test`. Local
processes and Docker Compose use the same coordinator seed endpoints (`localhost:9001` through
`localhost:9003`). Compose publishes them on loopback only; storage-node ports
remain private. See [SECURITY.md](SECURITY.md).

Typical flow:
1. Build:
   ```bash
   make build
   ```
2. Run a local cluster (coordinator + 2 data nodes + gateway + admin API):
   ```bash
   make run-cluster
   ```
3. Boostrap the cluster:
   ```bash
   make bootstrap-cluster
   ```
4. Smoke test the cluster:
   ```bash
   make smoke-test
   ```

For the durable three-coordinator Docker deployment:

```bash
export KVDB_ADMIN_SECURITY_API_KEY="$(openssl rand -hex 32)"
export KVDB_TLS_DIR=/absolute/path/to/kvdb-tls
docker compose up --build --detach
STORAGE_NODE_ADDRS=node1:8001,node2:8002 make bootstrap-cluster
docker compose up --detach --wait --wait-timeout 180
STORAGE_NODE_ADDRS=node1:8001,node2:8002 make smoke-test
docker compose down
```

Coordinator and storage-node state files live in named volumes. Use rolling
restarts to keep quorum available without re-bootstrap, and use
`docker compose down --volumes` only for an intentional wipe. The automated
failover, rolling-restart, persistence, and wipe check is:

```bash
./scripts/docker_failover_test.sh
```

### Raft persistence and snapshots

Coordinator Raft logs, term/vote state, and snapshots use versioned, bounded, CRC32C-protected records. Existing
length-prefixed logs and properties state files remain readable during rolling upgrades and are rewritten in the new
format on their next mutation. Any truncated, oversized, malformed, or checksum-invalid safety-critical file stops
the coordinator; an existing unreadable file is never interpreted as a new node.

`raft.snapshotThreshold` controls how many newly applied entries a leader retains before snapshotting (`10000` by
default, `0` disables automatic snapshots). The snapshot file is forced before the live log is compacted. Followers
receive snapshots in bounded chunks and persist a restartable installation file before atomically replacing the live
snapshot.

The stable-storage boundary is `write temporary -> fsync(file) -> atomic rename -> fsync(parent directory)`.
Append-only log records are forced before an RPC or command can acknowledge them. kvDB treats failure or lack of
support for file forcing, atomic rename, or directory forcing as a failed Raft write. The supported deployment
filesystems are local Linux and macOS filesystems that implement those operations; network filesystems require
independent crash-consistency qualification.
---

## Benchmarking

Detailed benchmark results and analysis are documented in [docs/performance.md](docs/performance.md).

### BatchGet fixed-fixture baseline

`KvGatewayContractTest.fixedMultiShardBaselineShowsOneClientRpcWithEqualBackendReadsAndBoundedFanout`
is a reproducible comparison using eight one-byte keys spread across four
shards, a deterministic 15 ms storage-call fixture, and BatchGet concurrency of
four. One run on 2026-09-04 produced:

| Path | Client RPCs | Elapsed | Backend reads | Max active reads |
|---|---:|---:|---:|---:|
| 8 sequential unary `Get`s | 8 | 152 ms | 8 | 1 |
| 1 `BatchGet` | 1 | 45 ms | 8 | 4 |

This controlled baseline demonstrates the saved client round trips and bounded
fanout. It does **not** show fewer backend reads: both paths issued eight. The
elapsed values are test-fixture observations, not production latency claims;
rerun the named test on the target hardware for a local baseline.

Gateway (gRPC)
```bash
make k6-gateway-bench
make ghz-gateway-bench
```

Admin API (HTTP)
```bash
make k6-admin-bench
make vegeta-admin-bench
```

## License

This project is licensed under the MIT License.
