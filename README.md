# KvDB — Distributed Key-Value Database

[![Build and Test](https://github.com/danieljhkim/Distributed-Key-Value-Database/actions/workflows/build.yml/badge.svg)](https://github.com/danieljhkim/Distributed-Key-Value-Database/actions/workflows/build.yml)

KvDB is a Redis-like distributed key-value store implemented in Java, built around a clear separation between the **control plane** (cluster metadata) and the **data plane** (storage nodes). The system uses gRPC for service-to-service communication and is designed to evolve toward production-grade correctness (leader routing, topology epochs, retries, and consistent metadata propagation).

> Note: KvDB currently exposes a gRPC API. There is no CLI included at this stage.

---

## Architecture

KvDB is composed of three primary components:

- **Gateway (gRPC)**: Front door for clients. Performs shard routing, retries, and maintains a local shard-map cache.
- **Coordinator (metadata / control plane)**: Owns the shard map, node records, shard epochs/versions, and streaming shard-map updates.
- **Storage Nodes (data plane)**: Own shard replicas, serve reads, and accept writes only when they are the shard leader (or can provide a leader hint).

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
                 ^                |
                 | watch/deltas   | bootstrap/refresh
                 +----------------+
                        Gateway
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
- `Put`
- `Delete`

The Gateway is responsible for:
- Resolving the shard for a key
- Routing reads/writes to appropriate nodes
- Retrying with backoff where safe
- Interpreting routing hints from trailers

### Gateway/Nodes → Coordinator (gRPC)
Metadata and control plane operations:
- Shard map snapshot reads
- Shard map watch (delta streaming)
- Node/shard admin mutations (e.g., register node, init shards, set node status, set shard replicas/leader)


---

## Running Locally

Consult the `Makefile` for common developer commands.

Typical flow:
1. Build:
   ```bash
   make build
   ```
2. Run a local cluster (coordinator + a few nodes + gateway):
   ```bash
   make run-cluster
   ```


---


## License

This project is licensed under the MIT License.
