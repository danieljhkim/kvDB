

# Performance Benchmarks

This document summarizes local performance benchmarks for kvDB using `ghz` against a 2-node cluster.

---

## Test Environment

- **Machine**: macOS (Apple Silicon)
- **Cluster Topology**:
  - 1 Coordinator
  - 2 Data Nodes
  - 1 Gateway
- **Replication Factor**: 2
- **Shards**: 8
- **Consistency**: STRONG
- **Tooling**: ghz (gRPC benchmarking)

---

## Gateway Benchmarks (gRPC)

**Target**: `localhost:7000`  
**Concurrency**: 50  
**Total Requests per Test**: 20,000  

### PUT — `kvdb.gateway.KvGateway/Put`

| Metric | Value |
|------|------|
| Throughput | **12,378 req/s** |
| Average Latency | **3.91 ms** |
| p50 | 3.59 ms |
| p90 | 6.01 ms |
| p95 | 7.44 ms |
| p99 | 11.44 ms |
| Fastest | 0.56 ms |
| Slowest | 34.28 ms |
| Errors | 0 |

**Observations**
- Write path sustains >12k ops/sec with strong consistency.
- Tail latency (p99) remains under 12 ms under load.
- No write failures observed.

---

### GET — `kvdb.gateway.KvGateway/Get`

| Metric | Value |
|------|------|
| Throughput | **31,275 req/s** |
| Average Latency | **1.41 ms** |
| p50 | 1.36 ms |
| p90 | 1.82 ms |
| p95 | 1.99 ms |
| p99 | 2.37 ms |
| Fastest | 0.34 ms |
| Slowest | 4.68 ms |
| Errors | 0 |

**Observations**
- Read throughput exceeds 30k ops/sec.
- p99 latency remains below 2.5 ms.
- Gateway read path is highly efficient and stable.

---

## Notes

- Results are **environment-specific** and intended for directional evaluation.
- See `benchmark/scripts/` for reproducible benchmark commands.

_Last updated: 2025-12-28_

---

## BatchGet fixed-fixture baseline

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
