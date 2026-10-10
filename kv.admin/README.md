
## Authentication

The dev-only API key filter (`SecurityConfig#adminApiKeyFilter`) has no default
credential. Export the same secret you configured via
`KVDB_ADMIN_SECURITY_API_KEY` before running any of the requests below:

```bash
export ADMIN_API_KEY=<your-secret>
```

## Initialize Cluster (First Time Setup)

```bash
# 1. Initialize shards
curl -X POST http://localhost:8089/admin/config/shard-init \
  -H "Content-Type: application/json" \
  -H "X-Admin-Api-Key: ${ADMIN_API_KEY}" \
  -d '{"num_shards": 8, "replication_factor": 2}'

# 2. Register nodes
curl -X POST http://localhost:8089/admin/nodes \
  -H "Content-Type: application/json" \
  -H "X-Admin-Api-Key: ${ADMIN_API_KEY}" \
  -d '{"node_id": "node-1", "address": "127.0.0.1:8001", "zone": "us-east-1a"}'

# 3. Check cluster status
curl -X GET http://localhost:8089/admin/cluster/summary \
  -H "X-Admin-Api-Key: ${ADMIN_API_KEY}"
```

## Monitor Cluster

```bash
# Get cluster summary
curl -X GET http://localhost:8089/admin/cluster/summary \
  -H "X-Admin-Api-Key: ${ADMIN_API_KEY}" | jq .

# List all nodes
curl -X GET http://localhost:8089/admin/nodes \
  -H "X-Admin-Api-Key: ${ADMIN_API_KEY}" | jq .

# List all shards
curl -X GET http://localhost:8089/admin/shards \
  -H "X-Admin-Api-Key: ${ADMIN_API_KEY}" | jq .

# Resolve which shard currently owns a binary key.
# The response is coordinator placement at observation time: it is not proof that a
# value exists and does not reflect the gateway's shard-map cache.
KEY_B64="$(printf 'user:1' | base64)"
curl -X POST http://localhost:8089/admin/shards/resolve-key \
  -H "Content-Type: application/json" \
  -H "X-Admin-Api-Key: ${ADMIN_API_KEY}" \
  -d "{\"key_base64\": \"${KEY_B64}\"}" | jq .

```

## Diagnose key placement

`POST /admin/shards/resolve-key` forwards the decoded key bytes to the coordinator
`ResolveShard` RPC. The admin service does not hash the key and does not Get/Put its
value. Existing API-key (`X-Admin-Api-Key`) and IP allowlist policy apply.

Request body:

```json
{"key_base64": "<standard base64 of the raw key bytes>"}
```

Successful response fields (`shard_id`, `epoch`, `replicas`, `leader`, `config_state`)
are copied from the coordinator observation. The supplied key/value is never echoed
in the response or in logs (only the decoded byte length is logged).

| Condition | HTTP | `error` |
| --- | --- | --- |
| Malformed base64 or missing `key_base64` | 400 | `INVALID_ARGUMENT` |
| Empty decoded key | 400 | `InvalidRequestException` |
| Encoded or decoded key larger than `kvdb.admin.max-key-bytes` (default 4096) | 429 | `PayloadTooLargeException` |
| Missing/invalid API key | 401 | `invalid_api_key` |
| Client IP not in allowlist | 403 | `ip_not_allowed` |
| Coordinator unavailable | 503 | `GRPC_ERROR` |
| Coordinator deadline exceeded | 504 | `GRPC_ERROR` |

## Mutation request bodies

Typed JSON bodies use snake_case. Unknown or misspelled fields are rejected
(`400` / `UNKNOWN_FIELD`) instead of being ignored. `POST /admin/config` and
`POST /admin/config/shard-init` are the exception: their body is an open JSON
object, and every key is configuration payload.

`POST /admin/shards/{shardId}/replicas` expects a JSON array of node ids.

Leader and status updates require `Content-Type: application/json`. A raw
`text/plain` body is rejected with `415` / `UNSUPPORTED_MEDIA_TYPE`. The raw
request text is never stored as a leader id or node status.

### Set shard leader

```bash
curl -X POST http://localhost:8089/admin/shards/shard-0/leader \
  -H "Content-Type: application/json" \
  -H "X-Admin-Api-Key: ${ADMIN_API_KEY}" \
  -d '{"leader_node_id": "node-9"}'
```

`leader_node_id` is required and must be non-blank. The stored leader is that
field.

### Set node status

```bash
curl -X POST http://localhost:8089/admin/nodes/node-1/status \
  -H "Content-Type: application/json" \
  -H "X-Admin-Api-Key: ${ADMIN_API_KEY}" \
  -d '{"status": "ALIVE"}'
```

`status` is required and must be `ALIVE`, `SUSPECT`, or `DEAD`.

### Trigger an operation

```bash
curl -X POST http://localhost:8089/admin/ops/trigger \
  -H "Content-Type: application/json" \
  -H "X-Admin-Api-Key: ${ADMIN_API_KEY}" \
  -d '{"operation": "REBALANCE", "target_nodes": ["node-1"]}'
```

`operation` is required. `REBALANCE` is supported (case-insensitive);
`COMPACT` returns `501 Not Implemented` until a node compaction RPC is
available. Any other value returns `400` / `INVALID_ARGUMENT`.
`POST /admin/ops/rebalance` and `POST /admin/ops/compact` use the same JSON
shape but do not require `operation`. The compact endpoint also returns
`501 Not Implemented` until node compaction is implemented.

### Client-error codes

Response `message` values below are stable and do not include Java exception
text. `code` is the HTTP status as a decimal string.

| Condition | HTTP | `error` | `message` |
| --- | --- | --- | --- |
| Malformed JSON | 400 | `MALFORMED_JSON` | `Request body is not valid JSON` |
| JSON of the wrong shape, or a missing body | 400 | `INVALID_REQUEST` | `Request body does not match the expected schema` |
| Unknown or misspelled field | 400 | `UNKNOWN_FIELD` | `Unknown field: <name>` |
| Missing or invalid field | 400 | `VALIDATION_ERROR` | `<field>: <reason>` |
| Content-Type other than `application/json` on a JSON endpoint | 415 | `UNSUPPORTED_MEDIA_TYPE` | `Content-Type must be application/json` |

## Check Node Health

```bash
# Check specific node
curl -X GET http://localhost:8089/admin/nodes/node-1/health \
  -H "X-Admin-Api-Key: ${ADMIN_API_KEY}" | jq .
```
