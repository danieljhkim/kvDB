
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

## Check Node Health

```bash
# Check specific node
curl -X GET http://localhost:8089/admin/nodes/node-1/health \
  -H "X-Admin-Api-Key: ${ADMIN_API_KEY}" | jq .
```