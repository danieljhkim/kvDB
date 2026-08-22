
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

```

## Check Node Health

```bash
# Check specific node
curl -X GET http://localhost:8089/admin/nodes/node-1/health \
  -H "X-Admin-Api-Key: ${ADMIN_API_KEY}" | jq .
```