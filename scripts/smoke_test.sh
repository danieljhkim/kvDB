#!/bin/bash
set -euo pipefail

# Basic integration smoke test:
# - bootstrap coordinator (register nodes + init shards)
# - Put(key,value) via gateway
# - Get(key) via gateway and assert value round-trips
#
# Requires: grpcurl
#
# Usage:
#   ./scripts/run_cluster.sh
#   ./scripts/smoke_test.sh
#
# Optional env:
#   COORDINATOR_ADDR=localhost:9000
#   GATEWAY_ADDR=localhost:7000
#   N_NODES=2
#   NUM_SHARDS=8
#   RF=2
#

BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/.."

COORDINATOR_ADDR="${COORDINATOR_ADDR:-localhost:9000}"
GATEWAY_ADDR="${GATEWAY_ADDR:-localhost:7000}"
N_NODES="${N_NODES:-2}"
NUM_SHARDS="${NUM_SHARDS:-8}"
RF="${RF:-$N_NODES}"

if ! command -v grpcurl >/dev/null 2>&1; then
  echo "grpcurl is required but not installed."
  echo "Install via Homebrew: brew install grpcurl"
  exit 1
fi

PROTO_DIR="$BASE_DIR/kv.proto/src/main/proto"

echo "== Put/Get smoke test via gateway =="

key_plain="smoke-key10"
val_plain="hello10"

key_b64="$(printf "%s" "${key_plain}" | base64 | tr -d '\n')"
val_b64="$(printf "%s" "${val_plain}" | base64 | tr -d '\n')"

put_resp="$(
  grpcurl -plaintext \
    -import-path "${PROTO_DIR}" \
    -proto kvgateway.proto \
    -d "{\"ctx\":{\"request_id\":\"smoke-put-1\"},\"key\":\"${key_b64}\",\"value\":\"${val_b64}\",\"options\":{}}" \
    "${GATEWAY_ADDR}" \
    kvdb.gateway.KvGateway/Put
)"

echo "PutResponse: ${put_resp}"

get_resp="$(
  grpcurl -plaintext \
    -import-path "${PROTO_DIR}" \
    -proto kvgateway.proto \
    -d "{\"ctx\":{\"request_id\":\"smoke-get-1\"},\"key\":\"${key_b64}\",\"options\":{\"consistency\":\"STRONG\"}}" \
    "${GATEWAY_ADDR}" \
    kvdb.gateway.KvGateway/Get
)"

echo "GetResponse: ${get_resp}"

# Minimal assertion without jq: ensure base64 value appears in response JSON.
if echo "${get_resp}" | grep -q "\"value\": *\"${val_b64}\""; then
  echo "✅ Smoke test passed (value round-tripped)"
  exit 0
fi

echo "❌ Smoke test failed: expected value base64 ${val_b64} not found in GetResponse"
exit 1


