#!/bin/bash
set -euo pipefail

# Bootstraps the coordinator shard map so the gateway can route requests.
#
# Requires: grpcurl (recommended install: `brew install grpcurl`)
#
# Usage:
#   COORDINATOR_ADDR=localhost:9000 N_NODES=2 ./scripts/bootstrap_cluster.sh
#

BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/.."

COORDINATOR_ADDR="${COORDINATOR_ADDR:-localhost:9002}"
N_NODES="${N_NODES:-2}"
FIRST_NODE_PORT="${FIRST_NODE_PORT:-8001}"
NODE_HOST="${NODE_HOST:-localhost}"
RF="${RF:-$N_NODES}"
NUM_SHARDS="${NUM_SHARDS:-8}"

if ! command -v grpcurl >/dev/null 2>&1; then
  echo "grpcurl is required but not installed."
  echo "Install via Homebrew: brew install grpcurl"
  exit 1
fi

PROTO_DIR="$BASE_DIR/kv.proto/src/main/proto"

echo "Bootstrapping coordinator at ${COORDINATOR_ADDR}"
echo "Registering ${N_NODES} node(s) starting at ${NODE_HOST}:${FIRST_NODE_PORT}"

for ((i=1; i<= N_NODES; i++)); do
  node_id="node-$i"
  node_port=$((FIRST_NODE_PORT + i - 1))
  node_addr="${NODE_HOST}:${node_port}"

  grpcurl -plaintext \
    -import-path "${PROTO_DIR}" \
    -proto coordinator.proto \
    -d "{\"node_id\":\"${node_id}\",\"address\":\"${node_addr}\",\"zone\":\"local\"}" \
    "${COORDINATOR_ADDR}" \
    kvdb.coordinator.Coordinator/RegisterNode >/dev/null

  echo "Registered ${node_id} @ ${node_addr}"
done

# Cap RF to N_NODES if user passed something larger.
if [ "${RF}" -gt "${N_NODES}" ]; then
  RF="${N_NODES}"
fi

echo "Initializing shards: num_shards=${NUM_SHARDS}, replication_factor=${RF}"
grpcurl -plaintext \
  -import-path "${PROTO_DIR}" \
  -proto coordinator.proto \
  -d "{\"num_shards\":${NUM_SHARDS},\"replication_factor\":${RF}}" \
  "${COORDINATOR_ADDR}" \
  kvdb.coordinator.Coordinator/InitShards

echo "Bootstrap complete."


