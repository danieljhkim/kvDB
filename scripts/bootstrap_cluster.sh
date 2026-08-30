#!/usr/bin/env bash
set -euo pipefail

# Idempotently registers storage nodes and initializes shards through the
# elected coordinator. Both local and Docker deployments expose the same
# loopback seed endpoints: localhost:9001, localhost:9002, localhost:9003.

BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/.."
PROTO_DIR="$BASE_DIR/kv.proto/src/main/proto"
source "$BASE_DIR/scripts/cluster_helpers.sh"

N_NODES="${N_NODES:-2}"
FIRST_NODE_PORT="${FIRST_NODE_PORT:-8001}"
NODE_HOST="${NODE_HOST:-localhost}"
STORAGE_NODE_ADDRS="${STORAGE_NODE_ADDRS:-}"
RF="${RF:-$N_NODES}"
NUM_SHARDS="${NUM_SHARDS:-8}"
require_grpcurl

leader="$(discover_coordinator_leader)"
echo "Bootstrapping through elected coordinator at ${leader}"
if [[ -n "$STORAGE_NODE_ADDRS" ]]; then
  IFS=',' read -r -a node_addresses <<< "$STORAGE_NODE_ADDRS"
  if (( ${#node_addresses[@]} != N_NODES )); then
    echo "STORAGE_NODE_ADDRS must contain exactly ${N_NODES} addresses" >&2
    exit 1
  fi
  echo "Registering ${N_NODES} node(s) at ${STORAGE_NODE_ADDRS}"
else
  echo "Registering ${N_NODES} node(s) starting at ${NODE_HOST}:${FIRST_NODE_PORT}"
fi

for ((i=1; i<=N_NODES; i++)); do
  node_id="node-$i"
  if [[ -n "$STORAGE_NODE_ADDRS" ]]; then
    node_addr="${node_addresses[$((i - 1))]//[[:space:]]/}"
  else
    node_port=$((FIRST_NODE_PORT + i - 1))
    node_addr="${NODE_HOST}:${node_port}"
  fi
  response="$(coordinator_call \
    kvdb.coordinator.Coordinator/RegisterNode \
    "{\"node_id\":\"${node_id}\",\"address\":\"${node_addr}\",\"zone\":\"local\"}")"
  if ! grep -Eq '"success"[[:space:]]*:[[:space:]]*true' <<< "$response"; then
    echo "Failed to register ${node_id}: ${response}" >&2
    exit 1
  fi
  echo "Registered ${node_id} @ ${node_addr}"
done

if (( RF > N_NODES )); then
  RF="$N_NODES"
fi

echo "Initializing shards: num_shards=${NUM_SHARDS}, replication_factor=${RF}"
response="$(coordinator_call \
  kvdb.coordinator.Coordinator/InitShards \
  "{\"num_shards\":${NUM_SHARDS},\"replication_factor\":${RF}}")"
if ! grep -Eq '"success"[[:space:]]*:[[:space:]]*true' <<< "$response"; then
  echo "Failed to initialize shards: ${response}" >&2
  exit 1
fi

echo "Bootstrap complete."
