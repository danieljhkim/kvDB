#!/bin/bash

set -e

############################################
# CONFIG
############################################

BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/.."

COORDINATOR_JAR="$BASE_DIR/kv.coordinator/target/kv.coordinator-1.0-SNAPSHOT.jar"
NODE_JAR="$BASE_DIR/kv.server/target/kv.server-1.0-SNAPSHOT.jar"

LOG_DIR="$BASE_DIR/logs"
DATA_DIR="$BASE_DIR/data"

mkdir -p "$LOG_DIR"
mkdir -p "$DATA_DIR"

# Number of nodes
N_NODES=${N_NODES:-2}

# Base ports
COORDINATOR_PORT=${COORDINATOR_PORT:-7000}


############################################
# FUNCTIONS
############################################

start_coordinator() {
  echo "Starting Coordinator..."
  export COORDINATOR_PORT

  nohup java -jar "$COORDINATOR_JAR" \
    > "$LOG_DIR/coordinator.log" 2>&1 &
  COORDINATOR_PID=$!

  echo "Coordinator started (PID: $COORDINATOR_PID, port: $COORDINATOR_PORT)"
}

start_nodes() {
  echo "Starting $N_NODES Data Node(s)..."

  for ((i=1; i<= N_NODES; i++)); do
    local node_id="node-$i"
    export NODE_ID="$node_id"

    nohup java -jar "$NODE_JAR" \
      "$node_id" \
      > "$LOG_DIR/node-$i.log" 2>&1 &
    NODE_PID=$!
    echo "Data-Node #$i started (NODE_ID=$node_id, PID: $NODE_PID)"
  done
}

stop_cluster() {
  echo "Stopping all cluster processes..."
  pkill -f "kv.coordinator-1.0-SNAPSHOT.jar" || true
  pkill -f "kv.server-1.0-SNAPSHOT.jar" || true
  echo "Cluster stopped."
}

is_running() {
  local port=7000

  # Check if any process is listening on port 7000 (IPv4 or IPv6)
  if command -v lsof >/dev/null 2>&1; then
    # lsof available
    if lsof -iTCP:"$port" -sTCP:LISTEN -P -n >/dev/null 2>&1; then
      return 0    # running
    else
      return 1    # not running
    fi
  elif command -v ss >/dev/null 2>&1; then
    # fallback to ss
    if ss -ltn "( sport = :$port )" | grep -q LISTEN; then
      return 0
    else
      return 1
    fi
  elif command -v netstat >/dev/null 2>&1; then
    # fallback to netstat (older systems)
    if netstat -an 2>/dev/null | grep -q "[.:]$port .*LISTEN"; then
      return 0
    else
      return 1
    fi
  else
    echo "⚠️ No suitable tool (lsof/ss/netstat) found to check port status." >&2
    return 1
  fi
}

status_server() {
  if is_running; then
    echo "🟢 ClusterServer running"
    echo "   Log: $LOG_DIR"
  else
    echo "🔴 ClusterServer not running"
  fi
}


############################################
# ENTRYPOINT
############################################

if [[ "$1" == "stop" ]]; then
  stop_cluster
  exit 0
fi

if [[ "$1" == "status" ]]; then
  status_server
  exit 0
fi

echo "================================================="
echo " Spinning up Distributed kvdb Cluster"
echo "================================================="
echo "Coordinator : localhost:${COORDINATOR_PORT}"
echo "Data Nodes : ${N_NODES}"
echo "================================================="

start_coordinator
sleep 1

start_nodes
sleep 1

echo ""
echo "================================================="
echo "Cluster is running!"
echo "Logs  : $LOG_DIR"
echo "Data  : $DATA_DIR"
echo "Stop  : ./run_cluster.sh stop"
echo "================================================="