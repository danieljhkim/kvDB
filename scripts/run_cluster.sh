#!/bin/bash

set -e

############################################
# CONFIG
############################################

BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/.."

COORDINATOR_JAR="$BASE_DIR/kv.coordinator/target/kv-coordinator.jar"
NODE_JAR="$BASE_DIR/kv.node/target/kv-node.jar"
GATEWAY_JAR="$BASE_DIR/kv.gateway/target/kv-gateway.jar"

LOG_DIR="$BASE_DIR/logs"
DATA_DIR="$BASE_DIR/data"

mkdir -p "$LOG_DIR"
mkdir -p "$DATA_DIR"

# Fail fast if build artifacts are missing (otherwise java will exit immediately and you won't see server logs).
require_file() {
  local f="$1"
  if [ ! -f "$f" ]; then
    echo "❌ Missing file: $f" >&2
    echo "   Did you run: make build ?" >&2
    exit 1
  fi
}

# Validate jar paths up-front
require_file "$COORDINATOR_JAR"
require_file "$NODE_JAR"
if [ "$START_GATEWAY" = "true" ]; then
  require_file "$GATEWAY_JAR"
fi

# Number of nodes
N_NODES=${N_NODES:-2}

# Base ports
# Coordinator (gRPC) defaults to 9000 in code
COORDINATOR_PORT=${COORDINATOR_PORT:-9000}
# Gateway (gRPC) defaults to 7000 in code
GATEWAY_PORT=${GATEWAY_PORT:-7000}

# Start gateway (optional)
START_GATEWAY=${START_GATEWAY:-true}


############################################
# FUNCTIONS
############################################

start_coordinator() {
  echo "Starting Coordinator..."
  export COORDINATOR_PORT

  # Ensure log file exists
  touch "$LOG_DIR/coordinator.log"

  nohup java -jar "$COORDINATOR_JAR" "$COORDINATOR_PORT" \
    > "$LOG_DIR/coordinator.log" 2>&1 &
  COORDINATOR_PID=$!

  echo "Coordinator started (PID: $COORDINATOR_PID, port: $COORDINATOR_PORT)"
  echo "  Log file: $LOG_DIR/coordinator.log"
  
  # Give it a moment to start and write to log
  sleep 0.5
  if ! ps -p $COORDINATOR_PID > /dev/null 2>&1; then
    echo "⚠️  Warning: Coordinator process may have crashed. Check $LOG_DIR/coordinator.log"
  fi
}

start_nodes() {
  echo "Starting $N_NODES Data Node(s)..."

  for ((i=1; i<= N_NODES; i++)); do
    local node_id="node-$i"
    export NODE_ID="$node_id"

    # Ensure log file exists
    touch "$LOG_DIR/node-$i.log"

    nohup java -jar "$NODE_JAR" \
      "$node_id" \
      > "$LOG_DIR/node-$i.log" 2>&1 &
    NODE_PID=$!
    echo "Data-Node #$i started (NODE_ID=$node_id, PID: $NODE_PID)"
    echo "  Log file: $LOG_DIR/node-$i.log"
    
    # Give it a moment to start and write to log
    sleep 0.3
    if ! ps -p $NODE_PID > /dev/null 2>&1; then
      echo "⚠️  Warning: Node #$i process may have crashed. Check $LOG_DIR/node-$i.log"
    fi
  done
}

start_gateway() {
  if [ "$START_GATEWAY" = "true" ]; then
    echo "Starting Gateway..."
    export GATEWAY_PORT

    # Ensure log file exists
    touch "$LOG_DIR/gateway.log"

    nohup java -jar "$GATEWAY_JAR" "$GATEWAY_PORT" \
      > "$LOG_DIR/gateway.log" 2>&1 &
    GATEWAY_PID=$!

    echo "Gateway started (PID: $GATEWAY_PID, port: $GATEWAY_PORT)"
    echo "  Log file: $LOG_DIR/gateway.log"
    
    # Give it a moment to start and write to log
    sleep 0.5
    if ! ps -p $GATEWAY_PID > /dev/null 2>&1; then
      echo "⚠️  Warning: Gateway process may have crashed. Check $LOG_DIR/gateway.log"
    fi
  fi
}

stop_cluster() {
  echo "Stopping all cluster processes..."
  pkill -f "kv-coordinator.jar" || true
  pkill -f "kv-node.jar" || true
  pkill -f "kv-gateway.jar" || true
  echo "Cluster stopped."
}

is_running() {
  local port="${COORDINATOR_PORT}"

  # Check if any process is listening on the coordinator port (IPv4 or IPv6)
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

start_gateway
if [ "$START_GATEWAY" = "true" ]; then
  sleep 1
fi

echo ""
echo "================================================="
echo "Cluster is running!"
echo "Coordinator : localhost:${COORDINATOR_PORT}"
if [ "$START_GATEWAY" = "true" ]; then
  echo "Gateway     : localhost:${GATEWAY_PORT}"
fi
echo "Logs  : $LOG_DIR"
echo "Data  : $DATA_DIR"
echo "Stop  : ./scripts/run_cluster.sh stop"
echo "================================================="