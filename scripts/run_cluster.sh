#!/bin/bash

set -e

############################################
# CONFIG
############################################

BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd -P )"

# app-config.yml uses data directories relative to the working directory, so always run from the repo
# to keep state under $BASE_DIR/data regardless of where the script was invoked.
cd "$BASE_DIR"

COORDINATOR_JAR="$BASE_DIR/kv.coordinator/target/kv-coordinator.jar"
NODE_JAR="$BASE_DIR/kv.node/target/kv-node.jar"
GATEWAY_JAR="$BASE_DIR/kv.gateway/target/kv-gateway.jar"
ADMIN_JAR="$BASE_DIR/kv.admin/target/kv-admin.jar"

LOG_DIR="$BASE_DIR/logs"
DATA_DIR="$BASE_DIR/data"
# Records "<component> <pid>" for every process this checkout started; `stop` only signals these.
# Dot-file under data/ so `rm -rf logs/*` and `rm -rf data/*` leave it alone.
PID_FILE="$DATA_DIR/.run_cluster.pids"

# Number of coordinator nodes (for Raft cluster)
N_COORDINATORS=${N_COORDINATORS:-3}

# Number of storage nodes
N_NODES=${N_NODES:-2}

# Base ports (these mirror kv.common app-config.yml, which is what the processes actually bind).
# Coordinator seed endpoints match Docker Compose: localhost:9001-9003.
COORDINATOR_BASE_PORT=${COORDINATOR_BASE_PORT:-9001}
NODE_BASE_PORT=${NODE_BASE_PORT:-8001}
# Gateway (gRPC) defaults to 7000 in code
GATEWAY_PORT=${GATEWAY_PORT:-7000}
# Admin API (HTTP) defaults to 8089 in code
ADMIN_PORT=${ADMIN_PORT:-8089}

# Start gateway (optional)
START_GATEWAY=${START_GATEWAY:-true}
# Start admin server (optional)
START_ADMIN=${START_ADMIN:-true}

# Max seconds to wait for each component to open its port, and to wait for processes to exit on stop.
STARTUP_TIMEOUT_SECONDS=${STARTUP_TIMEOUT_SECONDS:-60}
STOP_TIMEOUT_SECONDS=${STOP_TIMEOUT_SECONDS:-35}

export KVDB_ENV="${KVDB_ENV:-local}"
export KVDB_GRPC_SECURITY_MODE="${KVDB_GRPC_SECURITY_MODE:-development-plaintext}"

# Fail fast if build artifacts are missing (otherwise java will exit immediately and you won't see server logs).
require_file() {
  local f="$1"
  if [ ! -f "$f" ]; then
    echo "❌ Missing file: $f" >&2
    echo "   Did you run: make build ?" >&2
    exit 1
  fi
}

# Succeeds when something accepts TCP connections on the loopback port.
port_open() {
  (exec 3<>"/dev/tcp/127.0.0.1/$1") >/dev/null 2>&1
}

# Print the full command line of <pid> (empty when the process is gone). Falls back to pgrep when ps is unusable.
process_command() {
  local pid="$1"
  kill -0 "$pid" 2>/dev/null || return 0
  ps -p "$pid" -o command= 2>/dev/null && return 0
  pgrep -fl java 2>/dev/null | awk -v p="$pid" '$1 == p { sub(/^[0-9]+ /, ""); print }'
}

# Print "<name> <pid>" for the entries recorded that are still alive and running one of this checkout's jars
# (guards against a recycled PID belonging to an unrelated process).
live_recorded_pids() {
  [ -f "$PID_FILE" ] || return 0
  local name pid cmd
  while read -r name pid; do
    [ -n "$pid" ] || continue
    cmd="$(process_command "$pid")"
    case "$cmd" in
      *"$BASE_DIR"/kv.*/target/*.jar*) echo "$name $pid" ;;
    esac
  done < "$PID_FILE"
}

# Start a component in the background and record its PID. Sets LAUNCHED_PID.
# usage: launch <name> <log file> <command...>
launch() {
  local name="$1" log="$2"
  shift 2
  : > "$log"
  nohup "$@" > "$log" 2>&1 &
  LAUNCHED_PID=$!
  echo "$name $LAUNCHED_PID" >> "$PID_FILE"
}

# Tear down whatever this run started, then exit nonzero naming the failed component.
fail_component() {
  local name="$1" reason="$2" log="$3"
  echo "" >&2
  echo "❌ $name $reason" >&2
  if [ -s "$log" ]; then
    echo "   Last lines of $log:" >&2
    tail -n 15 "$log" | sed 's/^/   | /' >&2
  fi
  echo "   Stopping components started by this run..." >&2
  stop_cluster >&2
  exit 1
}

# Wait until <pid> is listening on <port>; fail naming <name> if it exits or the timeout elapses.
# usage: await_ready <name> <pid> <port> <log>
await_ready() {
  local name="$1" pid="$2" port="$3" log="$4"
  local ticks=0 max_ticks=$((STARTUP_TIMEOUT_SECONDS * 4))
  while [ "$ticks" -lt "$max_ticks" ]; do
    if ! kill -0 "$pid" 2>/dev/null; then
      fail_component "$name" "exited during startup (PID $pid)." "$log"
    fi
    if port_open "$port"; then
      echo "  $name ready on port $port"
      return 0
    fi
    sleep 0.25
    ticks=$((ticks + 1))
  done
  fail_component "$name" "did not open port $port within ${STARTUP_TIMEOUT_SECONDS}s (PID $pid)." "$log"
}

# Validate everything we can before any process starts.
preflight() {
  require_file "$COORDINATOR_JAR"
  require_file "$NODE_JAR"
  if [ "$START_GATEWAY" = "true" ]; then
    require_file "$GATEWAY_JAR"
  fi
  if [ "$START_ADMIN" = "true" ]; then
    require_file "$ADMIN_JAR"
    if [ -z "${KVDB_ADMIN_SECURITY_API_KEY// /}" ]; then
      echo "❌ KVDB_ADMIN_SECURITY_API_KEY is not set." >&2
      echo "   The Admin API refuses to start without an API key. Export one and retry, e.g.:" >&2
      echo "     export KVDB_ADMIN_SECURITY_API_KEY=\"\$(openssl rand -hex 32)\"" >&2
      echo "   (or set START_ADMIN=false to run without the Admin API)" >&2
      exit 1
    fi
  fi

  if [ -n "$(live_recorded_pids)" ]; then
    echo "❌ A cluster started from this checkout is already running. Run: ./scripts/run_cluster.sh stop" >&2
    exit 1
  fi

  local busy="" i p
  for ((i=0; i<N_COORDINATORS; i++)); do
    p=$((COORDINATOR_BASE_PORT + i)); port_open "$p" && busy="$busy $p"
  done
  for ((i=0; i<N_NODES; i++)); do
    p=$((NODE_BASE_PORT + i)); port_open "$p" && busy="$busy $p"
  done
  if [ "$START_GATEWAY" = "true" ] && port_open "$GATEWAY_PORT"; then busy="$busy $GATEWAY_PORT"; fi
  if [ "$START_ADMIN" = "true" ] && port_open "$ADMIN_PORT"; then busy="$busy $ADMIN_PORT"; fi
  if [ -n "$busy" ]; then
    echo "❌ Port(s) already in use:$busy" >&2
    echo "   Another cluster (possibly from a different checkout) is running; stop it or free the port(s)." >&2
    exit 1
  fi
}

mkdir -p "$LOG_DIR"
mkdir -p "$DATA_DIR"

############################################
# FUNCTIONS
############################################

start_coordinator() {
  echo "Starting $N_COORDINATORS Coordinator(s)..."

  local -a pids=()
  local i
  for ((i=1; i<= N_COORDINATORS; i++)); do
    local coordinator_id="coordinator-$i"
    local coordinator_port=$((COORDINATOR_BASE_PORT + i - 1))

    launch "$coordinator_id" "$LOG_DIR/coordinator-$i.log" \
      env COORDINATOR_NODE_ID="$coordinator_id" KVDB_IDENTITY_ROLE=coordinator KVDB_IDENTITY_PRINCIPAL="$coordinator_id" \
      java -jar "$COORDINATOR_JAR"
    pids[$i]=$LAUNCHED_PID

    echo "Coordinator #$i started (NODE_ID=$coordinator_id, PID: $LAUNCHED_PID, port: $coordinator_port)"
    echo "  Log file: $LOG_DIR/coordinator-$i.log"
  done

  for ((i=1; i<= N_COORDINATORS; i++)); do
    await_ready "coordinator-$i" "${pids[$i]}" "$((COORDINATOR_BASE_PORT + i - 1))" "$LOG_DIR/coordinator-$i.log"
  done
}

start_nodes() {
  echo "Starting $N_NODES Data Node(s)..."

  local -a pids=()
  local i
  for ((i=1; i<= N_NODES; i++)); do
    local node_id="node-$i"

    launch "$node_id" "$LOG_DIR/node-$i.log" \
      env STORAGE_NODE_ID="$node_id" KVDB_IDENTITY_ROLE=storage-node KVDB_IDENTITY_PRINCIPAL="$node_id" \
      java -jar "$NODE_JAR"
    pids[$i]=$LAUNCHED_PID

    echo "Data-Node #$i started (NODE_ID=$node_id, PID: $LAUNCHED_PID, port: $((NODE_BASE_PORT + i - 1)))"
    echo "  Log file: $LOG_DIR/node-$i.log"
  done

  for ((i=1; i<= N_NODES; i++)); do
    await_ready "node-$i" "${pids[$i]}" "$((NODE_BASE_PORT + i - 1))" "$LOG_DIR/node-$i.log"
  done
}

start_gateway() {
  if [ "$START_GATEWAY" = "true" ]; then
    echo "Starting Gateway..."

    launch gateway "$LOG_DIR/gateway.log" \
      env KVDB_IDENTITY_ROLE=gateway KVDB_IDENTITY_PRINCIPAL=gateway-local java -jar "$GATEWAY_JAR"

    echo "Gateway started (PID: $LAUNCHED_PID, port: $GATEWAY_PORT)"
    echo "  Log file: $LOG_DIR/gateway.log"

    await_ready gateway "$LAUNCHED_PID" "$GATEWAY_PORT" "$LOG_DIR/gateway.log"
  fi
}

start_admin() {
  if [ "$START_ADMIN" = "true" ]; then
    echo "Starting Admin API..."

    launch admin "$LOG_DIR/admin.log" \
      env SERVER_PORT="$ADMIN_PORT" KVDB_IDENTITY_ROLE=admin KVDB_IDENTITY_PRINCIPAL=admin-local java -jar "$ADMIN_JAR"

    echo "Admin API started (PID: $LAUNCHED_PID, port: $ADMIN_PORT)"
    echo "  Log file: $LOG_DIR/admin.log"

    await_ready admin "$LAUNCHED_PID" "$ADMIN_PORT" "$LOG_DIR/admin.log"
  fi
}

# Stop only the processes recorded in the pidfile by this checkout (never other checkouts' clusters).
stop_cluster() {
  echo "Stopping cluster processes started from $BASE_DIR..."
  local entries name pid waited
  entries="$(live_recorded_pids)"
  if [ -z "$entries" ]; then
    echo "No running cluster processes recorded for this checkout."
    rm -f "$PID_FILE"
    return 0
  fi

  while read -r name pid; do
    [ -n "$pid" ] || continue
    echo "  Stopping $name (PID: $pid)"
    kill "$pid" 2>/dev/null || true
  done <<< "$entries"

  waited=0
  while [ "$waited" -lt "$((STOP_TIMEOUT_SECONDS * 4))" ] && [ -n "$(live_recorded_pids)" ]; do
    sleep 0.25
    waited=$((waited + 1))
  done

  entries="$(live_recorded_pids)"
  if [ -n "$entries" ]; then
    while read -r name pid; do
      [ -n "$pid" ] || continue
      echo "  $name (PID: $pid) did not exit within ${STOP_TIMEOUT_SECONDS}s; sending SIGKILL"
      kill -9 "$pid" 2>/dev/null || true
    done <<< "$entries"
  fi

  rm -f "$PID_FILE"
  echo "Cluster stopped."
}

is_running() {
  local port="${COORDINATOR_BASE_PORT}"

  # Check if any process is listening on the first coordinator port (IPv4 or IPv6)
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

preflight

echo "================================================="
echo " Spinning up Distributed kvdb Cluster"
echo "================================================="
echo "Coordinators: $N_COORDINATORS (ports: $COORDINATOR_BASE_PORT-$((COORDINATOR_BASE_PORT + N_COORDINATORS - 1)))"
echo "Data Nodes  : $N_NODES"
if [ "$START_GATEWAY" = "true" ]; then
  echo "Gateway     : localhost:${GATEWAY_PORT}"
fi
if [ "$START_ADMIN" = "true" ]; then
  echo "Admin API   : localhost:${ADMIN_PORT}"
fi
echo "================================================="

start_coordinator
start_nodes
start_gateway
start_admin

echo ""
echo "================================================="
echo "Cluster is running!"
echo "Coordinators: $N_COORDINATORS (ports: $COORDINATOR_BASE_PORT-$((COORDINATOR_BASE_PORT + N_COORDINATORS - 1)))"
for ((i=1; i<= N_COORDINATORS; i++)); do
  port=$((COORDINATOR_BASE_PORT + i - 1))
  echo "  - Coordinator #$i: localhost:$port"
done
if [ "$START_GATEWAY" = "true" ]; then
  echo "Gateway     : localhost:${GATEWAY_PORT}"
fi
if [ "$START_ADMIN" = "true" ]; then
  echo "Admin API   : localhost:${ADMIN_PORT}"
fi
echo "Logs  : $LOG_DIR"
echo "Data  : $DATA_DIR"
echo "gRPC security mode: $KVDB_GRPC_SECURITY_MODE (valid only for explicit local development)"
echo "Stop  : ./scripts/run_cluster.sh stop"
echo "================================================="
