#!/usr/bin/env bash
# cluster-server.sh — Manage KV Cluster Server (start/stop/status/logs)

set -e
set -o pipefail

# --- Resolve paths exactly like your current script ---
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_DIR="$( cd "$SCRIPT_DIR/.." && pwd )/kv.coordinator"

CLUSTER_SERVER_JAR="$PROJECT_DIR/target/kv.coordinator-1.0-SNAPSHOT.jar"
CONFIG_FILE="$PROJECT_DIR/src/main/resources/cluster-config.yaml"

LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/cluster-server.log"
PID_FILE="$PROJECT_DIR/kv-coordinator.pid"

mkdir -p "$LOG_DIR"

# --- Helpers ---
is_running() {
  [[ -f "$PID_FILE" ]] || return 1
  local pid
  pid="$(cat "$PID_FILE" 2>/dev/null || true)"
  [[ -n "$pid" ]] || return 1
  kill -0 "$pid" 2>/dev/null
}

require_files() {
  if [[ ! -f "$CONFIG_FILE" ]]; then
    echo "❌ Configuration file not found: $CONFIG_FILE"
    exit 1
  fi
  if [[ ! -f "$CLUSTER_SERVER_JAR" ]]; then
    echo "❌ JAR not found: $CLUSTER_SERVER_JAR"
    echo "   Make sure to package the project first (e.g., mvn package)."
    exit 1
  fi
}

start_server() {
  if is_running; then
    echo "❌ Already running (PID: $(cat "$PID_FILE")). Use '$0 logs' or '$0 stop'."
    exit 1
  fi

  require_files
  echo "🚀 Starting ClusterServer..."
  # Note: keeping the same launch semantics as your script
  nohup java -jar "$CLUSTER_SERVER_JAR" > "$LOG_FILE" 2>&1 &
  echo $! > "$PID_FILE"
  sleep 1

  if is_running; then
    echo "✅ ClusterServer started (PID: $(cat "$PID_FILE")). Logs → $LOG_FILE"
    echo "   Node servers will be managed by the ClusterServer."
  else
    echo "❌ Failed to start. Check logs → $LOG_FILE"
    exit 1
  fi
}

stop_server() {
  if ! is_running; then
    echo "⚠️  Not running (no active PID)."
    [[ -f "$PID_FILE" ]] && rm -f "$PID_FILE"
    exit 0
  fi

  local pid
  pid="$(cat "$PID_FILE")"
  echo "🛑 Stopping ClusterServer (PID: $pid)..."
  kill "$pid" 2>/dev/null || true

  # Graceful wait
  for i in {1..20}; do
    if ! kill -0 "$pid" 2>/dev/null; then
      break
    fi
    sleep 0.3
  done

  # Force kill if still alive
  if kill -0 "$pid" 2>/dev/null; then
    echo "⚠️  Still running; sending SIGKILL..."
    kill -9 "$pid" 2>/dev/null || true
  fi

  rm -f "$PID_FILE"
  echo "✅ Stopped."
}

status_server() {
  if is_running; then
    echo "🟢 ClusterServer running (PID: $(cat "$PID_FILE"))"
    echo "   Log: $LOG_FILE"
  else
    echo "🔴 ClusterServer not running"
  fi
}

show_logs() {
  if [[ ! -f "$LOG_FILE" ]]; then
    echo "⚠️  No log file found at $LOG_FILE"
    exit 1
  fi
  echo "📜 Tailing logs: $LOG_FILE (Ctrl-C to exit)"
  tail -f "$LOG_FILE"
}

case "${1:-}" in
  start)   start_server ;;
  stop)    stop_server ;;
  restart) stop_server || true; start_server ;;
  status)  status_server ;;
  logs)    show_logs ;;
  *)
    cat <<USAGE
Usage: $0 {start|stop|restart|status|logs}

Paths:
  PROJECT_DIR        = $PROJECT_DIR
  CLUSTER_SERVER_JAR = $CLUSTER_SERVER_JAR
  CONFIG_FILE        = $CONFIG_FILE
  LOG_FILE           = $LOG_FILE
  PID_FILE           = $PID_FILE
USAGE
    exit 1
    ;;
esac