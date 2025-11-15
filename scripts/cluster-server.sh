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
    echo "❌ Already running."
    exit 1
  fi

  require_files
  echo "🚀 Starting ClusterServer..."
  nohup java -jar "$CLUSTER_SERVER_JAR" > "$LOG_FILE" 2>&1 &
  echo $! > "$PID_FILE"
  sleep 10

  if is_running; then
    echo "✅ ClusterServer started. Logs → $LOG_FILE"
    echo "   Node servers will be managed by the ClusterServer."
  else
    echo "❌ Failed to start. Check logs → $LOG_FILE"
    exit 1
  fi
}

status_server() {
  if is_running; then
    echo "🟢 ClusterServer running"
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
  status)  status_server ;;
  logs)    show_logs ;;
  *)
    cat <<USAGE
Usage: $0 {start|status|logs}

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