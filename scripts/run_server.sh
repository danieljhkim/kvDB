#!/usr/bin/env bash

set -euo pipefail

# run_server.sh - Script to start a single KV Node Server

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
BASE_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd -P)"
NODE_JAR="$BASE_DIR/kv.node/target/kv-node.jar"
LOG_DIR="${LOG_DIR:-$BASE_DIR/logs}"

if [[ ! -f "$NODE_JAR" ]]; then
  echo "Node JAR not found: $NODE_JAR" >&2
  echo "Build it from the checkout root with: mvn -f $BASE_DIR/pom.xml -pl kv.node -am package" >&2
  exit 1
fi

mkdir -p -- "$LOG_DIR"

echo "Starting KV Server..."
exec java -jar "$NODE_JAR" > "$LOG_DIR/node.log" 2>&1
