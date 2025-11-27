#!/bin/bash

# run_server.sh - Script to start a single KV Node Server

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_DIR="$( cd "$SCRIPT_DIR/.." && pwd )/kv.server"
CLIENT_JAR="$PROJECT_DIR/target/kv.server-1.0-SNAPSHOT.jar"

# Check if jar exists
if [ ! -f "$CLIENT_JAR" ]; then
  echo "Client JAR not found: $CLIENT_JAR"
  echo "Please build the client first using: mvn package -f $PROJECT_DIR/pom.xml"
  exit 1
fi

# Start server in foreground
echo "Starting KV Server..."

nohup java -jar "$CLIENT_JAR" \
  > "$LOG_DIR/node.log" 2>&1 &