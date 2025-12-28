#!/bin/bash
# run_client.sh - Script to start the KV Client interactively

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

PROJECT_DIR="$( cd "$SCRIPT_DIR/.." && pwd )/kv.client"

CLIENT_JAR="$PROJECT_DIR/target/kv-node.jar"

# Check if jar exists
if [ ! -f "$CLIENT_JAR" ]; then
  echo "Client JAR not found: $CLIENT_JAR"
  echo "Please build the client first using: mvn package -f $PROJECT_DIR/pom.xml"
  exit 1
fif

# Start client in foreground
echo "Starting KV Client..."
java -jar "$CLIENT_JAR"