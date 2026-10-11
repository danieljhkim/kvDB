#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd -P)"
SCRATCH_ROOT="$REPO_ROOT/.orbit/tmp"
mkdir -p -- "$SCRATCH_ROOT"
TEST_ROOT="$(mktemp -d "$SCRATCH_ROOT/server-launcher-test.XXXXXX")"
cleanup() {
  if [[ -d "$TEST_ROOT" ]]; then
    rm -r -- "$TEST_ROOT"
  fi
}
trap cleanup EXIT

CHECKOUT="$TEST_ROOT/checkout"
MOCK_BIN="$TEST_ROOT/bin"
OUTSIDE_DIR="$TEST_ROOT/outside"
ARGS_FILE="$TEST_ROOT/java-args"
PID_FILE="$TEST_ROOT/java-pid"
OVERRIDE_LOG_DIR="$TEST_ROOT/custom-logs"
mkdir -p -- "$CHECKOUT/scripts" "$CHECKOUT/kv.node/target" "$MOCK_BIN" "$OUTSIDE_DIR"
cp "$REPO_ROOT/scripts/run_server.sh" "$CHECKOUT/scripts/run_server.sh"
cp "$REPO_ROOT/pom.xml" "$CHECKOUT/pom.xml"

cat > "$MOCK_BIN/java" <<'MOCK_JAVA'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$@" > "$JAVA_ARGS_FILE"
printf '%s\n' "$$" > "$JAVA_PID_FILE"
printf 'mock java ran\n'
MOCK_JAVA
chmod +x "$MOCK_BIN/java"

fail() {
  printf '%s\n' "$1" >&2
  exit 1
}

unset LOG_DIR
if missing_output="$(cd "$OUTSIDE_DIR" && "$CHECKOUT/scripts/run_server.sh" 2>&1)"; then
  fail "launcher unexpectedly succeeded without the node jar"
fi
if [[ "$missing_output" != *"Node JAR not found: $CHECKOUT/kv.node/target/kv-node.jar"* ]]; then
  fail "missing-jar message did not identify the kv.node artifact"
fi
expected_build="mvn -f $CHECKOUT/pom.xml -pl kv.node -am package"
if [[ "$missing_output" != *"$expected_build"* ]]; then
  fail "missing-jar message did not provide the valid reactor build command"
fi
if [[ ! -f "$CHECKOUT/pom.xml" ]] || ! grep -Fq '<module>kv.node</module>' "$CHECKOUT/pom.xml"; then
  fail "suggested build command does not point to the root reactor containing kv.node"
fi
if [[ -e "$ARGS_FILE" ]]; then
  fail "launcher invoked Java despite the missing jar"
fi

assert_java_args() {
  local expected_jar="$1"
  local -a args=()
  local arg
  while IFS= read -r arg; do
    args+=("$arg")
  done < "$ARGS_FILE"
  if [[ "${#args[@]}" -ne 2 || "${args[0]-}" != "-jar" || "${args[1]-}" != "$expected_jar" ]]; then
    fail "Java was not invoked with -jar and the expected artifact: $expected_jar"
  fi
}

assert_mock_exited() {
  local pid
  pid="$(cat "$PID_FILE")"
  if kill -0 "$pid" 2>/dev/null; then
    fail "mock Java process was left running: $pid"
  fi
}

: > "$CHECKOUT/kv.node/target/kv-node.jar"
default_output="$(cd "$OUTSIDE_DIR" && PATH="$MOCK_BIN:$PATH" JAVA_ARGS_FILE="$ARGS_FILE" JAVA_PID_FILE="$PID_FILE" "$CHECKOUT/scripts/run_server.sh")"
if [[ "$default_output" != *"Starting KV Server..."* ]]; then
  fail "launcher did not report that it was starting the server"
fi
assert_java_args "$CHECKOUT/kv.node/target/kv-node.jar"
assert_mock_exited
if [[ ! -f "$CHECKOUT/logs/node.log" ]] || ! grep -Fq 'mock java ran' "$CHECKOUT/logs/node.log"; then
  fail "default log was not created under the checkout"
fi

override_output="$(cd "$OUTSIDE_DIR" && PATH="$MOCK_BIN:$PATH" JAVA_ARGS_FILE="$ARGS_FILE" JAVA_PID_FILE="$PID_FILE" LOG_DIR="$OVERRIDE_LOG_DIR" "$CHECKOUT/scripts/run_server.sh")"
if [[ "$override_output" != *"Starting KV Server..."* ]]; then
  fail "launcher did not start with the LOG_DIR override"
fi
assert_java_args "$CHECKOUT/kv.node/target/kv-node.jar"
assert_mock_exited
if [[ ! -f "$OVERRIDE_LOG_DIR/node.log" ]] || ! grep -Fq 'mock java ran' "$OVERRIDE_LOG_DIR/node.log"; then
  fail "LOG_DIR override was not used for the node log"
fi

printf 'server launcher regression checks passed\n'
