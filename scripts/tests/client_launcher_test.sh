#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd -P)"
SCRATCH_ROOT="${ORBIT_SCRATCH_DIR:-$REPO_ROOT/.orbit/tmp}"
mkdir -p -- "$SCRATCH_ROOT"
TEST_ROOT="$(mktemp -d "$SCRATCH_ROOT/client-launcher-test.XXXXXX")"
cleanup() {
  if [[ -d "$TEST_ROOT" ]]; then
    rm -r -- "$TEST_ROOT"
  fi
}
trap cleanup EXIT

CLEAN_CHECKOUT="$TEST_ROOT/clean-checkout"
MOCK_BIN="$TEST_ROOT/bin"
JAVA_MARKER="$TEST_ROOT/java-invoked"
mkdir -p -- "$CLEAN_CHECKOUT/scripts" "$MOCK_BIN"
cp "$REPO_ROOT/scripts/run_client.sh" "$CLEAN_CHECKOUT/scripts/run_client.sh"
chmod +x "$CLEAN_CHECKOUT/scripts/run_client.sh"

cat > "$MOCK_BIN/java" <<'MOCK_JAVA'
#!/usr/bin/env bash
set -euo pipefail
: "${JAVA_MARKER:?}"
printf 'java invoked\n' > "$JAVA_MARKER"
exit 97
MOCK_JAVA
chmod +x "$MOCK_BIN/java"

fail() {
  printf '%s\n' "$1" >&2
  exit 1
}

if ! bash -n "$REPO_ROOT/scripts/run_client.sh"; then
  fail "client launcher does not parse"
fi

if [[ -e "$CLEAN_CHECKOUT/kv.client/target/kv-node.jar" ]]; then
  fail "clean fixture unexpectedly contains the legacy client jar"
fi

if output="$(cd "$CLEAN_CHECKOUT" && PATH="$MOCK_BIN:$PATH" JAVA_MARKER="$JAVA_MARKER" "$CLEAN_CHECKOUT/scripts/run_client.sh" 2>&1)"; then
  fail "retired client launcher unexpectedly succeeded"
fi

if [[ "$output" != *"retired"* || "$output" != *"interactive clients are unsupported"* ]]; then
  fail "launcher did not explain that the legacy client is retired"
fi
if [[ "$output" != *"make go-build"* || "$output" != *"golang/kvcli/kv"* ]]; then
  fail "launcher did not provide the supported Go client migration path"
fi
if [[ -e "$JAVA_MARKER" ]]; then
  fail "retired launcher invoked Java"
fi

printf 'client launcher regression checks passed\n'
