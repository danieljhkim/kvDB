#!/usr/bin/env bash
set -euo pipefail

# Deterministic response-validation checks. grpcurl and the bootstrap script
# are replaced in a private fixture tree; no cluster or network is involved.
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
scratch_root="${ORBIT_SCRATCH_DIR:-$repo_root/.orbit/tmp}"
mkdir -p "$scratch_root"
work="$(mktemp -d "$scratch_root/smoke-response-test.XXXXXX")"
cleanup() {
  python3 - "$work" <<'PY'
import shutil
import sys
shutil.rmtree(sys.argv[1], ignore_errors=True)
PY
}
trap cleanup EXIT

sandbox="$work/repo"
mkdir -p "$sandbox/scripts" "$sandbox/kv.proto/src/main/proto" "$work/bin"
cp "$repo_root/scripts/smoke_test.sh" "$sandbox/scripts/smoke_test.sh"
cp "$repo_root/scripts/cluster_helpers.sh" "$sandbox/scripts/cluster_helpers.sh"
cat > "$sandbox/scripts/bootstrap_cluster.sh" <<'STUB'
#!/usr/bin/env bash
exit 0
STUB
cat > "$work/bin/grpcurl" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
method="${@: -1}"
printf '%s\n' "${method##*/}" >> "$STUB_RPC_LOG"
case "${method##*/}" in
  Put)
    if [[ "${STUB_PUT_EXIT:-0}" != 0 ]]; then exit "$STUB_PUT_EXIT"; fi
    printf '%s\n' "${STUB_PUT_RESPONSE:-}"
    ;;
  Get)
    if [[ "${STUB_GET_EXIT:-0}" != 0 ]]; then exit "$STUB_GET_EXIT"; fi
    printf '%s\n' "${STUB_GET_RESPONSE:-}"
    ;;
  *) echo "unexpected grpcurl method: $method" >&2; exit 1 ;;
esac
STUB
chmod +x "$sandbox/scripts/bootstrap_cluster.sh" "$work/bin/grpcurl"

smoke="$sandbox/scripts/smoke_test.sh"
key='c21va2Uta2V5MTA='
value='aGVsbG8xMA=='
stale_value='b2xkLWhlbGxv'
export STUB_RPC_LOG="$work/rpc.log"
export PATH="$work/bin:$PATH"
export GATEWAY_ADDR="stub-gateway:7000"
export COORDINATOR_ADDRS="stub-coordinator:9001"
export KVDB_ENV=test
export KVDB_GRPC_SECURITY_MODE=development-plaintext

failures=0
pass() { printf 'PASS: %s\n' "$1"; }
fail() { printf 'FAIL: %s\n' "$1" >&2; failures=$((failures + 1)); }
set_fixture() {
  export STUB_PUT_RESPONSE="$1"
  export STUB_GET_RESPONSE="$2"
  export STUB_PUT_EXIT="${3:-0}"
  export STUB_GET_EXIT="${4:-0}"
  : > "$STUB_RPC_LOG"
}
expect_failure() {
  local label="$1"
  if "$smoke" > "$work/output" 2>&1; then
    fail "$label (unexpectedly passed)"
    cat "$work/output" >&2
  else
    pass "$label"
  fi
}
expect_success() {
  local label="$1"
  if "$smoke" > "$work/output" 2>&1; then
    if grep -q 'Smoke test passed' "$work/output"; then
      pass "$label"
    else
      fail "$label (pass line missing)"
      cat "$work/output" >&2
    fi
  else
    fail "$label"
    cat "$work/output" >&2
  fi
}

# An application-level failed Put must stop the check even when Get can still
# return the matching value left by an earlier run.
set_fixture '{"status":{"code":"UNAVAILABLE"},"version":"1"}' \
  "{\"status\":{\"code\":\"OK\"},\"kv\":{\"key\":\"$key\",\"value\":\"$value\"}}"
expect_failure "non-OK Put fails despite a readable stale value"
if [[ "$(cat "$STUB_RPC_LOG")" == Put ]]; then
  pass "failed Put prevents a later Get from masking it"
else
  fail "failed Put RPC sequence: $(cat "$STUB_RPC_LOG")"
fi

set_fixture '{"status":{"code":"OK"},"version":"2"}' \
  "{\"status\":{\"code\":\"OK\"},\"kv\":{\"key\":\"$key\",\"value\":\"$stale_value\"}}"
expect_failure "successful Put with stale Get value fails"

set_fixture '{not-json' '{}'
expect_failure "malformed Put JSON fails"

set_fixture '{"status":{"code":"OK"},"version":"2"}' '{not-json'
expect_failure "malformed Get JSON fails"

set_fixture '{"status":{"code":"OK"},"version":"2"}' \
  "{\"status\":{\"code\":\"OK\"},\"kv\":{\"value\":\"$value\"}}"
expect_failure "Get without the returned key fails"

set_fixture '{"status":{"code":"OK"}}' \
  "{\"status\":{\"code\":\"OK\"},\"kv\":{\"key\":\"$key\",\"value\":\"$value\"}}"
expect_failure "Put without a committed version fails"

set_fixture '{"status":{"code":"OK"},"version":"2"}' \
  '{"status":{"code":"OK"},"kv":{"key":"b3RoZXI=","value":"aGVsbG8xMA=="}}'
expect_failure "Get with the wrong key fails"

set_fixture '{"status":{"code":"OK"},"version":"2"}' \
  "{\"status\":{\"code\":\"NOT_FOUND\"},\"kv\":{\"key\":\"$key\",\"value\":\"$value\"}}"
expect_failure "non-OK Get status fails despite matching fields"

set_fixture '{"status":{"code":"OK"},"version":"2"}' '{}' 17
expect_failure "Put transport failure fails"

set_fixture '{"status":{"code":"OK"},"version":"3"}' \
  "{\"status\":{\"code\":\"OK\"},\"kv\":{\"key\":\"$key\",\"value\":\"$value\"}}"
expect_success "enum-name OK responses with exact key/value pass"

set_fixture '{"status":{"code":1},"version":4}' \
  "{\"status\":{\"code\":1},\"kv\":{\"key\":\"$key\",\"value\":\"$value\"}}"
expect_success "numeric OK responses with exact key/value pass"

if (( failures > 0 )); then
  printf '%d check(s) failed\n' "$failures" >&2
  exit 1
fi
echo "All smoke response checks passed"
