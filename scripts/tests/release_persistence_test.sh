#!/usr/bin/env bash
set -euo pipefail

# Deterministic regression test for the release-rehearsal persistence checks.
# A stub grpcurl backed by a directory-based store replaces the cluster, so no
# Docker, coordinator, storage node, or load is involved. The store can be
# emptied to model total data loss, and every RPC is recorded so the test can
# prove that `smoke_test.sh verify` never writes or bootstraps.

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
smoke="$repo_root/scripts/smoke_test.sh"
checklist="$repo_root/docs/release-checklist.md"

work="${ORBIT_SCRATCH_DIR:-${TMPDIR:-/tmp}}/release-persistence-test.$$"
mkdir -p "$work/bin"
trap 'rm -rf "$work"' EXIT

export STUB_STORE="$work/store"
export STUB_RPC_LOG="$work/rpc.log"
export GATEWAY_ADDR="stub-gateway:7000"
export COORDINATOR_ADDRS="stub-coordinator:9001"
export LEADER_DISCOVERY_TIMEOUT_SECONDS=2
export N_NODES=2

cat > "$work/bin/grpcurl" <<'STUB'
#!/usr/bin/env bash
# Stub grpcurl: in-memory-on-disk KvGateway and Coordinator.
set -euo pipefail
data='{}'
args=("$@")
for ((i=0; i<${#args[@]}; i++)); do
  [[ "${args[$i]}" == "-d" ]] && data="${args[$((i + 1))]}"
done
method="${args[$((${#args[@]} - 1))]}"
rpc="${method##*/}"
printf '%s\n' "$rpc" >> "$STUB_RPC_LOG"
mkdir -p "$STUB_STORE"

field() { sed -n "s/.*\"$1\":\"\([^\"]*\)\".*/\1/p" <<< "$data"; }
slot() { printf '%s' "$1" | shasum -a 256 | cut -d' ' -f1; }

case "$rpc" in
  GetCoordinatorLeader) echo '{"isLeader": true, "leaderId": "coordinator1"}' ;;
  RegisterNode|InitShards) echo '{"success": true}' ;;
  Put)
    key="$(field key)"; value="$(field value)"; file="$STUB_STORE/$(slot "$key")"
    version=1
    if [[ -f "$file" ]]; then version=$(( $(sed -n 2p "$file") + 1 )); fi
    printf '%s\n%s\n' "$value" "$version" > "$file"
    echo "{\"status\": {\"code\": \"OK\"}, \"version\": \"$version\"}"
    ;;
  Get)
    key="$(field key)"; file="$STUB_STORE/$(slot "$key")"
    if [[ -f "$file" ]]; then
      echo "{\"status\": {\"code\": \"OK\"}, \"kv\": {\"key\": \"$key\", \"value\": \"$(sed -n 1p "$file")\", \"version\": \"$(sed -n 2p "$file")\"}}"
    else
      echo '{"status": {"code": "NOT_FOUND"}}'
    fi
    ;;
  *) echo "stub grpcurl: unexpected method $method" >&2; exit 1 ;;
esac
STUB
chmod +x "$work/bin/grpcurl"
export PATH="$work/bin:$PATH"

failures=0
pass() { printf 'PASS: %s\n' "$1"; }
fail() { printf 'FAIL: %s\n' "$1" >&2; failures=$((failures + 1)); }

expect_ok() { # label cmd...
  local label="$1"; shift
  if "$@" >"$work/out.log" 2>&1; then pass "$label"; else fail "$label"; cat "$work/out.log" >&2; fi
}
expect_fail() { # label cmd...
  local label="$1"; shift
  if "$@" >"$work/out.log" 2>&1; then fail "$label (unexpectedly passed)"; cat "$work/out.log" >&2; else pass "$label"; fi
}
reset_cluster() { rm -rf "$STUB_STORE"; : > "$STUB_RPC_LOG"; }
lose_all_data() { rm -rf "$STUB_STORE"; mkdir -p "$STUB_STORE"; }
rpcs_since() { tail -n +"$(($1 + 1))" "$STUB_RPC_LOG"; }
rpc_count() { wc -l < "$STUB_RPC_LOG" | tr -d ' '; }
no_mutation_since() { # line-count-before
  ! rpcs_since "$1" | grep -Eq '^(Put|Delete|RegisterNode|InitShards)$'
}

fixture_dir="$work/fixtures"   # deliberately outside STUB_STORE

# 1. Default (write) mode keeps its bootstrap + Put/Get behaviour.
reset_cluster
expect_ok "write mode round-trips a value" "$smoke"
if [[ "$(grep -v '^GetCoordinatorLeader$' "$STUB_RPC_LOG" | paste -sd, -)" == "RegisterNode,RegisterNode,InitShards,Put,Get" ]]; then
  pass "write mode RPC order is bootstrap, Put, Get"
else
  fail "write mode RPC order: $(paste -sd, "$STUB_RPC_LOG")"
fi

# 2. The reported defect: write mode cannot detect total data loss.
lose_all_data
expect_ok "counterfactual: write mode still passes after all data lost" "$smoke"

# 3. seed records expectations outside the store; verify is read-only.
reset_cluster
export SMOKE_FIXTURE_FILE="$fixture_dir/base.tsv"
expect_ok "seed writes fixtures" "$smoke" seed
[[ "$(wc -l < "$SMOKE_FIXTURE_FILE" | tr -d ' ')" == "3" ]] && pass "fixture records three key/value/version rows" || fail "fixture rows"
awk -F'\t' 'NF == 3 && $3 ~ /^[0-9]+$/ { ok++ } END { exit ok == 3 ? 0 : 1 }' "$SMOKE_FIXTURE_FILE" && pass "fixture rows carry numeric versions" || fail "fixture version column"
case "$SMOKE_FIXTURE_FILE" in "$STUB_STORE"/*) fail "fixture lives inside the store" ;; *) pass "fixture lives outside the store" ;; esac
expect_fail "seed refuses to overwrite an existing fixture" "$smoke" seed
unset SMOKE_FIXTURE_FILE
expect_fail "verify requires SMOKE_FIXTURE_FILE" "$smoke" verify
SMOKE_FIXTURE_FILE="$fixture_dir/missing.tsv" expect_fail "verify rejects a missing fixture file" "$smoke" verify
expect_fail "unknown mode is rejected" "$smoke" bogus

# 4. Each persistence phase: intact storage passes, total loss fails, and
#    verification issues no mutating or bootstrap RPC.
backup="$work/backup"
for phase in migration restored-volume rollback; do
  reset_cluster
  export SMOKE_FIXTURE_FILE="$fixture_dir/$phase.tsv"
  "$smoke" seed >/dev/null

  case "$phase" in
    restored-volume) rm -rf "$backup"; cp -R "$STUB_STORE" "$backup"; lose_all_data; cp -R "$backup"/. "$STUB_STORE"/ ;;
  esac

  before="$(rpc_count)"
  expect_ok "$phase: intact storage passes verify" "$smoke" verify
  no_mutation_since "$before" && pass "$phase: intact verify issued no Put/bootstrap RPC" || fail "$phase: intact verify mutated cluster state"

  lose_all_data
  before="$(rpc_count)"
  expect_fail "$phase: all-data-lost storage fails verify" "$smoke" verify
  no_mutation_since "$before" && pass "$phase: failing verify issued no Put/bootstrap RPC" || fail "$phase: failing verify mutated cluster state"
  [[ -z "$(ls -A "$STUB_STORE")" ]] && pass "$phase: verify did not recreate state" || fail "$phase: store repopulated by verify"
done

# 5. Partial corruption: a changed value or version is also detected.
reset_cluster
export SMOKE_FIXTURE_FILE="$fixture_dir/partial.tsv"
"$smoke" seed >/dev/null
victim="$(ls "$STUB_STORE" | head -n 1)"
original="$(cat "$STUB_STORE/$victim")"
printf '%s\n%s\n' "$(sed -n 1p "$STUB_STORE/$victim")" 99 > "$STUB_STORE/$victim"
expect_fail "version drift fails verify" "$smoke" verify
printf '%s\n%s\n' "Zm9yZ2VkLXZhbHVl" "$(sed -n 2p <<< "$original")" > "$STUB_STORE/$victim"
expect_fail "value drift fails verify" "$smoke" verify
printf '%s\n' "$original" > "$STUB_STORE/$victim"
expect_ok "restored original state passes verify" "$smoke" verify
unset SMOKE_FIXTURE_FILE

# 6. The runbook orders read-only verification before any write or bootstrap
#    and keeps fixtures outside the tested volumes.
section() { awk -v n="$1" '$0 ~ "^## "n"\\." { on=1; next } /^## / { on=0 } on' "$checklist"; }
for n in 4 5 6; do
  section "$n" | grep -q 'smoke_test.sh verify' && pass "runbook section $n uses verify mode" || fail "runbook section $n lacks verify mode"
done
if grep -nE 'smoke_test\.sh[[:space:]]*$' "$checklist" | grep -v 'smoke_test.sh verify'; then
  fail "runbook still invokes smoke_test.sh without an explicit mode"
else
  pass "runbook always names a smoke mode"
fi
for n in 5 6; do
  if section "$n" | grep -Eq 'bootstrap_cluster\.sh|smoke_test\.sh (write|seed)'; then
    fail "runbook section $n runs bootstrap or a write before verification"
  else
    pass "runbook section $n runs no bootstrap or write"
  fi
done
sec4="$(section 4)"
seed_line="$(grep -n 'smoke_test.sh seed' <<< "$sec4" | head -n 1 | cut -d: -f1)"
upgrade_line="$(grep -n 'KVDB_IMAGE_TAG="\$RELEASE_TAG"' <<< "$sec4" | head -n 1 | cut -d: -f1)"
verify_line="$(grep -n 'smoke_test.sh verify' <<< "$sec4" | head -n 1 | cut -d: -f1)"
if [[ -n "$seed_line" && -n "$upgrade_line" && -n "$verify_line" ]] \
  && (( seed_line < upgrade_line && upgrade_line < verify_line )); then
  pass "runbook section 4 seeds, upgrades, then verifies"
else
  fail "runbook section 4 ordering (seed=$seed_line upgrade=$upgrade_line verify=$verify_line)"
fi
grep -q 'SMOKE_FIXTURE_FILE' "$checklist" && grep -q 'persistence.tsv' "$checklist" && pass "runbook records the durable fixture file" || fail "runbook fixture file"

if (( failures > 0 )); then
  printf '%d check(s) failed\n' "$failures" >&2
  exit 1
fi
echo "All release persistence checks passed"
