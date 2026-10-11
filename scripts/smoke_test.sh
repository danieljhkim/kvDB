#!/bin/bash
set -euo pipefail

# Gateway smoke test with three modes:
#
#   write  (default) bootstrap coordinator (register nodes + init shards), then
#                    Put(key,value) and Get(key) a fixed key via the gateway.
#                    This proves a fresh write works. It does NOT prove earlier
#                    data survived, because it rewrites the value it reads.
#   seed             Put several fixture keys, read each back, and record the
#                    expected key/value/version to SMOKE_FIXTURE_FILE. Does not
#                    bootstrap.
#   verify           Read-only persistence check. Get every key recorded in
#                    SMOKE_FIXTURE_FILE and require the recorded value and
#                    version. Never bootstraps, registers nodes, or issues Put,
#                    so lost or reinitialized state cannot be recreated by it.
#
# Requires: grpcurl
#
# Usage:
#   ./scripts/run_cluster.sh
#   ./scripts/smoke_test.sh [write]
#   SMOKE_FIXTURE_FILE=/path/outside/volumes/fixture.tsv ./scripts/smoke_test.sh seed
#   SMOKE_FIXTURE_FILE=/path/outside/volumes/fixture.tsv ./scripts/smoke_test.sh verify
#
# Optional env:
#   COORDINATOR_ADDRS=localhost:9001,localhost:9002,localhost:9003
#   GATEWAY_ADDR=localhost:7000
#   N_NODES=2
#   NUM_SHARDS=8
#   RF=2
#   SMOKE_MODE=write|seed|verify   (the first argument takes precedence)
#   SMOKE_FIXTURE_FILE=<path>      required for seed and verify; keep it outside
#                                  every Docker volume under test
#   SMOKE_FIXTURE_KEYS=3           number of keys written by seed
#

BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/.."

GATEWAY_ADDR="${GATEWAY_ADDR:-localhost:7000}"
N_NODES="${N_NODES:-2}"
NUM_SHARDS="${NUM_SHARDS:-8}"
RF="${RF:-$N_NODES}"
SMOKE_MODE="${1:-${SMOKE_MODE:-write}}"
SMOKE_FIXTURE_FILE="${SMOKE_FIXTURE_FILE:-}"
SMOKE_FIXTURE_KEYS="${SMOKE_FIXTURE_KEYS:-3}"

PROTO_DIR="$BASE_DIR/kv.proto/src/main/proto"

b64() {
  printf "%s" "$1" | base64 | tr -d '\n'
}

gateway_put() {
  local request_id="$1" key_b64="$2" val_b64="$3"
  grpcurl -plaintext \
    -H "x-kvdb-development-identity: client/local/smoke-test" \
    -import-path "${PROTO_DIR}" \
    -proto kvgateway.proto \
    -d "{\"ctx\":{\"request_id\":\"${request_id}\"},\"key\":\"${key_b64}\",\"value\":\"${val_b64}\",\"options\":{}}" \
    "${GATEWAY_ADDR}" \
    kvdb.gateway.KvGateway/Put
}

gateway_get() {
  local request_id="$1" key_b64="$2"
  grpcurl -plaintext \
    -H "x-kvdb-development-identity: client/local/smoke-test" \
    -import-path "${PROTO_DIR}" \
    -proto kvgateway.proto \
    -d "{\"ctx\":{\"request_id\":\"${request_id}\"},\"key\":\"${key_b64}\",\"options\":{\"consistency\":\"STRONG\"}}" \
    "${GATEWAY_ADDR}" \
    kvdb.gateway.KvGateway/Get
}

# Extracts the numeric "version" from a Get response (proto3 JSON renders uint64
# as a string; a zero version is omitted).
response_version() {
  local version
  version="$(grep -Eo '"version"[[:space:]]*:[[:space:]]*"?[0-9]+' <<< "$1" | grep -Eo '[0-9]+$' | head -n 1 || true)"
  printf '%s' "${version:-0}"
}

run_write() {
  "$BASE_DIR/scripts/bootstrap_cluster.sh"

  echo "== Put/Get smoke test via gateway =="

  local key_plain="smoke-key10"
  local val_plain="hello10"
  local key_b64 val_b64 put_resp get_resp
  key_b64="$(b64 "${key_plain}")"
  val_b64="$(b64 "${val_plain}")"

  put_resp="$(gateway_put smoke-put-1 "${key_b64}" "${val_b64}")"
  echo "PutResponse: ${put_resp}"

  get_resp="$(gateway_get smoke-get-1 "${key_b64}")"
  echo "GetResponse: ${get_resp}"

  # Minimal assertion without jq: ensure base64 value appears in response JSON.
  if echo "${get_resp}" | grep -q "\"value\": *\"${val_b64}\""; then
    echo "✅ Smoke test passed (value round-tripped)"
    return 0
  fi

  echo "❌ Smoke test failed: expected value base64 ${val_b64} not found in GetResponse"
  return 1
}

require_fixture_file() {
  if [[ -z "$SMOKE_FIXTURE_FILE" ]]; then
    echo "SMOKE_FIXTURE_FILE is required for ${SMOKE_MODE} mode (keep it outside the volumes under test)" >&2
    return 1
  fi
}

run_seed() {
  require_fixture_file
  if [[ -e "$SMOKE_FIXTURE_FILE" ]]; then
    echo "Refusing to overwrite existing fixture file: ${SMOKE_FIXTURE_FILE}" >&2
    return 1
  fi
  if ! [[ "$SMOKE_FIXTURE_KEYS" =~ ^[1-9][0-9]*$ ]]; then
    echo "SMOKE_FIXTURE_KEYS must be a positive integer" >&2
    return 1
  fi

  echo "== Seeding ${SMOKE_FIXTURE_KEYS} persistence fixture key(s) =="
  local nonce i key_b64 val_b64 get_resp version tmp
  nonce="$(date -u +%Y%m%dT%H%M%SZ)-$$"
  mkdir -p "$(dirname "$SMOKE_FIXTURE_FILE")"
  tmp="${SMOKE_FIXTURE_FILE}.tmp.$$"
  : > "$tmp"
  for ((i=1; i<=SMOKE_FIXTURE_KEYS; i++)); do
    key_b64="$(b64 "persist-key-${i}")"
    val_b64="$(b64 "persist-value-${i}-${nonce}")"
    gateway_put "seed-put-${nonce}-${i}" "${key_b64}" "${val_b64}" >/dev/null
    get_resp="$(gateway_get "seed-get-${nonce}-${i}" "${key_b64}")"
    if ! grep -Eq "\"value\"[[:space:]]*:[[:space:]]*\"${val_b64}\"" <<< "${get_resp}"; then
      echo "❌ Seed failed: persist-key-${i} did not read back after Put: ${get_resp}" >&2
      rm -f "$tmp"
      return 1
    fi
    version="$(response_version "${get_resp}")"
    printf '%s\t%s\t%s\n' "${key_b64}" "${val_b64}" "${version}" >> "$tmp"
  done
  mv "$tmp" "$SMOKE_FIXTURE_FILE"
  echo "✅ Seeded ${SMOKE_FIXTURE_KEYS} key(s); expectations recorded in ${SMOKE_FIXTURE_FILE}"
}

run_verify() {
  require_fixture_file
  if [[ ! -s "$SMOKE_FIXTURE_FILE" ]]; then
    echo "Fixture file is missing or empty: ${SMOKE_FIXTURE_FILE}" >&2
    return 1
  fi

  echo "== Read-only persistence verification (no bootstrap, no Put) =="
  local checked=0 failed=0 key_b64 val_b64 want_version get_resp got_version
  while IFS=$'\t' read -r key_b64 val_b64 want_version; do
    [[ -n "$key_b64" ]] || continue
    checked=$((checked + 1))
    if ! get_resp="$(gateway_get "verify-get-$$-${checked}" "${key_b64}")"; then
      echo "❌ Get failed for fixture key #${checked}" >&2
      failed=$((failed + 1))
      continue
    fi
    if ! grep -Eq "\"value\"[[:space:]]*:[[:space:]]*\"${val_b64}\"" <<< "${get_resp}"; then
      echo "❌ Fixture key #${checked}: acknowledged value missing: ${get_resp}" >&2
      failed=$((failed + 1))
      continue
    fi
    got_version="$(response_version "${get_resp}")"
    if [[ "$got_version" != "$want_version" ]]; then
      echo "❌ Fixture key #${checked}: version ${got_version}, expected ${want_version}" >&2
      failed=$((failed + 1))
      continue
    fi
    echo "ok: fixture key #${checked} value and version ${got_version} preserved"
  done < "$SMOKE_FIXTURE_FILE"

  if (( checked == 0 || failed > 0 )); then
    echo "❌ Persistence verification failed (${failed} of ${checked} key(s) failed)" >&2
    return 1
  fi
  echo "✅ Persistence verified (${checked} key(s) match recorded value and version)"
}

case "$SMOKE_MODE" in
  write) run_write ;;
  seed) run_seed ;;
  verify) run_verify ;;
  *)
    echo "Unknown smoke mode '${SMOKE_MODE}' (expected write, seed, or verify)" >&2
    exit 2
    ;;
esac
