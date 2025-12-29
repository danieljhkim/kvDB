#!/usr/bin/env bash
set -euo pipefail

# Admin HTTP benchmark runner (kv.admin).
#
# uses vegeta for HTTP load testing while keeping the requested output filename format:
#   benchmark/results/admin/vegeta/<datetime>.log
#
# Usage:
#   ./benchmark/scripts/run_vegeta_admin.sh
#
# Common overrides:
#   ADMIN_BASE_URL=http://localhost:8089 RATE=500 DURATION=30s ./benchmark/scripts/run_vegeta_admin.sh
#   BOOTSTRAP=true NUM_SHARDS=8 RF=2 N_NODES=2 ./benchmark/scripts/run_vegeta_admin.sh
#
# Requirements:
#   - vegeta installed: https://github.com/tsenart/vegeta

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

RESULTS_DIR="${RESULTS_DIR:-${BASE_DIR}/benchmark/results}"
SERVICE_DIR="${RESULTS_DIR}/admin/vegeta"
mkdir -p "${SERVICE_DIR}"

TIMESTAMP="$(date +"%Y%m%d_%H%M%S")"
OUT_FILE="${SERVICE_DIR}/${TIMESTAMP}.log"

ADMIN_BASE_URL="${ADMIN_BASE_URL:-http://localhost:8089}"
API_KEY="${API_KEY:-dev-secret}"

# Load parameters
RATE="${RATE:-300}"              # requests per second
DURATION="${DURATION:-30s}"      # vegeta duration (e.g. 30s, 1m)
CONNECTIONS="${CONNECTIONS:-50}" # max connections

# Optional bootstrap (mutating calls) before load
BOOTSTRAP="${BOOTSTRAP:-false}"
NUM_SHARDS="${NUM_SHARDS:-8}"
RF="${RF:-2}"
N_NODES="${N_NODES:-2}"
ZONE="${ZONE:-us-east-1a}"

need_cmd() {
  local cmd="$1"
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    echo "ERROR: ${cmd} is required but not installed." >&2
    exit 1
  fi
}

need_cmd vegeta
need_cmd curl

hdr() {
  printf "Content-Type: application/json\nX-Admin-Api-Key: %s\n" "${API_KEY}"
}

curl_json() {
  local method="$1"
  local url="$2"
  local data="${3:-}"
  if [[ -n "${data}" ]]; then
    curl -sS -X "${method}" "${url}" -H "Content-Type: application/json" -H "X-Admin-Api-Key: ${API_KEY}" -d "${data}"
  else
    curl -sS -X "${method}" "${url}" -H "X-Admin-Api-Key: ${API_KEY}"
  fi
}

bootstrap_cluster() {
  echo "== Bootstrapping cluster via kv.admin =="

  echo "-- Init shards (idempotent if your API returns 409 when already initialized)"
  curl_json POST "${ADMIN_BASE_URL}/admin/config/shard-init" "{\"num_shards\": ${NUM_SHARDS}, \"replication_factor\": ${RF}}" >/dev/null || true

  echo "-- Register nodes"
  for i in $(seq 1 "${N_NODES}"); do
    local port_var="NODE_${i}_PORT"
    local port="${!port_var:-$((8000 + i))}" # default 8001, 8002, ...
    local node_id="node-${i}"
    curl_json POST "${ADMIN_BASE_URL}/admin/nodes" "{\"node_id\":\"${node_id}\",\"address\":\"localhost:${port}\",\"zone\":\"${ZONE}\"}" >/dev/null || true
  done

  echo "-- Sanity: cluster summary"
  curl_json GET "${ADMIN_BASE_URL}/admin/cluster/summary" >/dev/null || true
  echo "== Bootstrap complete =="
}

{
  echo "Running Admin HTTP benchmark (kv.admin) against ${ADMIN_BASE_URL}"
  echo
  echo "Config:"
  echo "  RATE=${RATE} req/s"
  echo "  DURATION=${DURATION}"
  echo "  CONNECTIONS=${CONNECTIONS}"
  echo "  BOOTSTRAP=${BOOTSTRAP}"
  echo "  NUM_SHARDS=${NUM_SHARDS}"
  echo "  RF=${RF}"
  echo "  N_NODES=${N_NODES}"
  echo "  ZONE=${ZONE}"
  echo
} | tee "${OUT_FILE}"

if [[ "${BOOTSTRAP,,}" == "true" ]]; then
  bootstrap_cluster | tee -a "${OUT_FILE}"
  echo | tee -a "${OUT_FILE}"
fi

# Build vegeta targets (read-only endpoints by default).
TARGETS_FILE="$(mktemp)"
cleanup() { rm -f "${TARGETS_FILE}"; }
trap cleanup EXIT

cat > "${TARGETS_FILE}" <<EOF
GET ${ADMIN_BASE_URL}/admin/cluster/summary
GET ${ADMIN_BASE_URL}/admin/nodes
GET ${ADMIN_BASE_URL}/admin/shards
EOF

{
  echo "== vegeta attack =="
  echo "Targets:"
  cat "${TARGETS_FILE}"
  echo
} | tee -a "${OUT_FILE}"

# Run attack and write binary results to a temp file so we can render multiple reports.
BIN_RESULTS="$(mktemp)"
trap 'rm -f "${TARGETS_FILE}" "${BIN_RESULTS}"' EXIT

vegeta attack \
  -rate="${RATE}" \
  -duration="${DURATION}" \
  -connections="${CONNECTIONS}" \
  -header="X-Admin-Api-Key: ${API_KEY}" \
  -header="Content-Type: application/json" \
  -targets="${TARGETS_FILE}" \
  | tee "${BIN_RESULTS}" >/dev/null

{
  echo
  echo "== vegeta report (text) =="
} | tee -a "${OUT_FILE}"
vegeta report "${BIN_RESULTS}" | tee -a "${OUT_FILE}"

{
  echo
  echo "== vegeta report (histogram) =="
} | tee -a "${OUT_FILE}"
vegeta report -type='hist[0,1ms,2ms,5ms,10ms,25ms,50ms,100ms,250ms,500ms,1s]' "${BIN_RESULTS}" | tee -a "${OUT_FILE}"

{
  echo
  echo "Saved results to: ${OUT_FILE}"
} | tee -a "${OUT_FILE}"
