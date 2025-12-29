

#!/usr/bin/env bash
set -euo pipefail

# ghz benchmark runner for kvdb.gateway.KvGateway (Put + Get)
#
# Writes a single combined report file:
#   benchmark/results/gateway/ghz/<datetime>.log
#
# Requirements:
#   - ghz installed (https://ghz.sh/)
#
# Usage:
#   ./benchmark/scripts/run_ghz_gateway.sh
#
# Common overrides:
#   TARGET=localhost:7000 TOTAL=5000 CONCURRENCY=50 ./benchmark/scripts/run_ghz_gateway.sh
#   KEY_PLAIN=mykey VALUE_PLAIN=myval VALUE_BYTES=1024 ./benchmark/scripts/run_ghz_gateway.sh
#   CONSISTENCY=STRONG ./benchmark/scripts/run_ghz_gateway.sh
#
# Notes:
#   - proto `bytes` fields must be base64 strings in JSON payloads.
#   - This script runs PUT (write) then GET (read) using the same key so GET should hit.
#

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

RESULTS_DIR="${RESULTS_DIR:-${BASE_DIR}/benchmark/results}"
SERVICE_DIR="${RESULTS_DIR}/gateway/ghz"
mkdir -p "${SERVICE_DIR}"

TIMESTAMP="$(date +"%Y%m%d_%H%M%S")"
OUT_FILE="${SERVICE_DIR}/${TIMESTAMP}.log"

TARGET="${TARGET:-localhost:7000}"

# Proto location defaults to repo layout
PROTO_DIR="${PROTO_DIR:-${BASE_DIR}/kv.proto/src/main/proto}"
PROTO_FILE="${PROTO_FILE:-kvgateway.proto}"

# Load parameters
TOTAL="${TOTAL:-20000}"
CONCURRENCY="${CONCURRENCY:-50}"

# Request content
KEY_PLAIN="${KEY_PLAIN:-bench-key}"
VALUE_PLAIN="${VALUE_PLAIN:-bench-val}"
VALUE_BYTES="${VALUE_BYTES:-0}" # if >0, generate a value payload of this size

# Read options
CONSISTENCY="${CONSISTENCY:-STRONG}" # STRONG | EVENTUAL

need_cmd() {
  local cmd="$1"
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    echo "ERROR: ${cmd} is required but not installed." >&2
    exit 1
  fi
}

b64() {
  # macOS base64 adds newlines by default; strip them.
  printf "%s" "$1" | base64 | tr -d '\n'
}

gen_bytes_b64() {
  local n="$1"
  if [[ "${n}" -le 0 ]]; then
    b64 "${VALUE_PLAIN}"
    return
  fi
  # Generate deterministic ASCII payload of size n.
  # Use python to avoid platform-specific head/tr behavior with null bytes.
  python3 - <<PY
import base64
n=${n}
payload=(b"x" * n)
print(base64.b64encode(payload).decode("ascii"), end="")
PY
}

need_cmd ghz
need_cmd base64
need_cmd python3

KEY_B64="$(b64 "${KEY_PLAIN}")"
VAL_B64="$(gen_bytes_b64 "${VALUE_BYTES}")"

# Unique-ish request IDs per run
REQID_PUT="ghz-put-${TIMESTAMP}"
REQID_GET="ghz-get-${TIMESTAMP}"

PUT_PAYLOAD="$(cat <<JSON
{"ctx":{"request_id":"${REQID_PUT}","tenant_id":"dev","principal":"ghz"},"key":"${KEY_B64}","value":"${VAL_B64}","options":{}}
JSON
)"

GET_PAYLOAD="$(cat <<JSON
{"ctx":{"request_id":"${REQID_GET}","tenant_id":"dev","principal":"ghz"},"key":"${KEY_B64}","options":{"consistency":"${CONSISTENCY}"},"head_only":false}
JSON
)"

{
  echo "Running ghz Gateway benchmarks against ${TARGET}"
  echo
  echo "Config:"
  echo "  PROTO_DIR=${PROTO_DIR}"
  echo "  PROTO_FILE=${PROTO_FILE}"
  echo "  TOTAL=${TOTAL}"
  echo "  CONCURRENCY=${CONCURRENCY}"
  echo "  KEY_PLAIN=${KEY_PLAIN}"
  if [[ "${VALUE_BYTES}" -gt 0 ]]; then
    echo "  VALUE_BYTES=${VALUE_BYTES}"
  else
    echo "  VALUE_PLAIN=${VALUE_PLAIN}"
  fi
  echo "  CONSISTENCY=${CONSISTENCY}"
  echo
  echo "=============================="
  echo "PUT benchmark (kvdb.gateway.KvGateway/Put)"
  echo "=============================="
  echo
} | tee "${OUT_FILE}"

# PUT run
ghz \
  --insecure \
  --proto "${PROTO_DIR}/${PROTO_FILE}" \
  --call kvdb.gateway.KvGateway.Put \
  --total "${TOTAL}" \
  --concurrency "${CONCURRENCY}" \
  -d "${PUT_PAYLOAD}" \
  "${TARGET}" 2>&1 | tee -a "${OUT_FILE}"

{
  echo
  echo "=============================="
  echo "GET benchmark (kvdb.gateway.KvGateway/Get)"
  echo "=============================="
  echo
} | tee -a "${OUT_FILE}"

# GET run
ghz \
  --insecure \
  --proto "${PROTO_DIR}/${PROTO_FILE}" \
  --call kvdb.gateway.KvGateway.Get \
  --total "${TOTAL}" \
  --concurrency "${CONCURRENCY}" \
  -d "${GET_PAYLOAD}" \
  "${TARGET}" 2>&1 | tee -a "${OUT_FILE}"

{
  echo
  echo "Saved ghz results to: ${OUT_FILE}"
} | tee -a "${OUT_FILE}"