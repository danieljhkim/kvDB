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
# Authentication (default is mTLS; there is no implicit plaintext fallback):
#   KVDB_CLIENT_TLS_TRUST_BUNDLE=ca.pem KVDB_CLIENT_TLS_CERT_CHAIN=client.crt \
#     KVDB_CLIENT_TLS_PRIVATE_KEY=client.key ./benchmark/scripts/run_ghz_gateway.sh
#   KVDB_CLIENT_TLS_SERVER_NAME=<name> overrides the name verified against the gateway certificate.
#   KVDB_GRPC_SECURITY_MODE=development-plaintext KVDB_ENV=local ./benchmark/scripts/run_ghz_gateway.sh
# KVDB_CLIENT_TENANT_ID / KVDB_CLIENT_PRINCIPAL (development default: dev / ghz) are sent as the
# x-kvdb-development-identity header and as the request ctx. In mTLS the identity comes from the
# client certificate SAN, so ctx tenant/principal are only sent when configured and must match it.
#
# Notes:
#   - proto `bytes` fields must be base64 strings in JSON payloads.
#   - This script runs PUT (write) then GET (read) using the same key so GET should hit.
#

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

die() {
  echo "ERROR: $*" >&2
  exit 1
}

# Transport authentication. Validated before anything is written or ghz is invoked.
SECURITY_MODE="${KVDB_GRPC_SECURITY_MODE:-mtls}"
CLIENT_TENANT="${KVDB_CLIENT_TENANT_ID:-}"
CLIENT_PRINCIPAL="${KVDB_CLIENT_PRINCIPAL:-}"
IDENTITY_PART_RE='^[A-Za-z0-9._:@-]+$'
GHZ_AUTH_ARGS=()
CTX_IDENTITY_JSON=""

require_identity_part() {
  [[ "$2" =~ ${IDENTITY_PART_RE} ]] ||
    die "$1 must be non-empty and contain only letters, digits and . _ : @ - (got \"$2\")"
}

require_readable_file() {
  [[ -n "$2" ]] || die "$1 is required when KVDB_GRPC_SECURITY_MODE=mtls"
  [[ -r "$2" && -f "$2" ]] || die "$1 $2 is not a readable file"
}

case "${SECURITY_MODE}" in
  development-plaintext)
    case "$(printf '%s' "${KVDB_ENV:-}" | tr '[:upper:]' '[:lower:]')" in
      local | dev | development | test) ;;
      *) die "KVDB_GRPC_SECURITY_MODE=development-plaintext requires KVDB_ENV to be local, dev, development or test" ;;
    esac
    CLIENT_TENANT="${CLIENT_TENANT:-dev}"
    CLIENT_PRINCIPAL="${CLIENT_PRINCIPAL:-ghz}"
    require_identity_part KVDB_CLIENT_TENANT_ID "${CLIENT_TENANT}"
    require_identity_part KVDB_CLIENT_PRINCIPAL "${CLIENT_PRINCIPAL}"
    GHZ_AUTH_ARGS=(--insecure -m "{\"x-kvdb-development-identity\":\"client/${CLIENT_TENANT}/${CLIENT_PRINCIPAL}\"}")
    ;;
  mtls)
    require_readable_file KVDB_CLIENT_TLS_TRUST_BUNDLE "${KVDB_CLIENT_TLS_TRUST_BUNDLE:-}"
    require_readable_file KVDB_CLIENT_TLS_CERT_CHAIN "${KVDB_CLIENT_TLS_CERT_CHAIN:-}"
    require_readable_file KVDB_CLIENT_TLS_PRIVATE_KEY "${KVDB_CLIENT_TLS_PRIVATE_KEY:-}"
    GHZ_AUTH_ARGS=(
      --cacert "${KVDB_CLIENT_TLS_TRUST_BUNDLE}"
      --cert "${KVDB_CLIENT_TLS_CERT_CHAIN}"
      --key "${KVDB_CLIENT_TLS_PRIVATE_KEY}"
    )
    if [[ -n "${KVDB_CLIENT_TLS_SERVER_NAME:-}" ]]; then
      GHZ_AUTH_ARGS+=(--cname "${KVDB_CLIENT_TLS_SERVER_NAME}")
    fi
    [[ -z "${CLIENT_TENANT}" ]] || require_identity_part KVDB_CLIENT_TENANT_ID "${CLIENT_TENANT}"
    [[ -z "${CLIENT_PRINCIPAL}" ]] || require_identity_part KVDB_CLIENT_PRINCIPAL "${CLIENT_PRINCIPAL}"
    ;;
  *)
    die "KVDB_GRPC_SECURITY_MODE must be mtls or development-plaintext (got \"${SECURITY_MODE}\")"
    ;;
esac

# Request ctx claims must agree with the transport identity (the server rejects a conflict).
if [[ -n "${CLIENT_TENANT}" ]]; then
  CTX_IDENTITY_JSON+=",\"tenant_id\":\"${CLIENT_TENANT}\""
fi
if [[ -n "${CLIENT_PRINCIPAL}" ]]; then
  CTX_IDENTITY_JSON+=",\"principal\":\"${CLIENT_PRINCIPAL}\""
fi

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
{"ctx":{"request_id":"${REQID_PUT}"${CTX_IDENTITY_JSON}},"key":"${KEY_B64}","value":"${VAL_B64}","options":{}}
JSON
)"

GET_PAYLOAD="$(cat <<JSON
{"ctx":{"request_id":"${REQID_GET}"${CTX_IDENTITY_JSON}},"key":"${KEY_B64}","options":{"consistency":"${CONSISTENCY}"},"head_only":false}
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
  echo "  KVDB_GRPC_SECURITY_MODE=${SECURITY_MODE}"
  if [[ "${SECURITY_MODE}" == "mtls" ]]; then
    echo "  KVDB_CLIENT_TLS_TRUST_BUNDLE=${KVDB_CLIENT_TLS_TRUST_BUNDLE}"
    echo "  KVDB_CLIENT_TLS_CERT_CHAIN=${KVDB_CLIENT_TLS_CERT_CHAIN}"
    echo "  KVDB_CLIENT_TLS_PRIVATE_KEY=${KVDB_CLIENT_TLS_PRIVATE_KEY}"
  fi
  echo "  KVDB_CLIENT_TENANT_ID=${CLIENT_TENANT:-<unset>}"
  echo "  KVDB_CLIENT_PRINCIPAL=${CLIENT_PRINCIPAL:-<unset>}"
  echo
  echo "=============================="
  echo "PUT benchmark (kvdb.gateway.KvGateway/Put)"
  echo "=============================="
  echo
} | tee "${OUT_FILE}"

# PUT run
ghz \
  "${GHZ_AUTH_ARGS[@]}" \
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
  "${GHZ_AUTH_ARGS[@]}" \
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