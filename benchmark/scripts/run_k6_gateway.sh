#!/usr/bin/env bash
set -euo pipefail

# Run k6 gateway benchmark and store results with timestamped filename.
#
# Usage:
#   ./benchmark/scripts/run_k6_gateway.sh
#
# Optional env overrides:
#   PROTO_DIR     (default: ../../kv.proto/src/main/proto)
#   PROTO_FILE    (default: kvgateway.proto)
#   RESULTS_DIR   (default: ./benchmark/results)
#
# Authentication (default is mTLS; there is no implicit plaintext fallback). The environment is
# passed through to k6; see benchmark/k6/gateway_bench.js:
#   KVDB_CLIENT_TLS_TRUST_BUNDLE, KVDB_CLIENT_TLS_CERT_CHAIN, KVDB_CLIENT_TLS_PRIVATE_KEY
#   KVDB_GRPC_SECURITY_MODE=development-plaintext KVDB_ENV=local   (local development only)
#   KVDB_CLIENT_TENANT_ID, KVDB_CLIENT_PRINCIPAL
# k6 resolves open() paths from the script directory, so relative TLS paths are made absolute
# against the current directory here. k6 cannot override the verified server name.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BENCH_DIR="${ROOT_DIR}/benchmark"
RESULTS_DIR="${RESULTS_DIR:-${BENCH_DIR}/results}"
SERVICE_DIR="${RESULTS_DIR}/gateway/k6"
mkdir -p "${SERVICE_DIR}"

PROTO_DIR="${PROTO_DIR:-../../kv.proto/src/main/proto}"
PROTO_FILE="${PROTO_FILE:-kvgateway.proto}"

for tls_var in KVDB_CLIENT_TLS_TRUST_BUNDLE KVDB_CLIENT_TLS_CERT_CHAIN KVDB_CLIENT_TLS_PRIVATE_KEY; do
  tls_path="${!tls_var:-}"
  if [[ -n "${tls_path}" && "${tls_path}" != /* ]]; then
    export "${tls_var}=${PWD}/${tls_path}"
  fi
done

TIMESTAMP="$(date +"%Y%m%d_%H%M%S")"
OUT_FILE="${SERVICE_DIR}/${TIMESTAMP}.log"

echo "== Running k6 gateway benchmark =="
echo "Proto dir : ${PROTO_DIR}"
echo "Proto file: ${PROTO_FILE}"
echo "Output    : ${OUT_FILE}"
echo

set -o pipefail

PROTO_DIR="${PROTO_DIR}" PROTO_FILE="${PROTO_FILE}" \
k6 run "${BENCH_DIR}/k6/gateway_bench.js" 2>&1 | tee "${OUT_FILE}"

echo
echo "== Benchmark completed =="
echo "Results written to ${OUT_FILE}"