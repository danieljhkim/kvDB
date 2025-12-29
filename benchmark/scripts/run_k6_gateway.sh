

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

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BENCH_DIR="${ROOT_DIR}/benchmark"
RESULTS_DIR="${RESULTS_DIR:-${BENCH_DIR}/results}"
SERVICE_DIR="${RESULTS_DIR}/gateway/k6"
mkdir -p "${SERVICE_DIR}"

PROTO_DIR="${PROTO_DIR:-../../kv.proto/src/main/proto}"
PROTO_FILE="${PROTO_FILE:-kvgateway.proto}"

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