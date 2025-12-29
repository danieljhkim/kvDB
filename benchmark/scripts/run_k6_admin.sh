

#!/usr/bin/env bash
set -euo pipefail

# Run k6 admin benchmark and store results with timestamped filename.
#
# Usage:
#   ./benchmark/scripts/run_k6_admin.sh
#
# Optional env overrides:
#   ADMIN_BASE_URL (default: http://localhost:8089)
#   RESULTS_DIR    (default: ./benchmark/results)

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BENCH_DIR="${ROOT_DIR}/benchmark"
RESULTS_DIR="${RESULTS_DIR:-${BENCH_DIR}/results}"
SERVICE_DIR="${RESULTS_DIR}/admin/k6"
mkdir -p "${SERVICE_DIR}"

ADMIN_BASE_URL="${ADMIN_BASE_URL:-http://localhost:8089}"

TIMESTAMP="$(date +"%Y%m%d_%H%M%S")"
OUT_FILE="${SERVICE_DIR}/${TIMESTAMP}.log"

echo "== Running k6 admin benchmark =="
echo "Admin base URL: ${ADMIN_BASE_URL}"
echo "Output        : ${OUT_FILE}"
echo

set -o pipefail

ADMIN_BASE_URL="${ADMIN_BASE_URL}" \
k6 run "${BENCH_DIR}/k6/admin_bench.js" 2>&1 | tee "${OUT_FILE}"

echo
echo "== Benchmark completed =="
echo "Results written to ${OUT_FILE}"