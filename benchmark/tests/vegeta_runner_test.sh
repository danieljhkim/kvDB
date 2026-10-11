#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
RUNNER="${REPO_ROOT}/benchmark/scripts/run_vegeta_admin.sh"
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/vegeta-runner-test.XXXXXX")"
trap 'rm -rf "${WORK_DIR}"' EXIT

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

assert_contains() {
  local file="$1"
  local expected="$2"
  if ! grep -F "${expected}" "${file}" >/dev/null; then
    fail "expected '${expected}' in ${file}"
  fi
}

assert_count() {
  local file="$1"
  local pattern="$2"
  local expected="$3"
  local actual
  actual="$(grep -c "${pattern}" "${file}" || true)"
  [[ "${actual}" == "${expected}" ]] || fail "expected ${expected} '${pattern}' lines in ${file}, got ${actual}"
}

if [[ ! -x "${RUNNER}" ]]; then
  fail "runner is missing or not executable: ${RUNNER}"
fi

if [[ "$#" -eq 0 ]]; then
  if [[ -x /bin/bash ]]; then
    set -- /bin/bash
  else
    set -- bash
  fi
fi

base_path="${PATH}"
shell_index=0
for requested_bash in "$@"; do
  shell_index=$((shell_index + 1))
  if [[ "${requested_bash}" == /* ]]; then
    bash_bin="${requested_bash}"
  else
    bash_bin="$(command -v "${requested_bash}")" || fail "Bash interpreter not found: ${requested_bash}"
  fi
  [[ -x "${bash_bin}" ]] || fail "Bash interpreter is not executable: ${bash_bin}"

  shell_dir="${WORK_DIR}/shell-${shell_index}"
  mock_bin="${shell_dir}/bin"
  mkdir -p "${mock_bin}"
  ln -s "${bash_bin}" "${mock_bin}/bash"

  cat > "${mock_bin}/curl" <<'MOCK_CURL'
#!/bin/sh
printf 'curl %s\n' "$*" >> "${MOCK_LOG}"
MOCK_CURL
  cat > "${mock_bin}/vegeta" <<'MOCK_VEGETA'
#!/bin/sh
printf 'vegeta %s\n' "$*" >> "${MOCK_LOG}"
case "$1" in
  attack) printf 'mock vegeta result\n' ;;
  report) printf 'mock vegeta report\n' ;;
  *) echo "unexpected vegeta command: $1" >&2; exit 2 ;;
esac
MOCK_VEGETA
  chmod +x "${mock_bin}/curl" "${mock_bin}/vegeta"

  case_index=0
  for bootstrap_value in __unset__ false False true TRUE TrUe; do
    case_index=$((case_index + 1))
    case_dir="${shell_dir}/case-${case_index}"
    result_dir="${case_dir}/results"
    mkdir -p "${result_dir}"
    mock_log="${case_dir}/mock.log"
    stdout_log="${case_dir}/stdout.log"
    stderr_log="${case_dir}/stderr.log"

    (
      unset BOOTSTRAP RATE DURATION CONNECTIONS NUM_SHARDS RF ZONE NODE_1_PORT NODE_2_PORT
      if [[ "${bootstrap_value}" != "__unset__" ]]; then
        BOOTSTRAP="${bootstrap_value}"
        export BOOTSTRAP
      fi
      export API_KEY="fixture-admin-key"
      export ADMIN_BASE_URL="http://127.0.0.1:1"
      export NUM_SHARDS=8
      export RF=2
      export N_NODES=2
      export ZONE="test-zone"
      export NODE_1_PORT=8101
      export NODE_2_PORT=8102
      export RESULTS_DIR="${result_dir}"
      export MOCK_LOG="${mock_log}"
      PATH="${mock_bin}:${base_path}" "${RUNNER}"
    ) >"${stdout_log}" 2>"${stderr_log}" || {
      cat "${stdout_log}" "${stderr_log}" >&2
      fail "runner failed with ${bash_bin} and BOOTSTRAP=${bootstrap_value}"
    }

    assert_count "${mock_log}" '^vegeta attack ' 1
    assert_contains "${mock_log}" 'vegeta attack -rate=300 -duration=30s -connections=50'
    assert_count "${mock_log}" '^vegeta report ' 2
    if [[ "${bootstrap_value}" == "true" || "${bootstrap_value}" == "TRUE" || "${bootstrap_value}" == "TrUe" ]]; then
      assert_count "${mock_log}" '^curl ' 4
      assert_contains "${mock_log}" 'shard-init'
      assert_contains "${mock_log}" 'node-1'
      assert_contains "${mock_log}" 'node-2'
      assert_contains "${mock_log}" 'cluster/summary'
    else
      assert_count "${mock_log}" '^curl ' 0
    fi

    printf 'PASS: bash=%s BOOTSTRAP=%s\n' "${bash_bin}" "${bootstrap_value}"
  done
done
