#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

check_plan() {
    local label="$1"
    shift

    local output
    if ! output=$("$@" 2>&1); then
        printf '%s\n' "$output" >&2
        printf '%s dry-run failed\n' "$label" >&2
        return 1
    fi

    if grep -Fq 'No rule to make target' <<<"$output"; then
        printf '%s\n' "$output" >&2
        printf '%s dry-run reported a missing target\n' "$label" >&2
        return 1
    fi

    if ! grep -Eq '^[[:space:]]*\./scripts/run_cluster\.sh[[:space:]]*$' <<<"$output"; then
        printf '%s\n' "$output" >&2
        printf '%s dry-run did not include the cluster-start recipe\n' "$label" >&2
        return 1
    fi

    printf '%s dry-run resolves the cluster launcher\n' "$label"
}

check_plan 'default target' make -n
check_plan 'run-cluster target' make -n run-cluster
