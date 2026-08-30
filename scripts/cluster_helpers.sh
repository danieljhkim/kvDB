#!/usr/bin/env bash

# Shared coordinator discovery and authenticated gRPC helpers. This file is
# sourced by cluster scripts and intentionally does not change shell options.

KVDB_COORDINATOR_ADDRS_DEFAULT="localhost:9001,localhost:9002,localhost:9003"

require_grpcurl() {
  if ! command -v grpcurl >/dev/null 2>&1; then
    echo "grpcurl is required but not installed." >&2
    echo "Install via Homebrew: brew install grpcurl" >&2
    return 1
  fi
}

coordinator_addresses() {
  local configured="${COORDINATOR_ADDRS:-${COORDINATOR_ADDR:-$KVDB_COORDINATOR_ADDRS_DEFAULT}}"
  local address
  local -a addresses
  IFS=',' read -r -a addresses <<< "$configured"
  for address in "${addresses[@]}"; do
    address="${address//[[:space:]]/}"
    if [[ -n "$address" ]]; then
      printf '%s\n' "$address"
    fi
  done
}

coordinator_grpcurl() {
  local address="$1"
  local method="$2"
  local data="${3:-}"
  if [[ -z "$data" ]]; then
    data='{}'
  fi

  grpcurl -plaintext -max-time 3 \
    -H "x-kvdb-internal-token: ${KVDB_INTERNAL_GRPC_TOKEN}" \
    -import-path "${PROTO_DIR}" \
    -proto coordinator.proto \
    -d "$data" \
    "$address" \
    "$method"
}

discover_coordinator_leader() {
  local timeout_seconds="${1:-${LEADER_DISCOVERY_TIMEOUT_SECONDS:-60}}"
  local deadline=$((SECONDS + timeout_seconds))
  local address configured response
  local -a addresses
  configured="${COORDINATOR_ADDRS:-${COORDINATOR_ADDR:-$KVDB_COORDINATOR_ADDRS_DEFAULT}}"
  IFS=',' read -r -a addresses <<< "$configured"

  if (( ${#addresses[@]} == 0 )); then
    echo "No coordinator addresses configured" >&2
    return 1
  fi

  while (( SECONDS < deadline )); do
    for address in "${addresses[@]}"; do
      address="${address//[[:space:]]/}"
      [[ -n "$address" ]] || continue
      if response="$(coordinator_grpcurl \
        "$address" \
        kvdb.coordinator.Coordinator/GetCoordinatorLeader \
        '{}' 2>/dev/null)" && grep -Eq '"isLeader"[[:space:]]*:[[:space:]]*true' <<< "$response"; then
        printf '%s\n' "$address"
        return 0
      fi
    done
    sleep 1
  done

  echo "No elected coordinator leader found within ${timeout_seconds}s (seeds: ${addresses[*]})" >&2
  return 1
}

coordinator_call() {
  local method="$1"
  local data="${2:-}"
  if [[ -z "$data" ]]; then
    data='{}'
  fi
  local timeout_seconds="${3:-${LEADER_DISCOVERY_TIMEOUT_SECONDS:-60}}"
  local deadline=$((SECONDS + timeout_seconds))
  local leader response

  while (( SECONDS < deadline )); do
    leader="$(discover_coordinator_leader "$((deadline - SECONDS))")" || return 1
    if response="$(coordinator_grpcurl "$leader" "$method" "$data")"; then
      printf '%s\n' "$response"
      return 0
    fi
    sleep 1
  done

  echo "Coordinator call did not succeed before timeout: ${method}" >&2
  return 1
}
