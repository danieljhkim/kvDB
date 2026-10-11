#!/usr/bin/env bash

# Shared coordinator discovery and gRPC transport helpers. This file is sourced
# by cluster scripts and intentionally does not change shell options.
#
# Transport follows the server policy in SECURITY.md. KVDB_GRPC_SECURITY_MODE
# defaults to mtls; development-plaintext must be selected explicitly and is
# refused unless KVDB_ENV is local, dev, development or test. Any other mode, or
# a missing credential file, fails before a connection is attempted.
#
# mTLS credentials (each defaults to a file under KVDB_OPERATOR_TLS_DIR):
#   operator calls to coordinators (certificate URI SAN spiffe://kvdb/admin/<principal>)
#     KVDB_OPERATOR_TLS_TRUST_BUNDLE  internal-ca-bundle.pem
#     KVDB_OPERATOR_TLS_CERT_CHAIN    operator/tls.crt
#     KVDB_OPERATOR_TLS_PRIVATE_KEY   operator/tls.key
#     KVDB_OPERATOR_TLS_SERVER_NAME   optional name verified instead of the dialed host
#   client calls to the gateway (certificate URI SAN spiffe://kvdb/client/<tenant>/<principal>)
#     KVDB_CLIENT_TLS_TRUST_BUNDLE    gateway-ca-bundle.pem
#     KVDB_CLIENT_TLS_CERT_CHAIN      client/tls.crt
#     KVDB_CLIENT_TLS_PRIVATE_KEY     client/tls.key
#     KVDB_CLIENT_TLS_SERVER_NAME     optional name verified instead of the dialed host

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

# Trims and lowercases a setting, matching how servers read KVDB_GRPC_SECURITY_MODE and KVDB_ENV.
normalized_setting() {
  printf '%s' "$1" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' | tr '[:upper:]' '[:lower:]'
}

# Sets GRPC_TRANSPORT_ARGS to the grpcurl transport flags for <scope>, which is
# "operator" (coordinator admin RPCs) or "client" (gateway data API).
# <development-identity> is sent only in development-plaintext mode.
grpc_transport_args() {
  local scope="$1"
  local development_identity="$2"
  local mode deployment prefix trust cert key server_name name path
  local credential_dir="${KVDB_OPERATOR_TLS_DIR:-}"
  GRPC_TRANSPORT_ARGS=()

  mode="$(normalized_setting "${KVDB_GRPC_SECURITY_MODE:-mtls}")"
  case "$mode" in
    development-plaintext)
      deployment="$(normalized_setting "${KVDB_ENV:-}")"
      case "$deployment" in
        local|dev|development|test) ;;
        *)
          echo "KVDB_GRPC_SECURITY_MODE=development-plaintext requires KVDB_ENV=local, dev, development, or test" >&2
          return 1
          ;;
      esac
      GRPC_TRANSPORT_ARGS=(-plaintext -H "x-kvdb-development-identity: ${development_identity}")
      return 0
      ;;
    mtls) ;;
    *)
      echo "KVDB_GRPC_SECURITY_MODE must be mtls or development-plaintext" >&2
      return 1
      ;;
  esac

  case "$scope" in
    operator)
      prefix=KVDB_OPERATOR_TLS
      trust="${KVDB_OPERATOR_TLS_TRUST_BUNDLE:-${credential_dir:+$credential_dir/internal-ca-bundle.pem}}"
      cert="${KVDB_OPERATOR_TLS_CERT_CHAIN:-${credential_dir:+$credential_dir/operator/tls.crt}}"
      key="${KVDB_OPERATOR_TLS_PRIVATE_KEY:-${credential_dir:+$credential_dir/operator/tls.key}}"
      server_name="${KVDB_OPERATOR_TLS_SERVER_NAME:-}"
      ;;
    client)
      prefix=KVDB_CLIENT_TLS
      trust="${KVDB_CLIENT_TLS_TRUST_BUNDLE:-${credential_dir:+$credential_dir/gateway-ca-bundle.pem}}"
      cert="${KVDB_CLIENT_TLS_CERT_CHAIN:-${credential_dir:+$credential_dir/client/tls.crt}}"
      key="${KVDB_CLIENT_TLS_PRIVATE_KEY:-${credential_dir:+$credential_dir/client/tls.key}}"
      server_name="${KVDB_CLIENT_TLS_SERVER_NAME:-}"
      ;;
    *)
      echo "Unknown gRPC credential scope: ${scope}" >&2
      return 1
      ;;
  esac

  for name in TRUST_BUNDLE CERT_CHAIN PRIVATE_KEY; do
    case "$name" in
      TRUST_BUNDLE) path="$trust" ;;
      CERT_CHAIN) path="$cert" ;;
      PRIVATE_KEY) path="$key" ;;
    esac
    if [[ -z "$path" ]]; then
      echo "mTLS ${scope} credentials are not configured: set ${prefix}_${name} or KVDB_OPERATOR_TLS_DIR" >&2
      return 1
    fi
    if [[ ! -f "$path" || ! -r "$path" ]]; then
      echo "${prefix}_${name} must name a readable file: ${path}" >&2
      return 1
    fi
  done

  GRPC_TRANSPORT_ARGS=(-cacert "$trust" -cert "$cert" -key "$key")
  if [[ -n "$server_name" ]]; then
    GRPC_TRANSPORT_ARGS+=(-authority "$server_name")
  fi
}

coordinator_grpcurl() {
  local address="$1"
  local method="$2"
  local data="${3:-}"
  if [[ -z "$data" ]]; then
    data='{}'
  fi

  grpc_transport_args operator "admin/bootstrap-local" || return 1
  grpcurl "${GRPC_TRANSPORT_ARGS[@]}" -max-time 3 \
    -import-path "${PROTO_DIR}" \
    -proto coordinator.proto \
    -d "$data" \
    "$address" \
    "$method"
}

discover_coordinator_leader() {
  local timeout_seconds="${1:-${LEADER_DISCOVERY_TIMEOUT_SECONDS:-60}}"
  local deadline=$((SECONDS + timeout_seconds))
  local address configured response last_error=""
  local -a addresses
  configured="${COORDINATOR_ADDRS:-${COORDINATOR_ADDR:-$KVDB_COORDINATOR_ADDRS_DEFAULT}}"
  IFS=',' read -r -a addresses <<< "$configured"

  if (( ${#addresses[@]} == 0 )); then
    echo "No coordinator addresses configured" >&2
    return 1
  fi
  # A transport misconfiguration is permanent; report it instead of polling until the deadline.
  grpc_transport_args operator "admin/bootstrap-local" || return 1

  while (( SECONDS < deadline )); do
    for address in "${addresses[@]}"; do
      address="${address//[[:space:]]/}"
      [[ -n "$address" ]] || continue
      if ! response="$(coordinator_grpcurl \
        "$address" \
        kvdb.coordinator.Coordinator/GetCoordinatorLeader \
        '{}' 2>&1)"; then
        last_error="${address}: ${response}"
      elif grep -Eq '"isLeader"[[:space:]]*:[[:space:]]*true' <<< "$response"; then
        printf '%s\n' "$address"
        return 0
      fi
    done
    sleep 1
  done

  echo "No elected coordinator leader found within ${timeout_seconds}s (seeds: ${addresses[*]})" >&2
  if [[ -n "$last_error" ]]; then
    echo "Last coordinator error: ${last_error}" >&2
  fi
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
