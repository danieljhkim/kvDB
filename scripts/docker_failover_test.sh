#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/.."
PROTO_DIR="$BASE_DIR/kv.proto/src/main/proto"
source "$BASE_DIR/scripts/cluster_helpers.sh"

export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-kvdb-failover-${GITHUB_RUN_ID:-$$}}"
export KVDB_INTERNAL_GRPC_TOKEN="${KVDB_INTERNAL_GRPC_TOKEN:-docker-failover-internal-token}"
export KVDB_ADMIN_SECURITY_API_KEY="${KVDB_ADMIN_SECURITY_API_KEY:-docker-failover-admin-key}"
export COORDINATOR_ADDRS="${COORDINATOR_ADDRS:-$KVDB_COORDINATOR_ADDRS_DEFAULT}"
export LEADER_DISCOVERY_TIMEOUT_SECONDS="${LEADER_DISCOVERY_TIMEOUT_SECONDS:-90}"
export N_NODES="${N_NODES:-2}"
export STORAGE_NODE_ADDRS="${STORAGE_NODE_ADDRS:-node1:8001,node2:8002}"
export NUM_SHARDS="${NUM_SHARDS:-8}"
export RF="${RF:-2}"

GATEWAY_ADDR="${GATEWAY_ADDR:-localhost:7000}"
COMPOSE=(docker compose --project-directory "$BASE_DIR" -f "$BASE_DIR/docker-compose.yml")
CLEANED_UP=false

cleanup() {
  local status=$?
  if [[ "$CLEANED_UP" == "true" ]]; then
    return "$status"
  fi
  CLEANED_UP=true

  if (( status != 0 )); then
    echo "Failover test failed; container state follows" >&2
    "${COMPOSE[@]}" ps --all >&2 || true
    "${COMPOSE[@]}" logs --no-color --tail=200 >&2 || true
  fi

  "${COMPOSE[@]}" down --volumes --remove-orphans --timeout 30 >/dev/null 2>&1 || status=1
  if [[ -n "$(docker volume ls -q --filter "label=com.docker.compose.project=${COMPOSE_PROJECT_NAME}")" ]]; then
    echo "Compose volumes remain after wipe for project ${COMPOSE_PROJECT_NAME}" >&2
    status=1
  fi
  if [[ -n "$("${COMPOSE[@]}" ps --all --quiet 2>/dev/null)" ]]; then
    echo "Compose containers remain after shutdown for project ${COMPOSE_PROJECT_NAME}" >&2
    status=1
  fi

  trap - EXIT
  exit "$status"
}
trap cleanup EXIT

require_grpcurl
command -v docker >/dev/null 2>&1 || {
  echo "docker is required" >&2
  exit 1
}

wait_healthy() {
  local service="$1"
  local timeout_seconds="${2:-120}"
  local deadline=$((SECONDS + timeout_seconds))
  local container_id status

  while (( SECONDS < deadline )); do
    container_id="$("${COMPOSE[@]}" ps --quiet "$service")"
    if [[ -n "$container_id" ]]; then
      status="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container_id")"
      if [[ "$status" == "healthy" || "$status" == "running" ]]; then
        return 0
      fi
    fi
    sleep 1
  done

  echo "Service did not become healthy: ${service}" >&2
  return 1
}

coordinator_service_for_address() {
  case "${1##*:}" in
    9001) printf '%s\n' coordinator1 ;;
    9002) printf '%s\n' coordinator2 ;;
    9003) printf '%s\n' coordinator3 ;;
    *) echo "Unknown coordinator endpoint: $1" >&2; return 1 ;;
  esac
}

coordinator_id_for_address() {
  case "${1##*:}" in
    9001) printf '%s\n' coordinator-1 ;;
    9002) printf '%s\n' coordinator-2 ;;
    9003) printf '%s\n' coordinator-3 ;;
    *) echo "Unknown coordinator endpoint: $1" >&2; return 1 ;;
  esac
}

assert_control_plane() {
  local nodes shards
  nodes="$(coordinator_call kvdb.coordinator.Coordinator/ListNodes '{}')"
  grep -q 'node-1' <<< "$nodes"
  grep -q 'node-2' <<< "$nodes"
  shards="$(coordinator_call kvdb.coordinator.Coordinator/GetShardMap '{"if_version_gt":0}')"
  grep -q 'shard-0' <<< "$shards"
}

gateway_put() {
  local key="$1"
  local value="$2"
  local request_id="$3"
  local key_b64 value_b64
  key_b64="$(printf '%s' "$key" | base64 | tr -d '\n')"
  value_b64="$(printf '%s' "$value" | base64 | tr -d '\n')"
  grpcurl -plaintext -max-time 10 \
    -import-path "$PROTO_DIR" \
    -proto kvgateway.proto \
    -d "{\"ctx\":{\"request_id\":\"${request_id}\"},\"key\":\"${key_b64}\",\"value\":\"${value_b64}\",\"options\":{}}" \
    "$GATEWAY_ADDR" \
    kvdb.gateway.KvGateway/Put >/dev/null
}

assert_gateway_value() {
  local key="$1"
  local value="$2"
  local request_id="$3"
  local key_b64 value_b64 response
  key_b64="$(printf '%s' "$key" | base64 | tr -d '\n')"
  value_b64="$(printf '%s' "$value" | base64 | tr -d '\n')"
  response="$(grpcurl -plaintext -max-time 10 \
    -import-path "$PROTO_DIR" \
    -proto kvgateway.proto \
    -d "{\"ctx\":{\"request_id\":\"${request_id}\"},\"key\":\"${key_b64}\",\"options\":{\"consistency\":\"STRONG\"}}" \
    "$GATEWAY_ADDR" \
    kvdb.gateway.KvGateway/Get)"
  if ! grep -Eq "\"value\"[[:space:]]*:[[:space:]]*\"${value_b64}\"" <<< "$response"; then
    echo "Expected value was not returned for key ${key}: ${response}" >&2
    return 1
  fi
}

assert_data_plane() {
  assert_gateway_value durable-before-failover value-before verify-before
  assert_gateway_value durable-after-failover value-after verify-after
}

echo "Validating Compose model"
"${COMPOSE[@]}" config --quiet

echo "Starting clean three-coordinator deployment"
"${COMPOSE[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
"${COMPOSE[@]}" up --build --detach

echo "Bootstrapping twice to prove idempotence"
"$BASE_DIR/scripts/bootstrap_cluster.sh"
"$BASE_DIR/scripts/bootstrap_cluster.sh"
"${COMPOSE[@]}" up --detach --wait --wait-timeout 180
assert_control_plane

echo "Writing acknowledged data before coordinator failover"
gateway_put durable-before-failover value-before put-before
assert_gateway_value durable-before-failover value-before get-before

old_leader="$(discover_coordinator_leader)"
old_leader_service="$(coordinator_service_for_address "$old_leader")"
echo "Killing elected leader ${old_leader_service} (${old_leader})"
"${COMPOSE[@]}" kill "$old_leader_service"

new_leader="$(discover_coordinator_leader)"
if [[ "$new_leader" == "$old_leader" ]]; then
  echo "Leader did not change after ${old_leader_service} was killed" >&2
  exit 1
fi
echo "New leader elected at ${new_leader}"
assert_control_plane
assert_gateway_value durable-before-failover value-before get-during-failover
gateway_put durable-after-failover value-after put-during-failover
assert_data_plane

echo "Restarting killed coordinator and verifying cluster convergence"
"${COMPOSE[@]}" start "$old_leader_service"
wait_healthy "$old_leader_service"
new_leader_id="$(coordinator_id_for_address "$new_leader")"
restarted_response="$(coordinator_grpcurl \
  "$old_leader" \
  kvdb.coordinator.Coordinator/GetCoordinatorLeader \
  '{}')"
grep -Eq "\"leaderId\"[[:space:]]*:[[:space:]]*\"${new_leader_id}\"" <<< "$restarted_response"

echo "Rolling coordinator restarts without re-bootstrap"
current_leader="$(discover_coordinator_leader)"
current_leader_service="$(coordinator_service_for_address "$current_leader")"
for service in coordinator1 coordinator2 coordinator3; do
  [[ "$service" == "$current_leader_service" ]] && continue
  "${COMPOSE[@]}" restart "$service"
  wait_healthy "$service"
  assert_control_plane
  assert_data_plane
done
"${COMPOSE[@]}" restart "$current_leader_service"
wait_healthy "$current_leader_service"
post_roll_leader="$(discover_coordinator_leader)"
if [[ "$post_roll_leader" == "$current_leader" ]]; then
  echo "Expected a new election while restarting current leader" >&2
  exit 1
fi
assert_control_plane
assert_data_plane

echo "Rolling storage-node restarts with persistent volumes"
for service in node1 node2; do
  "${COMPOSE[@]}" restart "$service"
  wait_healthy "$service"
  assert_data_plane
done

echo "Failover, convergence, rolling restart, and acknowledged-write checks passed"
echo "Clean shutdown and named-volume wipe will be verified during teardown"
