#!/usr/bin/env bash
set -euo pipefail

# Issues throwaway mTLS material for the Docker Compose cluster and its operator
# scripts. Intended for local and rehearsal clusters; production deployments
# should issue the same identities from their own CA.
#
# Writes two directories, which must not exist yet or must be empty:
#
#   KVDB_TLS_DIR           workload material mounted read-only into every
#                          container (layout documented in docker-compose.yml)
#   KVDB_OPERATOR_TLS_DIR  host-only operator and client credentials read by
#                          scripts/cluster_helpers.sh; never mounted into containers
#
# Identities (URI SANs, see SECURITY.md):
#   coordinator-1..3, node-1..2, admin-1, gateway-1   workload certificates
#   spiffe://kvdb/admin/<KVDB_OPERATOR_PRINCIPAL>     operator: node registration and shard init
#   spiffe://kvdb/client/<tenant>/<principal>         gateway data API client only
#
# Coordinator and gateway server certificates also carry localhost and
# 127.0.0.1, because Compose publishes them on host loopback. The CA private
# keys are discarded when the script exits; rotate by generating a new set.
#
# Optional env:
#   KVDB_TLS_VALID_DAYS=90
#   KVDB_OPERATOR_PRINCIPAL=operator
#   KVDB_CLIENT_TENANT_ID=smoke
#   KVDB_CLIENT_PRINCIPAL=smoke-test

WORKLOAD_DIR="${KVDB_TLS_DIR:?Set KVDB_TLS_DIR to the workload credential directory}"
OPERATOR_DIR="${KVDB_OPERATOR_TLS_DIR:?Set KVDB_OPERATOR_TLS_DIR to the operator credential directory}"
VALID_DAYS="${KVDB_TLS_VALID_DAYS:-90}"
OPERATOR_PRINCIPAL="${KVDB_OPERATOR_PRINCIPAL:-operator}"
CLIENT_TENANT="${KVDB_CLIENT_TENANT_ID:-smoke}"
CLIENT_PRINCIPAL="${KVDB_CLIENT_PRINCIPAL:-smoke-test}"

if ! command -v openssl >/dev/null 2>&1; then
  echo "openssl is required" >&2
  exit 1
fi
if ! [[ "$VALID_DAYS" =~ ^[1-9][0-9]*$ ]]; then
  echo "KVDB_TLS_VALID_DAYS must be a positive integer" >&2
  exit 1
fi
for value in "$OPERATOR_PRINCIPAL" "$CLIENT_TENANT" "$CLIENT_PRINCIPAL"; do
  if ! [[ "$value" =~ ^[A-Za-z0-9._-]+$ ]]; then
    echo "Identity components may contain only letters, digits, '.', '_' and '-': ${value}" >&2
    exit 1
  fi
done

prepare_dir() {
  local dir="$1"
  if [[ -e "$dir" && ! -d "$dir" ]]; then
    echo "Not a directory: ${dir}" >&2
    return 1
  fi
  if [[ -d "$dir" && -n "$(find "$dir" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
    echo "Refusing to write into non-empty directory: ${dir}" >&2
    return 1
  fi
  mkdir -p "$dir"
  (cd "$dir" && pwd -P)
}

umask 077
WORKLOAD_DIR="$(prepare_dir "$WORKLOAD_DIR")"
OPERATOR_DIR="$(prepare_dir "$OPERATOR_DIR")"
case "$OPERATOR_DIR/" in
  "$WORKLOAD_DIR"/*)
    echo "KVDB_OPERATOR_TLS_DIR must not be inside KVDB_TLS_DIR: operator keys would be mounted into containers" >&2
    exit 1
    ;;
esac
case "$WORKLOAD_DIR/" in
  "$OPERATOR_DIR"/*)
    echo "KVDB_TLS_DIR must not be inside KVDB_OPERATOR_TLS_DIR" >&2
    exit 1
    ;;
esac

CA_DIR="$(mktemp -d "${TMPDIR:-/tmp}/kvdb-ca.XXXXXX")"
trap 'rm -rf "$CA_DIR"' EXIT

new_key() {
  openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$1" 2>/dev/null
}

serial() {
  printf '0x%s' "$(openssl rand -hex 16)"
}

# usage: issue_ca <name> <common name>
issue_ca() {
  local name="$1" common_name="$2"
  new_key "$CA_DIR/$name.key"
  cat > "$CA_DIR/$name.cnf" <<EOF
[req]
distinguished_name = dn
prompt = no
x509_extensions = ca
[dn]
CN = ${common_name}
[ca]
basicConstraints = critical,CA:TRUE,pathlen:0
keyUsage = critical,keyCertSign,cRLSign
subjectKeyIdentifier = hash
EOF
  openssl req -new -x509 -key "$CA_DIR/$name.key" -config "$CA_DIR/$name.cnf" \
    -days "$VALID_DAYS" -sha256 -set_serial "$(serial)" -out "$CA_DIR/$name.crt"
}

# usage: issue_leaf <ca> <output dir> <common name> <extended key usage> <subject alt names>
issue_leaf() {
  local ca="$1" dir="$2" common_name="$3" usage="$4" sans="$5"
  mkdir -p "$dir"
  new_key "$dir/tls.key"
  openssl req -new -key "$dir/tls.key" -subj "/CN=${common_name}" -out "$CA_DIR/leaf.csr"
  cat > "$CA_DIR/leaf.ext" <<EOF
basicConstraints = critical,CA:FALSE
keyUsage = critical,digitalSignature
extendedKeyUsage = ${usage}
subjectAltName = ${sans}
authorityKeyIdentifier = keyid
subjectKeyIdentifier = hash
EOF
  openssl x509 -req -in "$CA_DIR/leaf.csr" -CA "$CA_DIR/$ca.crt" -CAkey "$CA_DIR/$ca.key" \
    -set_serial "$(serial)" -days "$VALID_DAYS" -sha256 -extfile "$CA_DIR/leaf.ext" \
    -out "$dir/tls.crt" 2>/dev/null
}

issue_ca internal "kvdb internal workload CA"
issue_ca client "kvdb gateway client CA"

workload() { # <dir name> <role> <principal> <dns names...>
  local dir="$1" role="$2" principal="$3" sans="" host
  shift 3
  for host in "$@"; do
    sans="${sans}DNS:${host},"
  done
  issue_leaf internal "$WORKLOAD_DIR/$dir" "$principal" serverAuth,clientAuth \
    "${sans}IP:127.0.0.1,URI:spiffe://kvdb/${role}/${principal}"
}

for i in 1 2 3; do
  workload "coordinator-$i" coordinator "coordinator-$i" "coordinator$i" localhost
done
for i in 1 2; do
  workload "node-$i" storage-node "node-$i" "node$i" localhost
done
workload admin admin admin-1 admin localhost
workload gateway-internal gateway gateway-1 gateway localhost
workload gateway-server gateway gateway-1 gateway localhost

cp "$CA_DIR/internal.crt" "$WORKLOAD_DIR/internal-ca-bundle.pem"
cp "$CA_DIR/client.crt" "$WORKLOAD_DIR/gateway-client-ca-bundle.pem"
: > "$WORKLOAD_DIR/internal-revoked.sha256"
: > "$WORKLOAD_DIR/gateway-revoked.sha256"

issue_leaf internal "$OPERATOR_DIR/operator" "$OPERATOR_PRINCIPAL" clientAuth \
  "URI:spiffe://kvdb/admin/${OPERATOR_PRINCIPAL}"
issue_leaf client "$OPERATOR_DIR/client" "$CLIENT_PRINCIPAL" clientAuth \
  "URI:spiffe://kvdb/client/${CLIENT_TENANT}/${CLIENT_PRINCIPAL}"
cp "$CA_DIR/internal.crt" "$OPERATOR_DIR/internal-ca-bundle.pem"
# The gateway server certificate is issued by the internal CA.
cp "$CA_DIR/internal.crt" "$OPERATOR_DIR/gateway-ca-bundle.pem"

echo "Workload credentials: ${WORKLOAD_DIR}"
echo "Operator credentials: ${OPERATOR_DIR} (admin/${OPERATOR_PRINCIPAL}, client/${CLIENT_TENANT}/${CLIENT_PRINCIPAL})"
