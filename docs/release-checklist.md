# Release checklist

Use this checklist for every `vMAJOR.MINOR.PATCH` tag. Keep the completed copy
with the release record; a green workflow alone is not migration or rollback
evidence.

## Supported release matrix

| Surface | Supported release target |
|---|---|
| Java | Temurin/OpenJDK 21 |
| Go CLI | Go 1.24.x; `go.mod` is the minimum toolchain contract |
| Developer validation | Linux amd64/arm64 and macOS arm64 |
| Published containers | Linux amd64 and arm64 |
| Persistent storage | Local Linux/macOS filesystems with working file fsync, atomic rename, and directory fsync |

Windows and network filesystems are not release-qualified. A platform is added
only after the commands below pass on it and the evidence is attached.

## 1. Pin the candidate

Run from a clean checkout of the commit to release:

```bash
export RELEASE_TAG=v1.2.3
export PREVIOUS_TAG=v1.2.2
export RELEASE_SHA="$(git rev-parse HEAD)"
test "$(git status --porcelain)" = ""
git tag --verify "$PREVIOUS_TAG"
git log -1 --format='%H %s' "$RELEASE_SHA"
```

Record the candidate SHA, previous release tag, operator, UTC timestamp, Java
version, Go version, OS, CPU architecture, Docker version, and Compose version.

## 2. Reproduce non-Docker CI locally

```bash
mvn -B -ntp clean verify
mvn -B -ntp spotless:check
test -f coverage/target/site/jacoco-aggregate/jacoco.xml

(
  cd golang/kvcli
  test "$(gofmt -l .)" = ""
  go test -race -covermode=atomic -coverprofile=coverage.out ./...
)

buf lint kv.proto/src/main/proto
buf breaking kv.proto/src/main/proto \
  --against ".git#tag=${PREVIOUS_TAG},subdir=kv.proto/src/main/proto"
```

## 3. Run manual Docker validation

Docker Compose and failover validation are manual release checks; they are not
scheduled by the GitHub Actions CI workflow.

```bash
KVDB_ADMIN_SECURITY_API_KEY=release-check \
KVDB_ENV=test \
KVDB_GRPC_SECURITY_MODE=development-plaintext \
KVDB_TLS_DIR=. \
docker compose config --quiet

./scripts/docker_failover_test.sh
```

Attach the Maven Surefire reports, every module JaCoCo report, the aggregate
JaCoCo report, Go race/coverage output, Buf output, and the container failover
log. The failover log must show an elected coordinator replacement, successful
reads of acknowledged data before and after failover, rolling restarts, and a
clean named-volume wipe.

## 4. Rehearse forward migration

The previous images must exist in GHCR. Build candidate images from the pinned
checkout so this rehearsal tests the exact candidate source before its tag is
published. Use an isolated Compose project and never point these commands at
production volumes.

```bash
export COMPOSE_PROJECT_NAME="kvdb-release-${RELEASE_TAG#v}"
export KVDB_ADMIN_SECURITY_API_KEY="$(openssl rand -hex 32)"
export KVDB_ENV=test
export KVDB_GRPC_SECURITY_MODE=development-plaintext
export KVDB_TLS_DIR="$(pwd)/data/release-tls-empty"
mkdir -p "$KVDB_TLS_DIR"

repository="${GITHUB_REPOSITORY:-danieljhkim/kvdb}"
repository="$(printf '%s' "$repository" | tr '[:upper:]' '[:lower:]')"
cat >/tmp/kvdb-release-images.yml <<'YAML'
services:
  coordinator1:
    image: ghcr.io/${KVDB_RELEASE_REPOSITORY}/kv.coordinator:${KVDB_IMAGE_TAG}
  coordinator2:
    image: ghcr.io/${KVDB_RELEASE_REPOSITORY}/kv.coordinator:${KVDB_IMAGE_TAG}
  coordinator3:
    image: ghcr.io/${KVDB_RELEASE_REPOSITORY}/kv.coordinator:${KVDB_IMAGE_TAG}
  node1:
    image: ghcr.io/${KVDB_RELEASE_REPOSITORY}/kv.node:${KVDB_IMAGE_TAG}
  node2:
    image: ghcr.io/${KVDB_RELEASE_REPOSITORY}/kv.node:${KVDB_IMAGE_TAG}
  gateway:
    image: ghcr.io/${KVDB_RELEASE_REPOSITORY}/kv.gateway:${KVDB_IMAGE_TAG}
  admin:
    image: ghcr.io/${KVDB_RELEASE_REPOSITORY}/kv.admin:${KVDB_IMAGE_TAG}
YAML
export KVDB_RELEASE_REPOSITORY="$repository"

for service in kv.node kv.coordinator kv.admin kv.gateway; do
  docker build \
    --file "${service}/Dockerfile" \
    --tag "ghcr.io/${repository}/${service}:${RELEASE_TAG}" \
    .
done

export KVDB_IMAGE_TAG="$PREVIOUS_TAG"
docker compose -f docker-compose.yml -f /tmp/kvdb-release-images.yml \
  pull --quiet
docker compose -f docker-compose.yml -f /tmp/kvdb-release-images.yml \
  up --detach --no-build --wait --wait-timeout 180
./scripts/bootstrap_cluster.sh
STORAGE_NODE_ADDRS=node1:8001,node2:8002 ./scripts/smoke_test.sh

export KVDB_IMAGE_TAG="$RELEASE_TAG"
docker compose -f docker-compose.yml -f /tmp/kvdb-release-images.yml \
  up --detach --no-build --wait --wait-timeout 180
STORAGE_NODE_ADDRS=node1:8001,node2:8002 ./scripts/smoke_test.sh
```

Record the old and new image digests and attach logs proving the candidate read
data written by the previous release. Stop the release if any persisted Raft,
snapshot, WAL, or shard file is silently reinitialized.

## 5. Verify backup and restore

Back up every labeled volume after the forward-migration rehearsal, wipe the
isolated project, restore the archives into newly created volumes, then rerun
the smoke test:

```bash
export BACKUP_DIR="$(pwd)/release-evidence/${RELEASE_TAG}/backup"
mkdir -p "$BACKUP_DIR"
for volume in $(docker volume ls -q \
  --filter "label=com.docker.compose.project=${COMPOSE_PROJECT_NAME}"); do
  docker run --rm \
    -v "${volume}:/source:ro" \
    -v "${BACKUP_DIR}:/backup" \
    alpine:3.24.1@sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b \
    sh -ec "cd /source && tar -czf /backup/${volume}.tgz ."
done
shasum -a 256 "$BACKUP_DIR"/*.tgz >"$BACKUP_DIR/SHA256SUMS"

docker compose -f docker-compose.yml -f /tmp/kvdb-release-images.yml \
  down --volumes --remove-orphans
for archive in "$BACKUP_DIR"/*.tgz; do
  volume="$(basename "$archive" .tgz)"
  docker volume create "$volume" >/dev/null
  docker run --rm \
    -v "${volume}:/restore" \
    -v "${BACKUP_DIR}:/backup:ro" \
    alpine:3.24.1@sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b \
    sh -ec "cd /restore && tar -xzf /backup/${volume}.tgz"
done
docker compose -f docker-compose.yml -f /tmp/kvdb-release-images.yml \
  up --detach --no-build --wait --wait-timeout 180
STORAGE_NODE_ADDRS=node1:8001,node2:8002 ./scripts/smoke_test.sh
```

Attach `SHA256SUMS`, the restore log, and the post-restore read results. Backups
are not verified until a fresh volume set has served the acknowledged values.

## 6. Prove rollback

First try the previous binaries against the candidate-written data. If backward
compatibility is intentionally unsupported, restore the pre-upgrade archives
before starting the previous release.

```bash
export KVDB_IMAGE_TAG="$PREVIOUS_TAG"
docker compose -f docker-compose.yml -f /tmp/kvdb-release-images.yml \
  up --detach --no-build --wait --wait-timeout 180
STORAGE_NODE_ADDRS=node1:8001,node2:8002 ./scripts/smoke_test.sh
```

Record whether rollback was in-place or restore-based, the exact image digests,
the backup checksum used, the recovery-point objective, elapsed recovery time,
and successful reads after rollback. Do not publish if neither rollback path
works.

## 7. Publish and verify immutable artifacts

Push the tag only after sections 1-6 have evidence:

```bash
git tag -s "$RELEASE_TAG" "$RELEASE_SHA" -m "kvDB ${RELEASE_TAG}"
git push origin "$RELEASE_TAG"
run_id="$(gh run list --workflow docker-publish.yml --commit "$RELEASE_SHA" \
  --limit 1 --json databaseId --jq '.[0].databaseId')"
test -n "$run_id"
gh run watch "$run_id" --exit-status
```

The tag workflow scans dependencies and images, emits SPDX SBOMs and SLSA
provenance, signs each manifest digest with keyless Cosign, and verifies the
signature and both attestations. Independently record the immutable digests:

```bash
repository="${GITHUB_REPOSITORY:-danieljhkim/kvdb}"
repository="$(printf '%s' "$repository" | tr '[:upper:]' '[:lower:]')"
for service in kv.node kv.coordinator kv.admin kv.gateway; do
  image="ghcr.io/${repository}/${service}:${RELEASE_TAG}"
  docker buildx imagetools inspect "$image"
done
```

The release record is complete only when it contains the signed tag, commit
SHA, four image digests, SBOM artifacts, provenance verification, migration
evidence, backup/restore evidence, and rollback evidence.
