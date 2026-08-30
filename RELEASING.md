# Releasing kvDB

This runbook is for a human-approved release from `main`. It records the
repository's current Maven and container-publishing behavior; it does not add a
separate package registry or GitHub Release workflow.

## Version and compatibility policy

kvDB is already on the `1.x` SemVer line: the latest reachable release tag is
`v1.0.0`. From that baseline, use normal SemVer:

- A backwards-compatible fix or operational/documentation-only change is a
  patch release.
- A backwards-compatible public feature is a minor release.
- Removing or changing a public gRPC/HTTP contract, protobuf field/service
  compatibility, persistent Raft/WAL/snapshot format compatibility, supported
  configuration behavior, or documented deployment contract is a breaking
  change and requires a major release.

Commit prefixes are evidence for a candidate bump, not a compatibility
decision. The release owner must review every breaking-change candidate with a
human before selecting the version.

## Authoritative Maven release version

The root reactor POM, [`pom.xml`](pom.xml), is the authoritative version
carrier. It currently reports `1.0-SNAPSHOT`, while the reachable `v1.0.0` tag
also contains `1.0-SNAPSHOT`. The tag therefore records the released baseline;
the root POM is the current development version and must not be treated as
proof that no `1.0.0` release exists.

Every reactor module inherits that root version through its parent declaration:

| Modules | Version relationship |
| --- | --- |
| `kv.common`, `kv.coordinator`, `kv.node`, `kv.client`, `kv.proto`, `kv.gateway`, `kv.admin`, `coverage` | Parent `com.danieljhkim.kvdb:kvdb:1.0-SNAPSHOT` |
| `kv.common` → `kv.proto`; `kv.coordinator`, `kv.node`, `kv.gateway`, `kv.admin` → `kv.common`/`kv.proto`; `kv.client` → `kv.common`; `coverage` → all service/library modules | Internal dependencies use `${project.version}` |

For a release, set the root version to the selected release version and update
each module parent version to that same value. Verify the effective reactor
version and every internal dependency resolves to it. After the release
commit/tag, restore the root and all module parent declarations together to the
next `-SNAPSHOT` development version in a separately reviewed follow-up; do
not guess that version from the tag.

## Prepare the candidate

1. Work from a clean, current `main` checkout. Identify the latest reachable
   `v*` tag and survey only non-merge commits in `v<previous>..HEAD`:

   ```sh
   git tag --merged HEAD --list 'v*' --sort=-version:refname | head -n 1
   git log v<previous>..HEAD --pretty='%h%x09%s' --no-merges
   git log v<previous>..HEAD --pretty='%s' --no-merges | grep -oE 'DANI-[0-9]+' | sort -u
   ```

2. Do not merge, tag, or begin release edits while any ordinary work is in
   backlog, in-progress, or review. A release-prep probe is exempt only when
   it has the exact provenance tag `auto-task:release-prep`.
3. Produce consumer-facing release notes from the surveyed changes and task
   evidence. This repository has no tracked `CHANGELOG.md` and the tag workflow
   does not create a GitHub Release, so do not invent a changelog update as a
   release prerequisite. Keep the approved notes and completed
   [`docs/release-checklist.md`](docs/release-checklist.md) evidence with the
   release record instead.
4. Review every compatibility candidate with the human. Human approval is
   required before classifying a change as breaking, editing versions, or
   approving the release task.

## Build, compatibility, and security verification

Follow the full local evidence checklist in
[`docs/release-checklist.md`](docs/release-checklist.md), including Maven
verification and formatting, Go race/coverage, Buf compatibility, Compose
configuration, failover/migration, backup/restore, and rollback rehearsal. At
minimum, the release candidate must pass the repository CI equivalents:

```sh
mvn -B -ntp clean verify
mvn -B -ntp spotless:check
(
  cd golang/kvcli
  test "$(gofmt -l .)" = ""
  go test -race -covermode=atomic -coverprofile=coverage.out ./...
)
buf lint kv.proto/src/main/proto
buf breaking kv.proto/src/main/proto \
  --against ".git#tag=v<previous>,subdir=kv.proto/src/main/proto"
KVDB_ADMIN_SECURITY_API_KEY=release-check KVDB_ENV=test \
KVDB_GRPC_SECURITY_MODE=development-plaintext KVDB_TLS_DIR=. \
  docker compose config --quiet
./scripts/docker_failover_test.sh
```

Keep generated test, coverage, migration, backup/restore, rollback, and image
digest evidence with the release record. A failed or unavailable check blocks
publication unless the release owner explicitly records and accepts the
exception.

## Commit, tag, and publish

1. After human approval, commit the selected Maven release-version changes and
   release documentation on `main`; push `main` first.
2. Create a signed annotated tag on that already-pushed commit, then push the
   tag:

   ```sh
   git push origin main
   git tag -s v<X.Y.Z> <release-sha> -m "kvDB v<X.Y.Z>"
   git push origin v<X.Y.Z>
   ```

   Never force-move a release tag.
3. Pushing `v*.*.*` starts
   [`.github/workflows/docker-publish.yml`](.github/workflows/docker-publish.yml).
   It scans release dependencies, builds and pushes linux/amd64 and linux/arm64
   images for `kv.node`, `kv.coordinator`, `kv.admin`, and `kv.gateway`, scans
   immutable digests, uploads SPDX SBOMs, signs each digest with keyless
   Cosign, and attaches provenance and SBOM attestations.
4. Require the `release-smoke` job to verify each published digest's Cosign
   identity, SLSA provenance, and SPDX attestation. Record the four immutable
   image digests and workflow run URL/ID before calling the release complete.
   Non-RC tags also receive `latest`; release evidence must name the immutable
   digest, not just a mutable tag.

## Failure and hotfix recovery

- If validation fails before tagging, fix the candidate, repeat the relevant
  checks, and obtain fresh human approval.
- If tag publication or the release workflow fails, preserve the tag, commit,
  workflow output, and artifact evidence. Do not retarget or force-push the
  tag. Diagnose whether the failure is source, credential, registry, or CI
  infrastructure, then cut a new patch tag after the fix is reviewed and
  verified.
- For an urgent released defect, branch from the released `main` commit, keep
  the fix minimal, rerun the release checklist, obtain human approval, and cut
  the next patch release. Use the documented migration, backup/restore, and
  rollback evidence rather than rewriting an existing release.
