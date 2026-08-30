

# Security Policy

## Supported Versions

kvDB is currently in **early development (alpha / pre-release)**.

Only the **latest version on the `main` branch** is supported for security updates.

| Version | Supported |
|-------|-----------|
| main (latest) | ✅ |
| Older releases | ❌ |

Security fixes are applied only to the latest codebase. Backports are not guaranteed.

---

## Reporting a Vulnerability

If you discover a **security vulnerability**, please report it **responsibly and privately**.

### 📫 How to Report
- **GitHub:** Use **GitHub Security Advisories** (preferrered)
  - Go to the repository
  - Click **Security → Advisories → New draft advisory**

Please **do not** open a public GitHub issue for security vulnerabilities.

---

### 🧾 What to Include

When reporting a vulnerability, include as much of the following as possible:

- A clear description of the issue
- Steps to reproduce
- Affected components (e.g., coordinator, gateway, storage engine, WAL, RPC layer)
- Potential impact (data loss, corruption, DoS, privilege escalation, etc.)
- Any proof-of-concept code or logs (if available)

---

## Response Process

We aim to follow this process:

1. **Acknowledgement** within **72 hours**
2. **Initial assessment** and severity classification
3. **Fix development and validation**
4. **Coordinated disclosure** (if applicable)

Timelines may vary depending on complexity and severity.

---

## Security Scope

### In Scope
- Data corruption or loss
- Authentication / authorization bypass (when applicable)
- Remote code execution
- Denial-of-service vectors
- Insecure default configurations
- gRPC / HTTP API vulnerabilities
- Persistence layer (WAL, snapshots, storage engine)
- Cluster coordination and membership logic

### Out of Scope
- Issues requiring physical access
- Vulnerabilities in third-party dependencies without a kvDB-specific exploit
- Theoretical attacks without practical impact
- Non-security bugs (use GitHub Issues instead)

---

## Security Considerations

kvDB is a **distributed systems learning and research project** and currently:

- Does **not** provide built-in encryption at rest
- Is **not yet production-hardened**

### gRPC identities and transport

All coordinator, Raft, storage-node, admin-to-cluster, gateway-to-cluster, and
gateway client gRPC connections use TLS. Internal listeners require client
certificates issued by the internal workload CA. The gateway listener uses a
separate client trust bundle so external client identity is not interchangeable
with workload identity. Channel hostnames are verified against certificate DNS
SANs; IP literals require matching IP SANs.

Authorization is derived only from a verified URI SAN:

```text
spiffe://kvdb/coordinator/<principal>
spiffe://kvdb/storage-node/<principal>
spiffe://kvdb/gateway/<principal>
spiffe://kvdb/admin/<principal>
spiffe://kvdb/client/<tenant>/<principal>
```

Roles are scoped in the server interceptor. Only coordinators may perform Raft
replication, only storage nodes may perform replica writes and node reports,
only gateways may invoke node data operations, and only admins may invoke
cluster mutations or node shutdown. External clients can invoke only the
gateway data API. A storage-node certificate is therefore insufficient for an
admin mutation. The gateway's `RequestContext.tenant_id` and `principal` fields
are informational and are not authorization inputs; services use the
certificate-derived identity available in `GrpcPeerIdentity`. Bearer
`Authorization` headers are rejected, including replay of a previously captured
value.

### Credential files

Set `KVDB_TLS_DIR` to a directory outside the repository before starting Docker
Compose. `docker-compose.yml` documents the expected per-workload files and
mounts the directory read-only. Each process receives:

- `KVDB_IDENTITY_ROLE` and `KVDB_IDENTITY_PRINCIPAL`
- `KVDB_INTERNAL_TLS_CERT_CHAIN` and `KVDB_INTERNAL_TLS_PRIVATE_KEY`
- `KVDB_INTERNAL_TLS_TRUST_BUNDLE` and `KVDB_INTERNAL_TLS_REVOCATION_LIST`
- Gateway only: the corresponding `KVDB_GATEWAY_TLS_*` server/client-boundary
  files

Never commit private keys, certificate bundles containing keys, or revocation
operational data. Configuration errors name paths but credentials and
certificate contents are never logged.

Issue server certificates with every configured service hostname in DNS SANs
(`coordinator1`, `node1`, and so on for Compose). Client certificates must also
carry exactly one recognized URI SAN from the formats above. The gateway client
trust bundle may contain the external client issuers plus the admin issuer when
the optional admin-to-gateway client is enabled.

### Rotation and revocation

CA and leaf rotation is performed as a rolling change; it does not require a
cluster-wide stop:

1. Add the new issuer certificate to the applicable trust bundle while keeping
   the old issuer, then roll processes.
2. Issue and roll new leaf certificates. Existing and new identities overlap
   during this window.
3. Remove the old issuer after every peer has moved and roll again.

For immediate leaf revocation, write the lowercase SHA-256 fingerprint of the
DER certificate (one per line, colons optional) to the applicable
`*_REVOCATION_LIST`. The interceptor reloads this list on every RPC, so new calls
from a revoked identity are denied without restarting the cluster. Protect
revocation files with the same operational controls as trust bundles.

### Explicit local-development mode

`scripts/run_cluster.sh` sets `KVDB_GRPC_SECURITY_MODE=development-plaintext`
and supplies role/principal headers for local smoke tests. The process refuses
that mode unless `KVDB_ENV` is exactly `local`, `dev`, `development`, or `test`;
the default mode is `mtls`, so a missing or mistyped production configuration
fails closed. Development identities are forgeable and must never be exposed on
an untrusted network. Docker Compose still binds published endpoints to
loopback by default.

---

## Dependency Security

- Dependencies are managed via **Maven**
- Automated tooling (e.g., Dependabot) may be used to surface known vulnerabilities
- Critical dependency updates may be prioritized over feature work

---

## Disclosure Policy

We support **responsible disclosure**.

If you would like public credit for a vulnerability report, please indicate this in your report.

---

## Thanks

We appreciate the effort of the security community and researchers who help improve kvDB.

Responsible disclosure helps make the project stronger for everyone.
