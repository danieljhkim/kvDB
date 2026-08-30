

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
- Does **not** provide TLS for gRPC (plaintext inside the deployment boundary)
- Is **not yet production-hardened**

### Internal gRPC authentication

Coordinator and storage-node gRPC listeners require a cluster-wide token on
control-plane mutations, data-plane writes, replication, shutdown, and Raft RPCs.
Clients send it as metadata header `x-kvdb-internal-token`.

Set the same value on every internal process:

```bash
export KVDB_INTERNAL_GRPC_TOKEN=$(openssl rand -hex 32)
```

- **Docker Compose:** coordinator seeds are published only on loopback as
  `127.0.0.1:9001` through `:9003` for authenticated bootstrap and failover
  diagnostics. Storage-node `:8001`/`:8002` and the internal Raft addresses
  remain reachable only on the `kvdb-net` bridge. Compose refuses to start
  unless `KVDB_INTERNAL_GRPC_TOKEN` is set (no committed default). Gateway
  `:7000` and admin `:8089` are also bound to loopback by default.
- **Local `scripts/run_cluster.sh`:** an ephemeral token is written to
  `data/.internal-grpc-token` (covered by `data/*` in `.gitignore`) and exported
  to coordinator, node, gateway, and admin processes. Do not publish
  coordinator/node ports on untrusted networks.
- **Gateway client API** (`Get`/`Put`/`Delete`) is the public data plane and is
  not gated by this token. Do not publish coordinator/node ports in production.

The node `Shutdown` RPC is retained for operator use and is rejected without a
valid token. mTLS is a follow-up; token + private network is the current
authenticated boundary.

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
