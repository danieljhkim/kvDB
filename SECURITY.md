

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
- Does **not** provide built-in authentication or authorization
- Assumes **trusted internal networks** by default
- Is **not yet production-hardened**

Running kvDB in hostile or untrusted environments is **not recommended** at this stage.

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