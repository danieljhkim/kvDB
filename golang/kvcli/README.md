# KV CLI

`kvcli` is a non-interactive command line client for the KvDB **gateway data
plane**. It speaks the `KvGateway` gRPC service defined in
[`kv.proto/src/main/proto/kvgateway.proto`](../../kv.proto/src/main/proto/kvgateway.proto)
using generated Go bindings, over the same authenticated transport the cluster
uses internally.

---

## Supported commands

| Command | Aliases | Description |
| --- | --- | --- |
| `kv get [key]` | — | Read one key. Value bytes go to stdout, outcome to stderr. |
| `kv batch-get --input path` | — | Read an ordered JSON array of base64 keys in one RPC. |
| `kv put [key] [value]` | `set` | Write one key. |
| `kv del [key]` | `delete` | Delete one key. |
| `kv ping [key]` | — | One bounded, head-only `Get` that proves reachability. |

There is **no interactive mode and no line protocol**. `kv connect` and
`kv --interactive` exist only to reject the removed behavior with an
explanation. `batch-get` is the only multi-key command; it uses one `BatchGet`
RPC and does not retry it automatically.

---

## Build

```bash
# from this directory
go build -o kv

# or from the repository root
make go-build
```

The generated protocol bindings are committed under
`internal/gen/kvdb/gateway`. Regenerate them reproducibly (pinned
`protoc-gen-go` and `protoc-gen-go-grpc` versions) with:

```bash
make proto-go            # requires protoc on PATH
```

Prerequisites: Go 1.24 or later; `protoc` only if you regenerate bindings.

---

## Configuration

Precedence is **flags → environment → config file → defaults**. The config file
is `--config <path>`, or `./config.yaml` or `$HOME/.kvcli/config.yaml`.

```yaml
server:
  host: localhost
  port: 7000

security:
  mode: mtls
  server_name: kvdb-gateway
  trust_bundle: /run/secrets/kvdb/gateway-client-ca-bundle.pem
  cert_chain: /run/secrets/kvdb/client/tls.crt
  private_key: /run/secrets/kvdb/client/tls.key

request:
  timeout: 5s
  tenant_id: tenant-1
  principal: operator-1
```

| Setting | Flag | Environment variable |
| --- | --- | --- |
| Endpoint | `--host`, `--port`, `--address host:port` | — |
| Security mode | `--security-mode` | `KVDB_GRPC_SECURITY_MODE` |
| Deployment | — | `KVDB_ENV` |
| CA bundle | `--tls-ca` | `KVDB_CLIENT_TLS_TRUST_BUNDLE` |
| Client chain | `--tls-cert` | `KVDB_CLIENT_TLS_CERT_CHAIN` |
| Client key | `--tls-key` | `KVDB_CLIENT_TLS_PRIVATE_KEY` |
| Verified name | `--server-name` | `KVDB_CLIENT_TLS_SERVER_NAME` |
| RPC deadline | `--timeout` | — |
| Tenant / principal | `--tenant`, `--principal` | `KVDB_CLIENT_TENANT_ID`, `KVDB_CLIENT_PRINCIPAL` |

### Authentication

The client follows the server policy documented in
[SECURITY.md](../../SECURITY.md):

- `mtls` is the default. The CA bundle, client certificate chain, and private
  key are all required and must be readable files; the gateway listener
  requires a client certificate, so an anonymous client is rejected. The
  gateway certificate is verified against the connection host unless
  `--server-name` names something else.
- `development-plaintext` must be selected explicitly **and** requires
  `KVDB_ENV` to be `dev`, `development`, `local`, or `test`. It is for local
  development only; development identities are forgeable and must never be used
  on an untrusted network. It also requires both a tenant and a principal. The
  CLI sends `x-kvdb-development-identity: client/<tenant>/<principal>` on every
  RPC, using `--tenant`/`--principal` or the matching environment variables.
  Empty, partial, or malformed values are rejected before an RPC is attempted.
- Any other mode, a missing credential file, or plaintext outside a development
  deployment is refused before a connection is attempted.

In mTLS mode, `RequestContext.tenant_id` and `principal` are optional request
claims that must agree with the verified certificate identity when supplied;
the CLI does not send development identity metadata in that mode.

```bash
export KVDB_GRPC_SECURITY_MODE=mtls
export KVDB_CLIENT_TLS_TRUST_BUNDLE=/run/secrets/kvdb/gateway-client-ca-bundle.pem
export KVDB_CLIENT_TLS_CERT_CHAIN=/run/secrets/kvdb/client/tls.crt
export KVDB_CLIENT_TLS_PRIVATE_KEY=/run/secrets/kvdb/client/tls.key
kv --address gateway:7000 get greeting

# local cluster started by `make run-cluster`; this identity is development-only
export KVDB_ENV=local
export KVDB_CLIENT_TENANT_ID=local
export KVDB_CLIENT_PRINCIPAL=kvcli
kv --security-mode development-plaintext --address 127.0.0.1:7000 ping
```

### Local cluster smoke path

After `make go-build`, `make run-cluster`, and `make bootstrap-cluster`, use
the development-only identity above with the built `./kv` binary. These commands
exercise every supported data-plane operation against the local gateway:

```bash
./kv --security-mode development-plaintext --address 127.0.0.1:7000 put local-key local-value
./kv --security-mode development-plaintext --address 127.0.0.1:7000 get local-key --raw
printf '["bG9jYWwta2V5","bG9jYWwta2V5"]' | ./kv --security-mode development-plaintext --address 127.0.0.1:7000 batch-get --input -
./kv --security-mode development-plaintext --address 127.0.0.1:7000 del local-key
./kv --security-mode development-plaintext --address 127.0.0.1:7000 ping
```

---

## Binary-safe input and output

Keys and values are byte strings, never text. Positional arguments are a
convenience for printable keys and values; use the file options for anything
else. `-` reads standard input, and standard input may be used at most once per
invocation.

```bash
kv put --key-file ./key.bin --value-file ./value.bin
cat payload.bin | kv put config --value-file -
kv get --key-file ./key.bin --raw > value.bin   # stdout gets only the value
kv get --key-file ./key.bin --output-file value.bin
kv get config --head                            # metadata only, no value bytes
printf '["Y29uZmln","AAH/"]' | kv batch-get --input - > results.json
```

Output rules:

- `get` writes value bytes to **stdout** and one `status=... version=...
  applied_version=... value_bytes=... request_id=...` line to **stderr**.
  `--raw` omits the trailing newline, so stdout contains exactly the stored
  bytes. Connection and configuration messages never go to stdout.
- `put` and `del` write `status=OK version=... request_id=...` to stdout.
- `--consistency strong|eventual` selects the read consistency; the default
  leaves the server policy in place.
- `batch-get --input <path|->` accepts one JSON array of standard-base64 key
  strings (up to 1 MiB and 1024 keys) from a file or stdin. Its stdout is one
  versioned JSON document. Each item retains `request_index` and `key_base64`,
  with an item `status`, terminal `outcome`, and metadata. A found empty value
  has `value_base64: ""`; a missing key has `status: "NOT_FOUND"` and omits
  `value_base64`. This preserves duplicates and partial outcomes for scripts.

---

## Deadlines, statuses, and exit codes

Every RPC is bounded by `--timeout` (default 5s) and cancelled on `SIGINT` or
`SIGTERM`. Failures print the stable status name — the `Status.Code` name from
the proto for application failures, the gRPC code name for transport failures.

| Exit | Meaning |
| --- | --- |
| `0` | `OK`. A stored empty value also exits `0` with empty output. |
| `1` | Usage or configuration error; no RPC was attempted. |
| `2` | Non-OK application status (for example `RATE_LIMITED`, `INVALID_ARGUMENT`). |
| `3` | Transport failure (for example `DeadlineExceeded`, `Unauthenticated`). |
| `4` | `NOT_FOUND` — the key does not exist, which is distinct from an empty value. |
| `5` | `WRITE_OUTCOME_UNKNOWN` — the write may or may not have been applied. |

For `batch-get`, a response with any non-OK item status or non-`COMPLETED`
terminal outcome writes the full JSON document and exits `2`. A top-level
application failure writes no document and exits `2`; a deadline, cancellation,
or other gRPC transport failure writes no document and exits `3`. The command
never replays or retries a BatchGet request.

### Write identity and retries

Every write carries a `RequestContext.request_id`; a fresh identifier is
generated per attempt and reported in the output. The CLI never retries a write
on its own. After an ambiguous outcome (exit `5`), rerun the command with the
same identifier so the cluster can de-duplicate it:

```bash
kv put greeting hello --request-id 5b1f6b1e-6d0e-4a54-9c94-1f9a8f4c2f10 --allow-server-replay
```

`--allow-server-replay` sets `require_idempotency`, which permits the gateway to
replay that write under the same request id. It is off by default, so an
ambiguous transport failure is reported instead of replayed.

---

## Tests

```bash
go test -race ./...      # or: make go-test
```

The tests run against a real in-process `KvGateway` gRPC server on an ephemeral
loopback port, including mutual TLS with certificates generated per test.
