# Data-plane compatibility policy

kvDB treats protobuf field numbers and their wire types as permanent once a
message is published. Existing fields are not renumbered or reused, new fields
are additive, and receivers preserve unknown fields when a message is parsed
and forwarded. Removing a field requires reserving both its number and name.

The gateway API already used `bytes` for keys and values. The internal
`KVService` fields 1/2 and replication fields 6/7 moved from `string` to
`bytes`; both use protobuf's length-delimited wire type. Existing UTF-8 clients
therefore remain wire-compatible with upgraded nodes, while upgraded peers no
longer perform a lossy text conversion. A rolling deployment must upgrade
storage nodes before gateways begin sending non-UTF-8 data, because an old node
may reject an invalid-UTF-8 `string` payload.

`if_version_equals` now has explicit proto3 presence without changing field 4's
varint encoding. Absent means no CAS guard; a present zero means the caller
expects no versioned live value. Unknown enum values and unsupported options
are rejected rather than silently treated as defaults.

Current option behavior is:

- unspecified durability defaults to quorum sync; `WAL_SYNC` is a local fsync,
  `QUORUM_SYNC` is a quorum fsync, and `WAL_ASYNC` is rejected;
- TTL is supported for puts and rejected for deletes;
- create-only and CAS are serialized with mutation admission; retries with the
  same request ID return the original committed version;
- head-only reads return existence and metadata with an empty value field;
- max-staleness bounds are rejected; read-your-writes requires strong
  consistency and low-latency mode requires eventual consistency.

Write request IDs identify an immutable mutation in the storage shard's
deduplication journal. Reusing an ID there with a different key, value, mutation
kind, TTL, or conditional-write options returns gRPC `INVALID_ARGUMENT` from
the node and application `INVALID_ARGUMENT` through the gateway. This is a
non-retryable conflict, including when `require_idempotency` is enabled: the
gateway does not replay it. The conflicting request leaves stored data
unchanged. An identical put or delete replay returns the original version.
Clients must use a fresh request ID for a different mutation. IDs are tracked
per shard; this does not introduce cluster-wide request-ID uniqueness.

Replica repair and leader reconciliation carry these identities too. Besides
the newest mutation per key in `committed_mutations`, a transfer lists the
overwritten committed mutations in the additive `superseded_mutations` field.
The receiver journals only their request identities and never exposes their
values, so a promoted replica answers a retried put or delete with its original
version. Both lists share one version cursor and the batch entry and message
limits. `FetchReplicaState` returns superseded entries only when the request
sets `include_superseded`, so older receivers keep their newest-per-key stream;
older senders simply leave the field empty. The receiver journals these
identities with a new replication-WAL record, so a node that has received one
cannot be downgraded to a build that does not recognize it.

The gateway preserves definitive node RPC status codes rather than converting
request errors to retryable `UNAVAILABLE`. Missing candidates and confirmed
pre-mutation quorum failures return `UNAVAILABLE`. A storage node rejecting
write admission before this request can prepare a mutation includes the
`x-write-outcome: not-applied` trailer with gRPC `UNAVAILABLE`. The gateway may
retry this definite rejection even without server replay enabled. Local
connection-establishment failures with a `ConnectException` cause are also
definite. Unmarked transport loss, generic I/O errors, and deadlines after a
write may have reached the node remain `WRITE_OUTCOME_UNKNOWN` unless server
replay is enabled. Older nodes without the trailer retain conservative outcome
classification. The CLI reports application `UNAVAILABLE` as exit `2` and
`WRITE_OUTCOME_UNKNOWN` as exit `5`. A completed RPC whose outcome line cannot
be written is exit `6` (`OUTPUT`). That outcome is known, and the CLI does not
repeat the RPC.

The `limits` configuration bounds key bytes, value bytes, decoded message size,
replication batch entries, context-field bytes, and concurrent RPCs per
connection. The transport rejects oversized frames with gRPC
`RESOURCE_EXHAUSTED`; gateway field validation returns application code
`PAYLOAD_TOO_LARGE`. Storage-node validation also maps through
`RESOURCE_EXHAUSTED`.

## BatchGet semantics and limits

`BatchGet` accepts binary `keys`, shared `ReadOptions`, and a shared optional
`head_only` flag. For every accepted request, `results` contains exactly one
entry per input key in input order. Duplicate keys remain duplicate results.
Each result echoes its binary key and carries the same status, value/version
metadata, and serving-node `applied_version` as unary `Get`.

**There is no cross-key snapshot or atomic-read guarantee.** Each key is routed
and read independently. `STRONG`, `EVENTUAL`, and `head_only` therefore have
exactly the unary `Get` semantics for that item; results from different keys
may reflect different instants or shard versions.

Example request (protobuf text notation):

```protobuf
keys: "\000customer-1"
keys: "\377customer-2"
keys: "\000customer-1"  // intentionally repeated
options { consistency: STRONG }
head_only: false
ctx { request_id: "read-set-42" }
```

The default gateway bounds are configured under `limits`:

- `maxBatchEntries: 128`
- `maxBatchAggregateKeyBytes: 65536`
- `maxBatchGetConcurrency: 16`
- `maxBatchGetResponseBytes: 2097152`

Key-count, aggregate-key-size, individual-key, option, and inbound-message
violations fail request-wide validation before any storage read is dispatched.
After dispatch, found, not-found, and unavailable results can coexist. Deadline
and cancellation outcomes explicitly mark every remaining key. If a successful
item would exceed the response budget, it and all remaining items are returned
as `RESPONSE_BUDGET_EXHAUSTED`; the serialized response remains within the
configured budget. The budget must be large enough to encode one termination
outcome per admitted key, or the request is rejected before dispatch.
