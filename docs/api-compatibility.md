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

The `limits` configuration bounds key bytes, value bytes, decoded message size,
replication batch entries, context-field bytes, and concurrent RPCs per
connection. The transport rejects oversized frames with gRPC
`RESOURCE_EXHAUSTED`; gateway field validation returns application code
`PAYLOAD_TOO_LARGE`. Storage-node validation also maps through
`RESOURCE_EXHAUSTED`.
