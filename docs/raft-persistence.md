# Raft persistence and snapshots

Coordinator Raft logs, term/vote state, and snapshots use versioned, bounded, CRC32C-protected records. Existing
length-prefixed logs and properties state files remain readable during rolling upgrades and are rewritten in the new
format on their next mutation. Any truncated, oversized, malformed, or checksum-invalid safety-critical file stops
the coordinator; an existing unreadable file is never interpreted as a new node.

`raft.snapshotThreshold` controls how many newly applied entries a leader retains before snapshotting (`10000` by
default, `0` disables automatic snapshots). The snapshot file is forced before the live log is compacted. Followers
receive snapshots in bounded chunks and persist a restartable installation file before atomically replacing the live
snapshot.

The stable-storage boundary is `write temporary -> fsync(file) -> atomic rename -> fsync(parent directory)`.
Append-only log records are forced before an RPC or command can acknowledge them. kvDB treats failure or lack of
support for file forcing, atomic rename, or directory forcing as a failed Raft write. The supported deployment
filesystems are local Linux and macOS filesystems that implement those operations; network filesystems require
independent crash-consistency qualification.
