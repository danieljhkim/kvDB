package com.danieljhkim.kvdb.kvclustercoordinator.raft;

import com.google.protobuf.InvalidProtocolBufferException;

public record RaftLogEntry(long index, long term, long timestamp, RaftCommand command) {

    public static RaftLogEntry create(long index, long term, RaftCommand command) {
        return new RaftLogEntry(index, term, System.currentTimeMillis(), command);
    }

    /**
     * Serialize to Protocol Buffer bytes
     */
    public byte[] toBytes() {
        com.danieljhkim.kvdb.proto.raft.RaftLogEntry proto = com.danieljhkim.kvdb.proto.raft.RaftLogEntry.newBuilder()
                .setIndex(index)
                .setTerm(term)
                .setTimestamp(timestamp)
                .setCommand(RaftCommandConverter.toProto(command))
                .build();
        return proto.toByteArray();
    }

    /**
     * Deserialize from Protocol Buffer bytes
     */
    public static RaftLogEntry fromBytes(byte[] bytes) throws InvalidProtocolBufferException {
        com.danieljhkim.kvdb.proto.raft.RaftLogEntry proto =
                com.danieljhkim.kvdb.proto.raft.RaftLogEntry.parseFrom(bytes);
        return new RaftLogEntry(
                proto.getIndex(),
                proto.getTerm(),
                proto.getTimestamp(),
                RaftCommandConverter.fromProto(proto.getCommand()));
    }
}
