package com.danieljhkim.kvdb.kvclustercoordinator.converter;

import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftCommand;
import com.danieljhkim.kvdb.kvclustercoordinator.state.NodeRecord;
import com.google.protobuf.InvalidProtocolBufferException;

public class RaftCommandConverter {

    /**
     * Convert domain RaftCommand to Protocol Buffer message
     */
    public static com.danieljhkim.kvdb.proto.raft.RaftCommand toProto(RaftCommand command) {
        com.danieljhkim.kvdb.proto.raft.RaftCommand.Builder builder =
                com.danieljhkim.kvdb.proto.raft.RaftCommand.newBuilder();

        switch (command) {
            case RaftCommand.InitShards cmd -> builder.setInitShards(
                    com.danieljhkim.kvdb.proto.raft.InitShards.newBuilder()
                            .setNumShards(cmd.numShards())
                            .setReplicationFactor(cmd.replicationFactor())
                            .build());
            case RaftCommand.RegisterNode cmd -> builder.setRegisterNode(
                    com.danieljhkim.kvdb.proto.raft.RegisterNode.newBuilder()
                            .setNodeId(cmd.nodeId())
                            .setAddress(cmd.address())
                            .setZone(cmd.zone())
                            .build());
            case RaftCommand.SetNodeStatus cmd -> builder.setSetNodeStatus(
                    com.danieljhkim.kvdb.proto.raft.SetNodeStatus.newBuilder()
                            .setNodeId(cmd.nodeId())
                            .setStatus(cmd.status().name())
                            .build());
            case RaftCommand.SetShardReplicas cmd -> builder.setSetShardReplicas(
                    com.danieljhkim.kvdb.proto.raft.SetShardReplicas.newBuilder()
                            .setShardId(cmd.shardId())
                            .addAllReplicas(cmd.replicas())
                            .build());
            case RaftCommand.SetShardLeader cmd -> builder.setSetShardLeader(
                    com.danieljhkim.kvdb.proto.raft.SetShardLeader.newBuilder()
                            .setShardId(cmd.shardId())
                            .setEpoch(cmd.epoch())
                            .setLeaderNodeId(cmd.leaderNodeId())
                            .build());
        }

        return builder.build();
    }

    /**
     * Convert Protocol Buffer message to domain RaftCommand
     */
    public static RaftCommand fromProto(com.danieljhkim.kvdb.proto.raft.RaftCommand proto) {
        return switch (proto.getCommandCase()) {
            case INIT_SHARDS -> {
                com.danieljhkim.kvdb.proto.raft.InitShards initShards = proto.getInitShards();
                yield new RaftCommand.InitShards(initShards.getNumShards(), initShards.getReplicationFactor());
            }
            case REGISTER_NODE -> {
                com.danieljhkim.kvdb.proto.raft.RegisterNode registerNode = proto.getRegisterNode();
                yield new RaftCommand.RegisterNode(
                        registerNode.getNodeId(), registerNode.getAddress(), registerNode.getZone());
            }
            case SET_NODE_STATUS -> {
                com.danieljhkim.kvdb.proto.raft.SetNodeStatus setNodeStatus = proto.getSetNodeStatus();
                yield new RaftCommand.SetNodeStatus(
                        setNodeStatus.getNodeId(), NodeRecord.NodeStatus.valueOf(setNodeStatus.getStatus()));
            }
            case SET_SHARD_REPLICAS -> {
                com.danieljhkim.kvdb.proto.raft.SetShardReplicas setShardReplicas = proto.getSetShardReplicas();
                yield new RaftCommand.SetShardReplicas(
                        setShardReplicas.getShardId(), setShardReplicas.getReplicasList());
            }
            case SET_SHARD_LEADER -> {
                com.danieljhkim.kvdb.proto.raft.SetShardLeader setShardLeader = proto.getSetShardLeader();
                yield new RaftCommand.SetShardLeader(
                        setShardLeader.getShardId(), setShardLeader.getEpoch(), setShardLeader.getLeaderNodeId());
            }
            case COMMAND_NOT_SET -> throw new IllegalArgumentException("Command not set in proto");
        };
    }

    /**
     * Serialize command to bytes for storage
     */
    public static byte[] serialize(RaftCommand command) {
        return toProto(command).toByteArray();
    }

    /**
     * Deserialize command from bytes
     */
    public static RaftCommand deserialize(byte[] bytes) throws InvalidProtocolBufferException {
        com.danieljhkim.kvdb.proto.raft.RaftCommand proto =
                com.danieljhkim.kvdb.proto.raft.RaftCommand.parseFrom(bytes);
        return fromProto(proto);
    }
}
