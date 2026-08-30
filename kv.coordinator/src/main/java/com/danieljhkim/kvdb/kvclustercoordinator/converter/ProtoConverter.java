package com.danieljhkim.kvdb.kvclustercoordinator.converter;

import com.danieljhkim.kvdb.kvclustercoordinator.state.NodeRecord;
import com.danieljhkim.kvdb.kvclustercoordinator.state.ShardMapDelta;
import com.danieljhkim.kvdb.kvclustercoordinator.state.ShardMapSnapshot;
import com.danieljhkim.kvdb.kvclustercoordinator.state.ShardRecord;
import com.danieljhkim.kvdb.proto.coordinator.ClusterState;
import com.danieljhkim.kvdb.proto.coordinator.KeyRange;
import com.danieljhkim.kvdb.proto.coordinator.NodeStatus;
import com.danieljhkim.kvdb.proto.coordinator.PartitioningConfig;
import com.danieljhkim.kvdb.proto.coordinator.ShardConfigState;
import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.util.HashMap;

/**
 * Converts between internal state classes and proto-generated classes. Keeps the service layer clean by centralizing
 * proto conversion logic.
 */
public final class ProtoConverter {

    private ProtoConverter() {
        // Utility class
    }

    // ============================
    // NodeRecord Conversions
    // ============================

    public static com.danieljhkim.kvdb.proto.coordinator.NodeRecord toProto(NodeRecord node) {
        if (node == null) {
            return null;
        }

        var builder = com.danieljhkim.kvdb.proto.coordinator.NodeRecord.newBuilder()
                .setNodeId(node.nodeId())
                .setAddress(node.address())
                .setStatus(toProto(node.status()))
                .setLastHeartbeatMs(node.lastHeartbeatMs());

        if (node.zone() != null) {
            builder.setZone(node.zone());
        }
        if (node.rack() != null) {
            builder.setRack(node.rack());
        }
        if (node.capacityHints() != null) {
            builder.putAllCapacityHints(node.capacityHints());
        }

        return builder.build();
    }

    public static NodeStatus toProto(NodeRecord.NodeStatus status) {
        if (status == null) {
            return NodeStatus.NODE_STATUS_UNSPECIFIED;
        }
        return switch (status) {
            case ALIVE -> NodeStatus.ALIVE;
            case SUSPECT -> NodeStatus.SUSPECT;
            case DEAD -> NodeStatus.DEAD;
            case UNSPECIFIED -> NodeStatus.NODE_STATUS_UNSPECIFIED;
        };
    }

    public static NodeRecord.NodeStatus fromProto(NodeStatus status) {
        if (status == null) {
            return NodeRecord.NodeStatus.UNSPECIFIED;
        }
        return switch (status) {
            case ALIVE -> NodeRecord.NodeStatus.ALIVE;
            case SUSPECT -> NodeRecord.NodeStatus.SUSPECT;
            case DEAD -> NodeRecord.NodeStatus.DEAD;
            default -> NodeRecord.NodeStatus.UNSPECIFIED;
        };
    }

    // ============================
    // ShardRecord Conversions
    // ============================

    public static com.danieljhkim.kvdb.proto.coordinator.ShardRecord toProto(ShardRecord shard) {
        if (shard == null) {
            return null;
        }

        var builder = com.danieljhkim.kvdb.proto.coordinator.ShardRecord.newBuilder()
                .setShardId(shard.shardId())
                .setEpoch(shard.epoch())
                .addAllReplicas(shard.replicas())
                .setConfigState(toProto(shard.configState()));

        if (shard.leader() != null) {
            builder.setLeader(shard.leader());
        }

        if (shard.keyRange() != null) {
            builder.setKeyRange(toProto(shard.keyRange()));
        }

        return builder.build();
    }

    public static ShardConfigState toProto(ShardRecord.ShardConfigState state) {
        if (state == null) {
            return ShardConfigState.CONFIG_STATE_UNSPECIFIED;
        }
        return switch (state) {
            case STABLE -> ShardConfigState.STABLE;
            case MOVING -> ShardConfigState.MOVING;
            case UNSPECIFIED -> ShardConfigState.CONFIG_STATE_UNSPECIFIED;
        };
    }

    public static KeyRange toProto(ShardRecord.KeyRange range) {
        if (range == null) {
            return null;
        }
        // Convert int hash values to bytes for the proto
        ByteBuffer startBuffer = ByteBuffer.allocate(4).putInt(range.startHash());
        ByteBuffer endBuffer = ByteBuffer.allocate(4).putInt(range.endHash());

        return KeyRange.newBuilder()
                .setStartKey(ByteString.copyFrom(startBuffer.array()))
                .setEndKey(ByteString.copyFrom(endBuffer.array()))
                .build();
    }

    public static ShardRecord.ShardConfigState fromProto(ShardConfigState state) {
        if (state == null) {
            return ShardRecord.ShardConfigState.UNSPECIFIED;
        }
        return switch (state) {
            case STABLE -> ShardRecord.ShardConfigState.STABLE;
            case MOVING -> ShardRecord.ShardConfigState.MOVING;
            default -> ShardRecord.ShardConfigState.UNSPECIFIED;
        };
    }

    private static ShardRecord.KeyRange fromProto(KeyRange range) {
        if (range.getStartKey().size() != Integer.BYTES || range.getEndKey().size() != Integer.BYTES) {
            throw new IllegalArgumentException("Snapshot shard key ranges must contain exactly four-byte hashes");
        }
        return new ShardRecord.KeyRange(
                ByteBuffer.wrap(range.getStartKey().toByteArray()).getInt(),
                ByteBuffer.wrap(range.getEndKey().toByteArray()).getInt());
    }

    // ============================
    // ShardMapSnapshot Conversions
    // ============================

    public static ClusterState toProto(ShardMapSnapshot snapshot) {
        if (snapshot == null) {
            return null;
        }

        var builder = ClusterState.newBuilder().setMapVersion(snapshot.getMapVersion());

        // Add nodes
        for (var entry : snapshot.getNodes().entrySet()) {
            builder.putNodes(entry.getKey(), toProto(entry.getValue()));
        }

        // Add shards
        for (var entry : snapshot.getShards().entrySet()) {
            builder.putShards(entry.getKey(), toProto(entry.getValue()));
        }

        // Add partitioning config
        builder.setPartitioning(PartitioningConfig.newBuilder()
                .setNumShards(snapshot.getNumShards())
                .setReplicationFactor(snapshot.getReplicationFactor())
                .build());

        return builder.build();
    }

    /** Reconstructs mutable cluster state from a validated snapshot protobuf. */
    public static com.danieljhkim.kvdb.kvclustercoordinator.state.ClusterState fromProto(ClusterState snapshot) {
        var nodes = new HashMap<String, NodeRecord>();
        snapshot.getNodesMap()
                .forEach((id, node) -> nodes.put(
                        id,
                        new NodeRecord(
                                node.getNodeId(),
                                node.getAddress(),
                                node.getZone().isBlank() ? null : node.getZone(),
                                node.getRack().isBlank() ? null : node.getRack(),
                                fromProto(node.getStatus()),
                                node.getLastHeartbeatMs(),
                                node.getCapacityHintsMap())));

        var shards = new HashMap<String, ShardRecord>();
        snapshot.getShardsMap()
                .forEach((id, shard) -> shards.put(
                        id,
                        new ShardRecord(
                                shard.getShardId(),
                                shard.getEpoch(),
                                shard.getReplicasList(),
                                shard.getLeader().isBlank() ? null : shard.getLeader(),
                                fromProto(shard.getConfigState()),
                                shard.hasKeyRange() ? fromProto(shard.getKeyRange()) : null)));

        var state = new com.danieljhkim.kvdb.kvclustercoordinator.state.ClusterState();
        state.restore(
                snapshot.getMapVersion(),
                nodes,
                shards,
                snapshot.hasPartitioning() ? snapshot.getPartitioning().getNumShards() : 0,
                snapshot.hasPartitioning() ? snapshot.getPartitioning().getReplicationFactor() : 1);
        return state;
    }

    // ============================
    // ShardMapDelta Conversions
    // ============================

    public static com.danieljhkim.kvdb.proto.coordinator.ShardMapDelta toProto(ShardMapDelta delta) {
        if (delta == null) {
            return null;
        }

        var builder = com.danieljhkim.kvdb.proto.coordinator.ShardMapDelta.newBuilder()
                .setNewMapVersion(delta.newMapVersion())
                .addAllChangedShards(delta.changedShards())
                .addAllChangedNodes(delta.changedNodes());

        if (delta.fullState() != null) {
            builder.setFullState(toProto(delta.fullState()));
        }

        return builder.build();
    }
}
