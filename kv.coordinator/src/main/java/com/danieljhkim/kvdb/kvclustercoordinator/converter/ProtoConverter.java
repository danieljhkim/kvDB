package com.danieljhkim.kvdb.kvclustercoordinator.converter;

import com.danieljhkim.kvdb.kvclustercoordinator.raft.ShardMapDelta;
import com.danieljhkim.kvdb.kvclustercoordinator.state.NodeRecord;
import com.danieljhkim.kvdb.kvclustercoordinator.state.ShardMapSnapshot;
import com.danieljhkim.kvdb.kvclustercoordinator.state.ShardRecord;
import com.danieljhkim.kvdb.proto.coordinator.ClusterState;
import com.danieljhkim.kvdb.proto.coordinator.KeyRange;
import com.danieljhkim.kvdb.proto.coordinator.NodeStatus;
import com.danieljhkim.kvdb.proto.coordinator.PartitioningConfig;
import com.danieljhkim.kvdb.proto.coordinator.ShardConfigState;
import com.google.protobuf.ByteString;

import java.nio.ByteBuffer;

/**
 * Converts between internal state classes and proto-generated classes.
 * Keeps the service layer clean by centralizing proto conversion logic.
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
