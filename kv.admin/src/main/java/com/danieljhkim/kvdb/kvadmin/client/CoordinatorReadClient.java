package com.danieljhkim.kvdb.kvadmin.client;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.danieljhkim.kvdb.kvadmin.api.dto.KeyRangeDto;
import com.danieljhkim.kvdb.kvadmin.api.dto.NodeDto;
import com.danieljhkim.kvdb.kvadmin.api.dto.PartitioningConfigDto;
import com.danieljhkim.kvdb.kvadmin.api.dto.ShardDto;
import com.danieljhkim.kvdb.kvadmin.api.dto.ShardMapSnapshotDto;
import com.danieljhkim.kvdb.proto.coordinator.ClusterState;
import com.danieljhkim.kvdb.proto.coordinator.CoordinatorGrpc;
import com.danieljhkim.kvdb.proto.coordinator.GetNodeRequest;
import com.danieljhkim.kvdb.proto.coordinator.GetNodeResponse;
import com.danieljhkim.kvdb.proto.coordinator.GetShardMapRequest;
import com.danieljhkim.kvdb.proto.coordinator.GetShardMapResponse;
import com.danieljhkim.kvdb.proto.coordinator.ListNodesRequest;
import com.danieljhkim.kvdb.proto.coordinator.ListNodesResponse;
import com.danieljhkim.kvdb.proto.coordinator.NodeRecord;
import com.danieljhkim.kvdb.proto.coordinator.ShardRecord;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

/**
 * gRPC client for Coordinator read operations.
 */
public class CoordinatorReadClient {

	private static final Logger logger = LoggerFactory.getLogger(CoordinatorReadClient.class);

	private final ManagedChannel channel;
	private final CoordinatorGrpc.CoordinatorBlockingStub blockingStub;
	private final long timeoutSeconds;

	public CoordinatorReadClient(String host, int port, long timeout, TimeUnit timeUnit) {
		this.channel = ManagedChannelBuilder.forAddress(host, port)
				.usePlaintext()
				.build();
		this.blockingStub = CoordinatorGrpc.newBlockingStub(channel);
		this.timeoutSeconds = timeUnit.toSeconds(timeout);
		logger.info("CoordinatorReadClient created for {}:{}", host, port);
	}

	public ShardMapSnapshotDto getShardMap() {
		GetShardMapRequest request = GetShardMapRequest.newBuilder()
				.setIfVersionGt(0)
				.build();

		try {
			GetShardMapResponse response = blockingStub
					.withDeadlineAfter(timeoutSeconds, TimeUnit.SECONDS)
					.getShardMap(request);

			if (response.getNotModified()) {
				// If notModified is true, it means the version hasn't changed,
				// but we should still have the state. If state is missing, that's an error.
				if (!response.hasState()) {
					logger.error("Shard map response marked as notModified but has no state");
					throw new IllegalStateException(
							"Shard map not available: coordinator returned notModified without state");
				}
				logger.debug("Shard map not modified (version unchanged), returning existing state");
			}

			if (!response.hasState()) {
				logger.error("Shard map response has no state");
				throw new IllegalStateException("Shard map not available: coordinator returned response without state");
			}

			ClusterState state = response.getState();
			return convertToDto(state);
		} catch (StatusRuntimeException e) {
			logger.error("Failed to get shard map", e);
			throw e;
		}
	}

	/**
	 * Lists all nodes in the cluster.
	 * 
	 * @return List of NodeDto objects
	 * @throws StatusRuntimeException
	 *             if the RPC fails
	 */
	public List<NodeDto> listNodes() {
		ListNodesRequest request = ListNodesRequest.newBuilder().build();

		try {
			ListNodesResponse response = blockingStub
					.withDeadlineAfter(timeoutSeconds, TimeUnit.SECONDS)
					.listNodes(request);

			return response.getNodesList().stream()
					.map(this::convertNode)
					.collect(Collectors.toList());
		} catch (StatusRuntimeException e) {
			logger.error("Failed to list nodes", e);
			throw e;
		}
	}

	/**
	 * Gets a specific node by node ID.
	 * 
	 * @param nodeId
	 *            The node ID to retrieve
	 * @return NodeDto for the requested node
	 * @throws StatusRuntimeException
	 *             if the RPC fails or node not found
	 */
	public NodeDto getNode(String nodeId) {
		GetNodeRequest request = GetNodeRequest.newBuilder()
				.setNodeId(nodeId)
				.build();

		try {
			GetNodeResponse response = blockingStub
					.withDeadlineAfter(timeoutSeconds, TimeUnit.SECONDS)
					.getNode(request);

			if (!response.hasNode()) {
				throw new StatusRuntimeException(
						io.grpc.Status.NOT_FOUND.withDescription("Node not found: " + nodeId));
			}

			return convertNode(response.getNode());
		} catch (StatusRuntimeException e) {
			logger.error("Failed to get node: {}", nodeId, e);
			throw e;
		}
	}

	private ShardMapSnapshotDto convertToDto(ClusterState state) {
		Map<String, NodeDto> nodes = state.getNodesMap().entrySet().stream()
				.collect(Collectors.toMap(
						Map.Entry::getKey,
						e -> convertNode(e.getValue())));

		Map<String, ShardDto> shards = state.getShardsMap().entrySet().stream()
				.collect(Collectors.toMap(
						Map.Entry::getKey,
						e -> convertShard(e.getValue())));

		PartitioningConfigDto partitioning = null;
		if (state.hasPartitioning()) {
			partitioning = PartitioningConfigDto.builder()
					.numShards(state.getPartitioning().getNumShards())
					.replicationFactor(state.getPartitioning().getReplicationFactor())
					.build();
		}

		return ShardMapSnapshotDto.builder()
				.mapVersion(state.getMapVersion())
				.nodes(nodes)
				.shards(shards)
				.partitioning(partitioning)
				.build();
	}

	private NodeDto convertNode(NodeRecord node) {
		return NodeDto.builder()
				.nodeId(node.getNodeId())
				.address(node.getAddress())
				.zone(node.getZone())
				.rack(node.getRack())
				.status(node.getStatus().name())
				.lastHeartbeatMs(node.getLastHeartbeatMs())
				.capacityHints(node.getCapacityHintsMap())
				.build();
	}

	private ShardDto convertShard(ShardRecord shard) {
		KeyRangeDto keyRange = null;
		if (shard.hasKeyRange()) {
			keyRange = KeyRangeDto.builder()
					.startKey(shard.getKeyRange().getStartKey().toByteArray())
					.endKey(shard.getKeyRange().getEndKey().toByteArray())
					.build();
		}

		return ShardDto.builder()
				.shardId(shard.getShardId())
				.epoch(shard.getEpoch())
				.replicas(shard.getReplicasList())
				.leader(shard.getLeader())
				.configState(shard.getConfigState().name())
				.keyRange(keyRange)
				.build();
	}

	public void shutdown() {
		logger.info("Shutting down CoordinatorReadClient");
		try {
			channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			logger.warn("Interrupted while shutting down CoordinatorReadClient");
			channel.shutdownNow();
			Thread.currentThread().interrupt();
		}
	}
}
