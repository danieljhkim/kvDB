package com.danieljhkim.kvdb.kvadmin.client;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.danieljhkim.kvdb.proto.coordinator.CoordinatorGrpc;
import com.danieljhkim.kvdb.proto.coordinator.InitShardsRequest;
import com.danieljhkim.kvdb.proto.coordinator.InitShardsResponse;
import com.danieljhkim.kvdb.proto.coordinator.RegisterNodeRequest;
import com.danieljhkim.kvdb.proto.coordinator.RegisterNodeResponse;
import com.danieljhkim.kvdb.proto.coordinator.SetNodeStatusRequest;
import com.danieljhkim.kvdb.proto.coordinator.SetNodeStatusResponse;
import com.danieljhkim.kvdb.proto.coordinator.SetShardLeaderRequest;
import com.danieljhkim.kvdb.proto.coordinator.SetShardLeaderResponse;
import com.danieljhkim.kvdb.proto.coordinator.SetShardReplicasRequest;
import com.danieljhkim.kvdb.proto.coordinator.SetShardReplicasResponse;
import com.danieljhkim.kvdb.proto.coordinator.NodeStatus;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

/**
 * gRPC client for Coordinator admin operations (Raft-replicated writes).
 */
public class CoordinatorAdminClient {

	private static final Logger logger = LoggerFactory.getLogger(CoordinatorAdminClient.class);

	private final ManagedChannel channel;
	private final CoordinatorGrpc.CoordinatorBlockingStub blockingStub;
	private final long timeoutSeconds;

	public CoordinatorAdminClient(String host, int port, long timeout, TimeUnit timeUnit) {
		this.channel = ManagedChannelBuilder.forAddress(host, port)
				.usePlaintext()
				.build();
		this.blockingStub = CoordinatorGrpc.newBlockingStub(channel);
		this.timeoutSeconds = timeUnit.toSeconds(timeout);
		logger.info("CoordinatorAdminClient created for {}:{}", host, port);
	}

	public RegisterNodeResponse registerNode(String nodeId, String address, String zone) {
		RegisterNodeRequest request = RegisterNodeRequest.newBuilder()
				.setNodeId(nodeId)
				.setAddress(address)
				.setZone(zone != null ? zone : "")
				.build();

		try {
			return blockingStub
					.withDeadlineAfter(timeoutSeconds, TimeUnit.SECONDS)
					.registerNode(request);
		} catch (StatusRuntimeException e) {
			logger.error("Failed to register node: {}", nodeId, e);
			throw e;
		}
	}

	public InitShardsResponse initShards(int numShards, int replicationFactor) {
		InitShardsRequest request = InitShardsRequest.newBuilder()
				.setNumShards(numShards)
				.setReplicationFactor(replicationFactor)
				.build();

		try {
			return blockingStub
					.withDeadlineAfter(timeoutSeconds, TimeUnit.SECONDS)
					.initShards(request);
		} catch (StatusRuntimeException e) {
			logger.error("Failed to initialize shards", e);
			throw e;
		}
	}

	public SetNodeStatusResponse setNodeStatus(String nodeId, String status) {
		NodeStatus nodeStatus = NodeStatus.valueOf(status);
		SetNodeStatusRequest request = SetNodeStatusRequest.newBuilder()
				.setNodeId(nodeId)
				.setStatus(nodeStatus)
				.build();

		try {
			return blockingStub
					.withDeadlineAfter(timeoutSeconds, TimeUnit.SECONDS)
					.setNodeStatus(request);
		} catch (StatusRuntimeException e) {
			logger.error("Failed to set node status: {} -> {}", nodeId, status, e);
			throw e;
		}
	}

	public SetShardReplicasResponse setShardReplicas(String shardId, List<String> replicaNodeIds) {
		SetShardReplicasRequest request = SetShardReplicasRequest.newBuilder()
				.setShardId(shardId)
				.addAllReplicas(replicaNodeIds)
				.build();

		try {
			return blockingStub
					.withDeadlineAfter(timeoutSeconds, TimeUnit.SECONDS)
					.setShardReplicas(request);
		} catch (StatusRuntimeException e) {
			logger.error("Failed to set shard replicas: {}", shardId, e);
			throw e;
		}
	}

	public SetShardLeaderResponse setShardLeader(String shardId, long epoch, String leaderNodeId) {
		SetShardLeaderRequest request = SetShardLeaderRequest.newBuilder()
				.setShardId(shardId)
				.setEpoch(epoch)
				.setLeaderNodeId(leaderNodeId)
				.build();

		try {
			return blockingStub
					.withDeadlineAfter(timeoutSeconds, TimeUnit.SECONDS)
					.setShardLeader(request);
		} catch (StatusRuntimeException e) {
			logger.error("Failed to set shard leader: {} -> {}", shardId, leaderNodeId, e);
			throw e;
		}
	}

	public void shutdown() {
		logger.info("Shutting down CoordinatorAdminClient");
		try {
			channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			logger.warn("Interrupted while shutting down CoordinatorAdminClient");
			channel.shutdownNow();
			Thread.currentThread().interrupt();
		}
	}
}

