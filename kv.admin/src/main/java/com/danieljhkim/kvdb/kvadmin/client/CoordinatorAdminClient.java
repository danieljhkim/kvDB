package com.danieljhkim.kvdb.kvadmin.client;

import com.danieljhkim.kvdb.proto.coordinator.*;
import io.grpc.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * gRPC client for Coordinator admin operations (Raft-replicated writes).
 * Automatically retries on leader changes by following leader hints.
 */
public class CoordinatorAdminClient {

    private static final Logger logger = LoggerFactory.getLogger(CoordinatorAdminClient.class);
    private static final Metadata.Key<String> LEADER_HINT_KEY =
            Metadata.Key.of("leader-hint", Metadata.ASCII_STRING_MARSHALLER);
    private static final int MAX_LEADER_RETRIES = 3;

    private final Map<String, ManagedChannel> channels = new HashMap<>();
    private String currentCoordinatorAddress;
    private CoordinatorGrpc.CoordinatorBlockingStub blockingStub;
    private final long timeoutSeconds;

    public CoordinatorAdminClient(String host, int port, long timeout, TimeUnit timeUnit) {
        this.currentCoordinatorAddress = host + ":" + port;
        this.timeoutSeconds = timeUnit.toSeconds(timeout);
        this.blockingStub = createStubForAddress(currentCoordinatorAddress);
        logger.info("CoordinatorAdminClient created for {}", currentCoordinatorAddress);
    }

    /**
     * Creates or reuses a channel and stub for the given address.
     */
    private CoordinatorGrpc.CoordinatorBlockingStub createStubForAddress(String address) {
        ManagedChannel channel = channels.computeIfAbsent(address, addr -> {
            String[] parts = addr.split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);
            logger.debug("Creating new channel to {}", address);
            return ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        });
        return CoordinatorGrpc.newBlockingStub(channel);
    }

    /**
     * Switches to a new coordinator based on leader hint.
     */
    private void switchToLeader(String leaderAddress) {
        if (leaderAddress != null && !leaderAddress.equals(currentCoordinatorAddress)) {
            logger.info("Switching from {} to leader at {}", currentCoordinatorAddress, leaderAddress);
            currentCoordinatorAddress = leaderAddress;
            blockingStub = createStubForAddress(leaderAddress);
        }
    }

    /**
     * Retries an operation with leader discovery on NOT_LEADER errors.
     */
    private <T> T retryWithLeaderDiscovery(java.util.function.Supplier<T> operation) {
        for (int attempt = 0; attempt < MAX_LEADER_RETRIES; attempt++) {
            try {
                return operation.get();
            } catch (StatusRuntimeException e) {
                // Check if this is a "not leader" error
                if (e.getStatus().getCode() == Status.Code.FAILED_PRECONDITION
                        || e.getStatus().getCode() == Status.Code.UNAVAILABLE) {

                    // Try to extract leader hint from trailers
                    Metadata trailers = e.getTrailers();
                    if (trailers != null) {
                        String leaderHint = trailers.get(LEADER_HINT_KEY);
                        if (leaderHint != null) {
                            logger.info(
                                    "Received leader hint: {} (attempt {}/{})",
                                    leaderHint,
                                    attempt + 1,
                                    MAX_LEADER_RETRIES);
                            switchToLeader(leaderHint);
                            continue; // Retry with new leader
                        }
                    }
                }

                // Not a retriable error or no leader hint
                throw e;
            }
        }

        throw new StatusRuntimeException(
                Status.UNAVAILABLE.withDescription("Failed to find leader after " + MAX_LEADER_RETRIES + " attempts"));
    }

    public RegisterNodeResponse registerNode(String nodeId, String address, String zone) {
        return retryWithLeaderDiscovery(() -> {
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
        });
    }

    public InitShardsResponse initShards(int numShards, int replicationFactor) {
        return retryWithLeaderDiscovery(() -> {
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
        });
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
        for (ManagedChannel channel : channels.values()) {
            try {
                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.warn("Interrupted while shutting down channel");
                channel.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        channels.clear();
    }
}
