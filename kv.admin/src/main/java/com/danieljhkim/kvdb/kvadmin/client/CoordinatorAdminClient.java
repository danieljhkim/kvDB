package com.danieljhkim.kvdb.kvadmin.client;

import com.danieljhkim.kvdb.proto.coordinator.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * gRPC client for Coordinator admin operations.
 * Automatically discovers and routes requests to the Raft leader.
 */
public class CoordinatorAdminClient {

    private static final Logger logger = LoggerFactory.getLogger(CoordinatorAdminClient.class);
    private static final int MAX_RETRIES = 5;

    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final Map<String, CoordinatorGrpc.CoordinatorBlockingStub> stubs = new ConcurrentHashMap<>();
    private final List<String> coordinatorAddresses;
    private final AtomicReference<String> leaderAddress = new AtomicReference<>();
    private final long timeoutSeconds;

    public CoordinatorAdminClient(String host, int port, long timeout, TimeUnit timeUnit) {
        this(List.of(host + ":" + port), timeout, timeUnit);
    }

    public CoordinatorAdminClient(List<String> coordinatorAddresses, long timeout, TimeUnit timeUnit) {
        this.coordinatorAddresses = new ArrayList<>(coordinatorAddresses);
        this.timeoutSeconds = timeUnit.toSeconds(timeout);
        logger.info("CoordinatorAdminClient created for: {}", coordinatorAddresses);
    }

    private CoordinatorGrpc.CoordinatorBlockingStub getStub(String address) {
        return stubs.computeIfAbsent(address, addr -> {
                    ManagedChannel channel = channels.computeIfAbsent(addr, a -> {
                        String[] parts = a.split(":");
                        return ManagedChannelBuilder.forAddress(parts[0], Integer.parseInt(parts[1]))
                                .usePlaintext()
                                .build();
                    });
                    return CoordinatorGrpc.newBlockingStub(channel);
                })
                .withDeadlineAfter(timeoutSeconds, TimeUnit.SECONDS);
    }

    @SuppressWarnings("all")
    private CoordinatorGrpc.CoordinatorBlockingStub getLeaderStub() {
        // Use cached leader if available - but verify it's still the leader
        String leader = leaderAddress.get();
        if (leader != null) {
            try {
                GetCoordinatorLeaderResponse response =
                        getStub(leader).getCoordinatorLeader(GetCoordinatorLeaderRequest.getDefaultInstance());
                if (response.getIsLeader()) {
                    return getStub(leader);
                }
                // Cached leader is no longer the leader, clear it
                logger.info("Cached leader {} is no longer leader", leader);
                leaderAddress.set(null);
            } catch (StatusRuntimeException e) {
                logger.debug("Failed to verify cached leader {}: {}", leader, e.getStatus());
                leaderAddress.set(null);
            }
        }

        // Discover leader by querying coordinators
        for (String address : coordinatorAddresses) {
            try {
                GetCoordinatorLeaderResponse response =
                        getStub(address).getCoordinatorLeader(GetCoordinatorLeaderRequest.getDefaultInstance());
                if (response.getIsLeader()) {
                    leaderAddress.set(address);
                    logger.info("Discovered leader: {}", address);
                    return getStub(address);
                }
                if (!response.getLeaderAddress().isEmpty()) {
                    String hint = response.getLeaderAddress();
                    // Verify the hinted leader
                    try {
                        GetCoordinatorLeaderResponse hintResponse =
                                getStub(hint).getCoordinatorLeader(GetCoordinatorLeaderRequest.getDefaultInstance());
                        if (hintResponse.getIsLeader()) {
                            leaderAddress.set(hint);
                            logger.info("Discovered and verified leader via hint: {}", hint);
                            if (!coordinatorAddresses.contains(hint)) {
                                coordinatorAddresses.add(hint);
                            }
                            return getStub(hint);
                        }
                    } catch (StatusRuntimeException e) {
                        logger.debug("Failed to verify hinted leader {}: {}", hint, e.getStatus());
                    }
                }
            } catch (StatusRuntimeException e) {
                logger.debug("Failed to query {}: {}", address, e.getStatus());
            }
        }

        // Fallback to first coordinator
        logger.warn("Could not discover leader, using first coordinator");
        return getStub(coordinatorAddresses.getFirst());
    }

    @SuppressWarnings("all")
    private <T> T execute(Function<CoordinatorGrpc.CoordinatorBlockingStub, T> operation) {
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                return operation.apply(getLeaderStub());
            } catch (StatusRuntimeException e) {
                Status.Code code = e.getStatus().getCode();
                if (code == Status.Code.FAILED_PRECONDITION || code == Status.Code.UNAVAILABLE) {
                    // Try to extract leader hint from error description
                    String description = e.getStatus().getDescription();
                    String hint = extractLeaderHintFromDescription(description);
                    if (hint != null) {
                        logger.info("Extracted leader hint from error: {}", hint);
                        leaderAddress.set(hint);
                        if (!coordinatorAddresses.contains(hint)) {
                            coordinatorAddresses.add(hint);
                        }
                    } else {
                        leaderAddress.set(null); // Clear and rediscover
                    }
                    logger.info("Leader unavailable, retrying ({}/{})", attempt + 1, MAX_RETRIES);

                    // Small delay to allow cluster to stabilize
                    try {
                        Thread.sleep(100 * (attempt + 1));
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new StatusRuntimeException(Status.CANCELLED.withDescription("Interrupted"));
                    }
                    continue;
                }
                throw e;
            }
        }
        throw new StatusRuntimeException(
                Status.UNAVAILABLE.withDescription("Failed after " + MAX_RETRIES + " attempts"));
    }

    private String extractLeaderHintFromDescription(String description) {
        if (description == null) {
            return null;
        }
        // Parse "Not the leader. Leader hint: localhost:9003"
        String prefix = "Leader hint: ";
        int idx = description.indexOf(prefix);
        if (idx >= 0) {
            return description.substring(idx + prefix.length()).trim();
        }
        return null;
    }

    public RegisterNodeResponse registerNode(String nodeId, String address, String zone) {
        return execute(stub -> stub.registerNode(RegisterNodeRequest.newBuilder()
                .setNodeId(nodeId)
                .setAddress(address)
                .setZone(zone != null ? zone : "")
                .build()));
    }

    public InitShardsResponse initShards(int numShards, int replicationFactor) {
        return execute(stub -> stub.initShards(InitShardsRequest.newBuilder()
                .setNumShards(numShards)
                .setReplicationFactor(replicationFactor)
                .build()));
    }

    public SetNodeStatusResponse setNodeStatus(String nodeId, String status) {
        return execute(stub -> stub.setNodeStatus(SetNodeStatusRequest.newBuilder()
                .setNodeId(nodeId)
                .setStatus(NodeStatus.valueOf(status))
                .build()));
    }

    public SetShardReplicasResponse setShardReplicas(String shardId, List<String> replicaNodeIds) {
        return execute(stub -> stub.setShardReplicas(SetShardReplicasRequest.newBuilder()
                .setShardId(shardId)
                .addAllReplicas(replicaNodeIds)
                .build()));
    }

    public SetShardLeaderResponse setShardLeader(String shardId, long epoch, String leaderNodeId) {
        return execute(stub -> stub.setShardLeader(SetShardLeaderRequest.newBuilder()
                .setShardId(shardId)
                .setEpoch(epoch)
                .setLeaderNodeId(leaderNodeId)
                .build()));
    }

    public String getLeaderAddress() {
        return leaderAddress.get();
    }

    public void shutdown() {
        channels.values().forEach(ch -> {
            try {
                ch.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                ch.shutdownNow();
                Thread.currentThread().interrupt();
            }
        });
        channels.clear();
        stubs.clear();
    }
}
