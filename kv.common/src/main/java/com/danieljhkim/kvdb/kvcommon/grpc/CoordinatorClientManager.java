package com.danieljhkim.kvdb.kvcommon.grpc;

import com.danieljhkim.kvdb.kvcommon.config.AppConfig;
import com.danieljhkim.kvdb.proto.coordinator.ClusterState;
import com.danieljhkim.kvdb.proto.coordinator.CoordinatorGrpc;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Manages connections to multiple coordinator nodes with leader-aware failover.
 * Provides automatic retry with leader rediscovery for all coordinator operations.
 */
public class CoordinatorClientManager {

    private static final Logger log = LoggerFactory.getLogger(CoordinatorClientManager.class);
    private static final int MAX_RETRIES = 3;

    private final Map<String, CoordinatorClient> clients = new ConcurrentHashMap<>();
    private final AtomicReference<String> leaderId = new AtomicReference<>(null);
    private final List<String> nodeIds = new ArrayList<>();

    public CoordinatorClientManager(AppConfig appConfig) {
        AppConfig.NodeGroupConfig coordinatorNodes = appConfig.getCoordinatorNodes();
        if (coordinatorNodes == null
                || coordinatorNodes.getNodes() == null
                || coordinatorNodes.getNodes().isEmpty()) {
            throw new IllegalArgumentException("No coordinator nodes configured");
        }

        for (AppConfig.NodeConfig nodeConfig : coordinatorNodes.getNodes()) {
            String nodeId = nodeConfig.getId();
            String host = nodeConfig.getHost();
            int port = nodeConfig.getPort();
            CoordinatorClient client = new CoordinatorClient(host, port);
            clients.put(nodeId, client);
            nodeIds.add(nodeId);
        }

        log.info("Created coordinator client manager with {} nodes", clients.size());
    }

    /**
     * Executes an operation on the leader coordinator with automatic retry and leader rediscovery.
     *
     * @param operation the operation to execute on the coordinator stub
     * @param <T> the response type
     * @return the response from the coordinator
     * @throws StatusRuntimeException if all retries fail
     */
    public <T> T execute(Function<CoordinatorGrpc.CoordinatorBlockingStub, T> operation) {
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            CoordinatorClient client = getLeaderClient();
            if (client == null) {
                log.warn("No leader client available, attempt {}/{}", attempt + 1, MAX_RETRIES);
                continue;
            }
            try {
                return operation.apply(client.getBlockingStub());
            } catch (StatusRuntimeException e) {
                Status.Code code = e.getStatus().getCode();
                log.warn("Request failed on {} ({}), attempt {}/{}", leaderId.get(), code, attempt + 1, MAX_RETRIES);

                if (code == Status.Code.FAILED_PRECONDITION || code == Status.Code.UNAVAILABLE) {
                    // Clear leader and rediscover on next attempt
                    leaderId.set(null);
                    continue;
                }
                throw e;
            }
        }
        throw new StatusRuntimeException(
                Status.UNAVAILABLE.withDescription("Failed after " + MAX_RETRIES + " attempts"));
    }

    /**
     * Fetches the shard map from the coordinator cluster.
     */
    public ClusterState fetchShardMap(long ifVersionGt) {
        try {
            var response = execute(
                    stub -> stub.getShardMap(com.danieljhkim.kvdb.proto.coordinator.GetShardMapRequest.newBuilder()
                            .setIfVersionGt(ifVersionGt)
                            .build()));
            return response.getNotModified() ? null : response.getState();
        } catch (StatusRuntimeException e) {
            log.warn("Failed to fetch shard map: {}", e.getStatus());
            return null;
        }
    }

    /**
     * Gets the leader client, discovering it if necessary.
     */
    @SuppressWarnings("all")
    public CoordinatorClient getLeaderClient() {
        String leader = leaderId.get();
        if (leader != null && clients.containsKey(leader)) {
            // Verify cached leader is still the leader
            CoordinatorClient cachedClient = clients.get(leader);
            if (verifyIsLeader(cachedClient)) {
                return cachedClient;
            }
            log.info("Cached leader {} is no longer leader, rediscovering", leader);
            leaderId.set(null);
        }

        // Discover leader by querying each coordinator
        // First pass: find a node that claims to be the leader
        for (String nodeId : nodeIds) {
            CoordinatorClient client = clients.get(nodeId);
            try {
                var response = client.getBlockingStub()
                        .getCoordinatorLeader(
                                com.danieljhkim.kvdb.proto.coordinator.GetCoordinatorLeaderRequest
                                        .getDefaultInstance());

                log.debug(
                        "Queried {}: isLeader={}, leaderId={}, leaderAddress={}",
                        nodeId,
                        response.getIsLeader(),
                        response.getLeaderId(),
                        response.getLeaderAddress());

                // If this node claims to be the leader, use it
                if (response.getIsLeader()) {
                    updateLeader(nodeId);
                    return client;
                }
            } catch (StatusRuntimeException e) {
                log.debug("Failed to query {}: {}", nodeId, e.getStatus());
            }
        }

        // Second pass: no node claims to be leader, try hints
        for (String nodeId : nodeIds) {
            CoordinatorClient client = clients.get(nodeId);
            try {
                var response = client.getBlockingStub()
                        .getCoordinatorLeader(
                                com.danieljhkim.kvdb.proto.coordinator.GetCoordinatorLeaderRequest
                                        .getDefaultInstance());

                String hintedLeader = response.getLeaderId();
                if (hintedLeader != null && !hintedLeader.isEmpty() && clients.containsKey(hintedLeader)) {
                    CoordinatorClient hintedClient = clients.get(hintedLeader);
                    if (verifyIsLeader(hintedClient)) {
                        log.info("Verified leader {} via hint from {}", hintedLeader, nodeId);
                        updateLeader(hintedLeader);
                        return hintedClient;
                    }
                }
            } catch (StatusRuntimeException e) {
                // Already logged in first pass
            }
        }

        log.warn("Could not discover leader from any coordinator");
        return null;
    }

    /**
     * Verifies that a client's coordinator actually thinks it's the leader.
     */
    private boolean verifyIsLeader(CoordinatorClient client) {
        try {
            var response = client.getBlockingStub()
                    .getCoordinatorLeader(
                            com.danieljhkim.kvdb.proto.coordinator.GetCoordinatorLeaderRequest.getDefaultInstance());
            return response.getIsLeader();
        } catch (StatusRuntimeException e) {
            log.debug("Failed to verify leader: {}", e.getStatus());
            return false;
        }
    }

    private void updateLeader(String nodeId) {
        String oldLeader = leaderId.getAndSet(nodeId);
        if (!nodeId.equals(oldLeader)) {
            log.info("Updated coordinator leader: {} -> {}", oldLeader, nodeId);
        }
    }

    public String getLeaderId() {
        return leaderId.get();
    }

    /**
     * Clears the cached leader ID, forcing rediscovery on next getLeaderClient() call.
     */
    public void clearCachedLeader() {
        String old = leaderId.getAndSet(null);
        if (old != null) {
            log.info("Cleared cached leader: {}", old);
        }
    }

    public CoordinatorClient getClient(String nodeId) {
        return clients.get(nodeId);
    }

    public void shutdown() {
        log.info("Shutting down coordinator client manager");
        clients.values().forEach(client -> {
            try {
                client.shutdown();
            } catch (Exception e) {
                log.warn("Error shutting down client", e);
            }
        });
        clients.clear();
        nodeIds.clear();
    }
}
