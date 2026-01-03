package com.danieljhkim.kvdb.kvnode.cluster;

import com.danieljhkim.kvdb.kvcommon.exception.NodeUnavailableException;
import com.danieljhkim.kvdb.kvnode.cache.ShardMapCache;
import com.danieljhkim.kvdb.kvnode.client.ReplicaWriteClient;
import com.danieljhkim.kvdb.proto.coordinator.ShardRecord;
import com.kvdb.proto.kvstore.ReplicateDeleteRequest;
import com.kvdb.proto.kvstore.ReplicateSetRequest;
import com.kvdb.proto.kvstore.SetResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages quorum-based replication to replica nodes.
 * Handles parallel replication with timeout and quorum enforcement.
 */
public class ReplicationManager {

    private static final Logger log = LoggerFactory.getLogger(ReplicationManager.class);

    private final String nodeId;
    private final ShardMapCache shardMapCache;
    private final ReplicaWriteClient replicaWriteClient;
    private final Duration replicationTimeout;

    public ReplicationManager(
            String nodeId,
            ShardMapCache shardMapCache,
            ReplicaWriteClient replicaWriteClient,
            Duration replicationTimeout) {
        this.nodeId = nodeId;
        this.shardMapCache = shardMapCache;
        this.replicaWriteClient = replicaWriteClient;
        this.replicationTimeout = replicationTimeout != null ? replicationTimeout : Duration.ofMillis(500);
    }

    /**
     * Replicates a SET operation to replica nodes.
     * Requires quorum (majority) acknowledgment.
     *
     * @param shardId the shard ID
     * @param shard the shard record containing replica list
     * @param key the key to set
     * @param value the value to set
     * @throws NodeUnavailableException if quorum is not reached
     */
    public void replicateSet(String shardId, ShardRecord shard, String key, String value) {
        if (replicaWriteClient == null) {
            log.debug("No replica write client configured, skipping replication");
            return;
        }

        List<String> targets = getReplicationTargets(shard);
        if (targets.isEmpty()) {
            log.debug("No replication targets for shard {}, skipping replication", shardId);
            return;
        }

        int totalReplicas = shard.getReplicasList().size();
        int requiredAcks = calculateQuorum(totalReplicas);

        ReplicateSetRequest request = ReplicateSetRequest.newBuilder()
                .setShardId(shardId)
                .setEpoch(shard.getEpoch())
                .setKey(key)
                .setValue(value)
                .setOriginNodeId(nodeId)
                .build();

        int acks = executeReplication(targets, requiredAcks, replicaTarget -> {
            SetResponse response = replicaWriteClient.replicateSet(replicaTarget, request);
            return response.getSuccess();
        });

        if (acks < requiredAcks) {
            throw new NodeUnavailableException(
                    String.format(
                            "Replication quorum not reached for shard %s (acks=%d, required=%d)",
                            shardId, acks, requiredAcks),
                    shardId);
        }

        log.debug("Successfully replicated SET to {} replicas for shard {} (quorum={})", acks, shardId, requiredAcks);
    }

    /**
     * Replicates a DELETE operation to replica nodes.
     * Requires quorum (majority) acknowledgment.
     *
     * @param shardId the shard ID
     * @param shard the shard record containing replica list
     * @param key the key to delete
     * @throws NodeUnavailableException if quorum is not reached
     */
    public void replicateDelete(String shardId, ShardRecord shard, String key) {
        if (replicaWriteClient == null) {
            log.debug("No replica write client configured, skipping replication");
            return;
        }

        List<String> targets = getReplicationTargets(shard);
        if (targets.isEmpty()) {
            log.debug("No replication targets for shard {}, skipping replication", shardId);
            return;
        }

        int totalReplicas = shard.getReplicasList().size();
        int requiredAcks = calculateQuorum(totalReplicas);

        ReplicateDeleteRequest request = ReplicateDeleteRequest.newBuilder()
                .setShardId(shardId)
                .setEpoch(shard.getEpoch())
                .setKey(key)
                .setOriginNodeId(nodeId)
                .build();

        int acks = executeReplication(targets, requiredAcks, replicaTarget -> {
            replicaWriteClient.replicateDelete(replicaTarget, request);
            return true; // Delete always returns success
        });

        if (acks < requiredAcks) {
            throw new NodeUnavailableException(
                    String.format(
                            "Replication quorum not reached for shard %s (acks=%d, required=%d)",
                            shardId, acks, requiredAcks),
                    shardId);
        }

        log.debug(
                "Successfully replicated DELETE to {} replicas for shard {} (quorum={})", acks, shardId, requiredAcks);
    }

    /**
     * Gets the list of replication target addresses (excluding this node).
     */
    private List<String> getReplicationTargets(ShardRecord shard) {
        List<String> targets = new ArrayList<>();
        for (String replicaId : shard.getReplicasList()) {
            if (replicaId == null || replicaId.isEmpty() || replicaId.equals(nodeId)) {
                continue;
            }
            String address = shardMapCache.getNodeAddress(replicaId).orElse("");
            if (!address.isEmpty()) {
                targets.add(address);
            }
        }
        return targets;
    }

    /**
     * Calculates quorum size (majority).
     * Includes local write in the count (hence +1 before division).
     */
    private int calculateQuorum(int totalReplicas) {
        return (totalReplicas / 2) + 1;
    }

    /**
     * Executes replication to multiple targets in parallel.
     * Returns the number of successful acknowledgments (including local write).
     */
    private int executeReplication(List<String> targets, int requiredAcks, ReplicationTask task) {

        AtomicInteger acks = new AtomicInteger(1); // Count local write
        List<Thread> threads = new ArrayList<>(targets.size());

        for (String target : targets) {
            threads.add(Thread.startVirtualThread(() -> {
                try {
                    boolean success = task.execute(target);
                    if (success) {
                        acks.incrementAndGet();
                    }
                } catch (Throwable t) {
                    log.warn("Replication to {} failed: {}", target, t.getMessage());
                    // Count as failure, don't propagate exception
                }
            }));
        }

        // Wait for threads with timeout
        long deadlineMs = System.currentTimeMillis() + replicationTimeout.toMillis();
        for (Thread thread : threads) {
            if (acks.get() >= requiredAcks) {
                // Early exit if quorum reached
                return acks.get();
            }

            long remaining = deadlineMs - System.currentTimeMillis();
            if (remaining <= 0) {
                break;
            }

            try {
                thread.join(remaining);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        return acks.get();
    }

    @FunctionalInterface
    private interface ReplicationTask {
        boolean execute(String target) throws Exception;
    }
}
