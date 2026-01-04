package com.danieljhkim.kvdb.kvnode.cluster;

import com.danieljhkim.kvdb.kvcommon.cache.ShardMapCache;
import com.danieljhkim.kvdb.kvcommon.exception.NotLeaderException;
import com.danieljhkim.kvdb.kvcommon.exception.ShardMovedException;

/**
 * Validates leadership and replica membership for shard operations.
 * Determines if this node can serve read/write requests for a given shard.
 */
public class ShardLeadershipValidator {

    private final ShardMapCache shardMapCache;
    private final String nodeId;

    public ShardLeadershipValidator(ShardMapCache shardMapCache, String nodeId) {
        this.shardMapCache = shardMapCache;
        this.nodeId = nodeId;
    }

    /**
     * Validates that this node can serve a write request for the given shard.
     * Checks:
     * 1. Node is a replica for the shard
     * 2. Node is the leader for the shard
     *
     * @param shardId the shard ID
     * @throws ShardMovedException if node is not a replica
     * @throws NotLeaderException if node is not the leader
     */
    public void validateWriteLeadership(String shardId) {
        // Check if we're a replica
        if (!shardMapCache.isReplica(shardId, nodeId)) {
            String hint = getRedirectHint(shardId);
            throw new ShardMovedException(shardId, hint);
        }

        // Check if we're the leader
        String leaderNodeId = shardMapCache.getLeaderNodeId(shardId).orElse("");
        if (!leaderNodeId.isEmpty() && !leaderNodeId.equals(nodeId)) {
            throw new NotLeaderException(getLeaderAddress(shardId));
        }
    }

    /**
     * Validates that this node can serve a read request for the given shard.
     * Only checks replica membership (reads can go to any replica).
     *
     * @param shardId the shard ID
     * @throws ShardMovedException if node is not a replica
     */
    public void validateReadReplica(String shardId) {
        if (!shardMapCache.isReplica(shardId, nodeId)) {
            String hint = getRedirectHint(shardId);
            throw new ShardMovedException(shardId, hint);
        }
    }

    /**
     * Validates that this node can accept a replication request for the given shard.
     * Checks replica membership (not leadership, since replicas accept writes from leader).
     *
     * @param shardId the shard ID
     * @throws ShardMovedException if node is not a replica
     */
    public void validateReplicaTarget(String shardId) {
        if (!shardMapCache.isReplica(shardId, nodeId)) {
            String hint = getRedirectHint(shardId);
            throw new ShardMovedException(shardId, hint);
        }
    }

    /**
     * Checks if this node is the leader for a given shard.
     *
     * @param shardId the shard ID
     * @return true if this node is the leader
     */
    public boolean isLeader(String shardId) {
        String leaderNodeId = shardMapCache.getLeaderNodeId(shardId).orElse("");
        return !leaderNodeId.isEmpty() && leaderNodeId.equals(nodeId);
    }

    /**
     * Gets the leader address for a given shard.
     *
     * @param shardId the shard ID
     * @return the leader address, or empty string if not known
     */
    public String getLeaderAddress(String shardId) {
        return shardMapCache.getLeaderAddress(shardId).orElse("");
    }

    private String getRedirectHint(String shardId) {
        return shardMapCache
                .getLeaderAddress(shardId)
                .or(() -> shardMapCache.getAnyReplicaAddress(shardId))
                .orElse("");
    }
}
