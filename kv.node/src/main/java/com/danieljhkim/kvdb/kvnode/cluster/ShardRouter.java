package com.danieljhkim.kvdb.kvnode.cluster;

import com.danieljhkim.kvdb.kvcommon.cache.ShardMapCache;
import com.danieljhkim.kvdb.kvcommon.exception.InvalidRequestException;
import com.danieljhkim.kvdb.kvcommon.exception.ShardMapUnavailableException;
import com.danieljhkim.kvdb.kvcommon.exception.ShardMovedException;
import com.danieljhkim.kvdb.proto.coordinator.ShardRecord;
import java.nio.charset.StandardCharsets;

/**
 * Handles shard resolution and routing logic.
 * Determines which shard a key belongs to and validates if this node can serve the request.
 */
public class ShardRouter {

    private final ShardMapCache shardMapCache;
    private final String nodeId;

    public ShardRouter(ShardMapCache shardMapCache, String nodeId) {
        this.shardMapCache = shardMapCache;
        this.nodeId = nodeId;
    }

    /**
     * Resolves the shard ID for a given key.
     *
     * @param key the key to resolve
     * @return the shard ID
     * @throws ShardMapUnavailableException if shard map is not available
     */
    public String resolveShardId(String key) {
        validateShardMapAvailable();
        return shardMapCache.resolveShardId(key.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Gets the shard record for a given shard ID.
     *
     * @param shardId the shard ID
     * @return the shard record
     * @throws ShardMapUnavailableException if shard not found
     */
    public ShardRecord getShardRecord(String shardId) {
        validateShardMapAvailable();
        ShardRecord shard = shardMapCache.getShard(shardId);
        if (shard == null) {
            throw new ShardMapUnavailableException("Shard not found in shard map: " + shardId);
        }
        return shard;
    }

    /**
     * Validates that this node is a replica for the given shard.
     * Throws ShardMovedException if not.
     *
     * @param shardId the shard ID
     * @throws ShardMovedException if this node is not a replica
     */
    public void validateReplica(String shardId) {
        validateShardMapAvailable();
        if (!shardMapCache.isReplica(shardId, nodeId)) {
            String hint = getRedirectHint(shardId);
            throw new ShardMovedException(shardId, hint);
        }
    }

    /**
     * Validates that the request's shard ID matches the key's resolved shard.
     *
     * @param key the key
     * @param requestShardId the shard ID from the request
     * @throws InvalidRequestException if they don't match
     */
    public void validateShardIdForKey(String key, String requestShardId) {
        String expectedShard = resolveShardId(key);
        if (!expectedShard.equals(requestShardId)) {
            throw new InvalidRequestException("shard_id does not match key's resolved shard");
        }
    }

    /**
     * Validates that the epoch matches the current shard epoch.
     *
     * @param shardId the shard ID
     * @param requestEpoch the epoch from the request
     * @throws ShardMovedException if epochs don't match
     */
    public void validateEpoch(String shardId, long requestEpoch) {
        ShardRecord shard = getShardRecord(shardId);
        if (requestEpoch != 0 && shard.getEpoch() != 0 && shard.getEpoch() != requestEpoch) {
            String hint = getRedirectHint(shardId);
            throw new ShardMovedException(shardId, hint);
        }
    }

    /**
     * Gets a redirect hint (address) for a shard.
     * Prefers leader address, falls back to any replica.
     *
     * @param shardId the shard ID
     * @return the redirect address hint
     */
    public String getRedirectHint(String shardId) {
        return shardMapCache
                .getLeaderAddress(shardId)
                .or(() -> shardMapCache.getAnyReplicaAddress(shardId))
                .orElse("");
    }

    /**
     * Checks if this node is a replica for the given shard.
     *
     * @param shardId the shard ID
     * @return true if this node is a replica
     */
    public boolean isReplica(String shardId) {
        if (shardMapCache == null || !shardMapCache.isInitialized()) {
            return false;
        }
        return shardMapCache.isReplica(shardId, nodeId);
    }

    private void validateShardMapAvailable() {
        if (shardMapCache == null || !shardMapCache.isInitialized()) {
            throw new ShardMapUnavailableException("Shard map not initialized");
        }
    }
}
