package com.danieljhkim.kvdb.kvgateway.cache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks per-shard routing failures (e.g., NOT_LEADER) for a short TTL to avoid thrashing on a stale leader hint.
 */
public class ShardRoutingFailureTracker {

    private final Map<String, Long> shardNotLeaderAtMs = new ConcurrentHashMap<>();
    private final long ttlMs;

    public ShardRoutingFailureTracker() {
        this(3000);
    }

    public ShardRoutingFailureTracker(long ttlMs) {
        this.ttlMs = ttlMs;
    }

    public void recordNotLeader(String shardId) {
        if (shardId == null || shardId.isEmpty()) {
            return;
        }
        shardNotLeaderAtMs.put(shardId, System.currentTimeMillis());
    }

    public boolean isRecentlyNotLeader(String shardId) {
        if (shardId == null || shardId.isEmpty()) {
            return false;
        }
        Long ts = shardNotLeaderAtMs.get(shardId);
        if (ts == null) {
            return false;
        }
        long now = System.currentTimeMillis();
        if (now - ts > ttlMs) {
            shardNotLeaderAtMs.remove(shardId, ts);
            return false;
        }
        return true;
    }

    public void clear(String shardId) {
        if (shardId == null || shardId.isEmpty()) {
            return;
        }
        shardNotLeaderAtMs.remove(shardId);
    }

    public void clearAll() {
        shardNotLeaderAtMs.clear();
    }
}
