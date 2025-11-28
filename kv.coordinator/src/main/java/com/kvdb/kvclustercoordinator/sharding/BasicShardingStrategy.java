package com.kvdb.kvclustercoordinator.sharding;

import com.kvdb.kvclustercoordinator.cluster.ClusterNode;
import com.kvdb.kvcommon.exception.NoHealthyNodesAvailable;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class BasicShardingStrategy implements ShardingStrategy {

    /**
     * Used for round-robin selection. AtomicInteger makes this strategy safe to use from multiple
     * threads concurrently.
     */
    private final AtomicInteger nextIndex = new AtomicInteger(0);

    @Override
    public ClusterNode getShardWithKey(String key, List<ClusterNode> nodes) {
        if (nodes == null || nodes.isEmpty()) {
            throw new NoHealthyNodesAvailable("No nodes available for sharding");
        }

        int size = nodes.size();
        int hash = key != null ? key.hashCode() : 0;

        // floorMod handles negative hashes correctly.
        int shardIndex = Math.floorMod(hash, size);
        return nodes.get(shardIndex);
    }

    @Override
    public ClusterNode getShard(List<ClusterNode> nodes) {
        if (nodes == null || nodes.isEmpty()) {
            throw new NoHealthyNodesAvailable("No nodes available for round-robin");
        }

        int size = nodes.size();
        int idx = Math.floorMod(nextIndex.getAndIncrement(), size);
        return nodes.get(idx);
    }

    @Override
    public ClusterNode getNextHealthyShard(List<ClusterNode> nodes) {
        if (nodes == null || nodes.isEmpty()) {
            throw new NoHealthyNodesAvailable("No nodes available for healthy selection");
        }

        int size = nodes.size();
        int start = nextIndex.getAndIncrement(); // starting point for this selection

        // Try each node at most once
        for (int i = 0; i < size; i++) {
            int idx = Math.floorMod(start + i, size);
            ClusterNode candidate = nodes.get(idx);
            if (candidate.isRunning()) {
                return candidate;
            }
        }

        throw new NoHealthyNodesAvailable("No healthy nodes available");
    }
}
