package com.kvdb.kvclustercoordinator.sharding;

import com.kvdb.kvclustercoordinator.cluster.ClusterNode;
import com.kvdb.kvcommon.exception.NoHealthyNodesAvailable;

import java.util.List;

public class BasicShardingStrategy implements ShardingStrategy {

    int shardIdx = 0;

    @Override
    public ClusterNode getShardWithKey(String key, List<ClusterNode> nodes) {
        // modulo-based sharding
        int shardIndex = Math.abs(key.hashCode()) % nodes.size();
        return nodes.get(shardIndex);
    }

    @Override
    public ClusterNode getShard(List<ClusterNode> nodes) {
        // simple round-robin selection
        shardIdx = (shardIdx + 1) % nodes.size();
        return nodes.get(shardIdx);
    }

    @Override
    public ClusterNode getNextHealthyShard(List<ClusterNode> nodes) {
        // Round-robin selection of next healthy node
        int limit = nodes.size();
        while (!nodes.get(shardIdx).isRunning() && limit-- > 0) {
            shardIdx = (shardIdx + 1) % nodes.size();
        }
        if (limit <= 0) {
            throw new NoHealthyNodesAvailable("Oopsies, no healthy nodes :(");
        }
        return nodes.get(shardIdx);
    }
}
