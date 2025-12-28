package com.danieljhkim.kvdb.kvclustercoordinator.sharding;

import com.danieljhkim.kvdb.kvclustercoordinator.cluster.ClusterNode;
import com.danieljhkim.kvdb.kvcommon.exception.NoHealthyNodesAvailable;

import java.util.List;

public interface ShardingStrategy {
	/**
	 * Determines the shard for a given key.
	 *
	 * @param key
	 *            the key to be sharded
	 * @return the shard
	 */
	ClusterNode getShardWithKey(String key, List<ClusterNode> nodes);

	ClusterNode getShard(List<ClusterNode> nodes);

	ClusterNode getNextHealthyShard(List<ClusterNode> nodes) throws NoHealthyNodesAvailable;
}
