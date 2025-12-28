package com.danieljhkim.kvdb.kvgateway.cache;

import com.danieljhkim.kvdb.kvgateway.client.CoordinatorClient;
import com.danieljhkim.kvdb.proto.coordinator.ClusterState;
import com.danieljhkim.kvdb.proto.coordinator.NodeRecord;
import com.danieljhkim.kvdb.proto.coordinator.NodeStatus;
import com.danieljhkim.kvdb.proto.coordinator.ShardRecord;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

/**
 * Caches the cluster shard map from the coordinator.
 * Provides thread-safe access and shard resolution for keys.
 */
public class ShardMapCache {

	private static final Logger LOGGER = Logger.getLogger(ShardMapCache.class.getName());

	private final CoordinatorClient coordinatorClient;
	private final AtomicReference<ClusterState> cachedState = new AtomicReference<>();
	/**
	 * -- GETTER --
	 * Gets the last refresh timestamp.
	 *
	 * @return The last refresh time in milliseconds
	 */
	@Getter
	private volatile long lastRefreshTime = 0;

	public ShardMapCache(CoordinatorClient coordinatorClient) {
		this.coordinatorClient = coordinatorClient;
	}

	/**
	 * Refreshes the shard map from the coordinator.
	 *
	 * @return true if the map was updated, false otherwise
	 */
	public boolean refresh() {
		LOGGER.info("Refreshing shard map from coordinator");
		ClusterState newState = coordinatorClient.fetchShardMap();
		if (newState != null) {
			ClusterState oldState = cachedState.get();
			if (oldState == null || newState.getMapVersion() > oldState.getMapVersion()) {
				cachedState.set(newState);
				lastRefreshTime = System.currentTimeMillis();
				LOGGER.info("Shard map updated to version " + newState.getMapVersion()
						+ " with " + newState.getShardsCount() + " shards and "
						+ newState.getNodesCount() + " nodes");
				return true;
			}
		}
		return false;
	}

	/**
	 * Resolves a key to a shard ID using hash-based sharding.
	 *
	 * @param key
	 *            The key bytes
	 * @return The shard ID for this key
	 * @throws IllegalStateException
	 *             if no shard map is cached
	 */
	public String resolveShardId(byte[] key) {
		ClusterState state = cachedState.get();
		if (state == null || state.getShardsCount() == 0) {
			throw new IllegalStateException("No shard map available");
		}

		int numShards = state.getPartitioning().getNumShards();
		if (numShards == 0) {
			numShards = state.getShardsCount();
		}

		// Hash the key and map to a shard index
		int hash = hashKey(key);
		int shardIndex = Math.floorMod(hash, numShards);

		// Shard IDs are formatted as "shard-N"
		return "shard-" + shardIndex;
	}

	/**
	 * Gets the shard record for a given shard ID.
	 *
	 * @param shardId
	 *            The shard ID
	 * @return The shard record, or null if not found
	 */
	public ShardRecord getShard(String shardId) {
		ClusterState state = cachedState.get();
		if (state == null) {
			return null;
		}
		return state.getShardsMap().get(shardId);
	}

	/**
	 * Gets the leader node for a shard.
	 *
	 * @param shardId
	 *            The shard ID
	 * @return The leader node record, or null if not found
	 */
	public NodeRecord getLeader(String shardId) {
		ClusterState state = cachedState.get();
		if (state == null) {
			return null;
		}

		ShardRecord shard = state.getShardsMap().get(shardId);
		if (shard == null || shard.getLeader().isEmpty()) {
			return null;
		}

		return state.getNodesMap().get(shard.getLeader());
	}

	/**
	 * Gets all replica nodes for a shard.
	 *
	 * @param shardId
	 *            The shard ID
	 * @return List of replica node records
	 */
	public List<NodeRecord> getReplicas(String shardId) {
		ClusterState state = cachedState.get();
		if (state == null) {
			return List.of();
		}

		ShardRecord shard = state.getShardsMap().get(shardId);
		if (shard == null) {
			return List.of();
		}

		List<NodeRecord> replicas = new ArrayList<>();
		for (String nodeId : shard.getReplicasList()) {
			NodeRecord node = state.getNodesMap().get(nodeId);
			if (node != null) {
				replicas.add(node);
			}
		}
		return replicas;
	}

	/**
	 * Gets a healthy replica for a shard (for eventual consistency reads).
	 *
	 * @param shardId
	 *            The shard ID
	 * @return A healthy replica node, or null if none available
	 */
	public NodeRecord getHealthyReplica(String shardId) {
		List<NodeRecord> replicas = getReplicas(shardId);
		for (NodeRecord replica : replicas) {
			if (replica.getStatus() == NodeStatus.ALIVE) {
				return replica;
			}
		}
		return null;
	}

	/**
	 * Gets the current map version.
	 *
	 * @return The map version, or 0 if no map is cached
	 */
	public long getMapVersion() {
		ClusterState state = cachedState.get();
		return state != null ? state.getMapVersion() : 0;
	}

	/**
	 * Checks if the cache has a valid shard map.
	 *
	 * @return true if a shard map is cached
	 */
	public boolean isInitialized() {
		return cachedState.get() != null;
	}

	/**
	 * Gets a node record by ID.
	 *
	 * @param nodeId
	 *            The node ID
	 * @return The node record, or null if not found
	 */
	public NodeRecord getNode(String nodeId) {
		ClusterState state = cachedState.get();
		if (state == null) {
			return null;
		}
		return state.getNodesMap().get(nodeId);
	}

	private int hashKey(byte[] key) {
		if (key == null || key.length == 0) {
			return 0;
		}
		// Simple hash using Arrays.hashCode equivalent
		int result = 1;
		for (byte b : key) {
			result = 31 * result + b;
		}
		return result;
	}
}
