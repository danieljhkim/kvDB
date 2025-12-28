package com.danieljhkim.kvdb.kvgateway.cache;

import com.danieljhkim.kvdb.kvgateway.client.CoordinatorClient;
import com.danieljhkim.kvdb.proto.coordinator.*;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.logging.Logger;

/**
 * Caches the cluster shard map from the coordinator.
 * Provides thread-safe access and shard resolution for keys.
 * Supports both polling (refresh) and streaming (delta updates).
 */
public class ShardMapCache implements Consumer<ShardMapDelta> {

	private static final Logger LOGGER = Logger.getLogger(ShardMapCache.class.getName());

	/** Minimum interval between refresh attempts to avoid thrashing */
	private static final long MIN_REFRESH_INTERVAL_MS = 5000;

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

	/**
	 * -- GETTER --
	 * Gets the last delta update timestamp.
	 *
	 * @return The last delta update time in milliseconds
	 */
	@Getter
	private volatile long lastDeltaUpdateTime = 0;

	public ShardMapCache(CoordinatorClient coordinatorClient) {
		this.coordinatorClient = coordinatorClient;
	}

	/**
	 * Refreshes the shard map from the coordinator via polling.
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
	 * Consumer interface implementation for receiving delta updates.
	 * Called by WatchShardMapClient when deltas are received.
	 */
	@Override
	public void accept(ShardMapDelta delta) {
		updateFromDelta(delta);
	}

	/**
	 * Updates the cache from a streaming delta update.
	 * Handles both full state updates and incremental deltas.
	 *
	 * @param delta
	 *            The delta received from the coordinator
	 * @return true if the cache was updated, false otherwise
	 */
	public boolean updateFromDelta(ShardMapDelta delta) {
		if (delta == null) {
			return false;
		}

		long newVersion = delta.getNewMapVersion();

		// Version 0 is a heartbeat, ignore
		if (newVersion == 0) {
			LOGGER.fine("Ignoring heartbeat delta");
			return false;
		}

		ClusterState oldState = cachedState.get();
		long currentVersion = oldState != null ? oldState.getMapVersion() : 0;

		// Only apply if newer
		if (newVersion <= currentVersion) {
			LOGGER.fine("Ignoring stale delta (version=" + newVersion + ", current=" + currentVersion + ")");
			return false;
		}

		// Check if this is a full state update
		if (delta.hasFullState() && delta.getFullState().getMapVersion() > 0) {
			ClusterState fullState = delta.getFullState();
			cachedState.set(fullState);
			lastDeltaUpdateTime = System.currentTimeMillis();
			LOGGER.info("Shard map updated from delta (full state) to version " + fullState.getMapVersion()
					+ " with " + fullState.getShardsCount() + " shards and "
					+ fullState.getNodesCount() + " nodes");
			return true;
		}

		// Incremental delta - we need to merge changes
		// For now, if we receive an incremental delta without full state,
		// trigger a full refresh to ensure consistency
		if (!delta.getChangedShardsList().isEmpty() || !delta.getChangedNodesList().isEmpty()) {
			LOGGER.info("Received incremental delta for version " + newVersion
					+ " (changedShards=" + delta.getChangedShardsCount()
					+ ", changedNodes=" + delta.getChangedNodesCount()
					+ "). Triggering full refresh for consistency.");

			// Schedule async refresh to avoid blocking the stream handler
			// For now, do a synchronous refresh
			boolean refreshed = refresh();
			if (refreshed) {
				lastDeltaUpdateTime = System.currentTimeMillis();
			}
			return refreshed;
		}

		return false;
	}

	/**
	 * Schedules an asynchronous refresh of the shard map.
	 * Useful when we detect staleness but don't want to block.
	 */
	public void scheduleRefresh() {
		Thread.startVirtualThread(() -> {
			try {
				refresh();
			} catch (Exception e) {
				LOGGER.warning("Async refresh failed: " + e.getMessage());
			}
		});
	}

	/**
	 * Schedules a refresh only if enough time has passed since the last refresh.
	 * This prevents thrashing when multiple errors occur in quick succession.
	 */
	public void scheduleRefreshIfStale() {
		long now = System.currentTimeMillis();
		long timeSinceLastRefresh = now - lastRefreshTime;
		long timeSinceLastDelta = now - lastDeltaUpdateTime;

		// Only refresh if we haven't refreshed recently
		if (timeSinceLastRefresh > MIN_REFRESH_INTERVAL_MS
				&& timeSinceLastDelta > MIN_REFRESH_INTERVAL_MS) {
			LOGGER.fine("Scheduling refresh (timeSinceLastRefresh="
					+ timeSinceLastRefresh + "ms, timeSinceLastDelta=" + timeSinceLastDelta + "ms)");
			scheduleRefresh();
		} else {
			LOGGER.fine("Skipping refresh (too recent: timeSinceLastRefresh="
					+ timeSinceLastRefresh + "ms, timeSinceLastDelta=" + timeSinceLastDelta + "ms)");
		}
	}

	/**
	 * Forces an asynchronous refresh of the shard map, bypassing refresh interval
	 * gating. This is intended for strong stale-routing signals (e.g.,
	 * SHARD_MOVED).
	 */
	public void forceRefreshAsync() {
		LOGGER.info("Forcing shard map refresh (bypassing refresh gating)");
		scheduleRefresh();
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
