package com.danieljhkim.kvdb.kvclustercoordinator.state;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Mutable cluster state managed by the coordinator.
 * This class is NOT thread-safe on its own - thread safety is provided by the
 * Raft state machine which serializes all mutations.
 *
 * After each mutation, a new immutable {@link ShardMapSnapshot} should be
 * created for safe concurrent reads.
 */
public class ClusterState {

	private long mapVersion;
	private final Map<String, NodeRecord> nodes;
	private final Map<String, ShardRecord> shards;
	private int numShards;
	private int replicationFactor;

	public ClusterState() {
		this.mapVersion = 0;
		this.nodes = new HashMap<>();
		this.shards = new HashMap<>();
		this.numShards = 0;
		this.replicationFactor = 1;
	}

	/**
	 * Copy constructor for creating snapshots.
	 */
	public ClusterState(ClusterState other) {
		this.mapVersion = other.mapVersion;
		this.nodes = new HashMap<>(other.nodes);
		this.shards = new HashMap<>(other.shards);
		this.numShards = other.numShards;
		this.replicationFactor = other.replicationFactor;
	}

	// ============================
	// Getters
	// ============================

	public long getMapVersion() {
		return mapVersion;
	}

	public Map<String, NodeRecord> getNodes() {
		return nodes;
	}

	public Map<String, ShardRecord> getShards() {
		return shards;
	}

	public int getNumShards() {
		return numShards;
	}

	public int getReplicationFactor() {
		return replicationFactor;
	}

	public NodeRecord getNode(String nodeId) {
		return nodes.get(nodeId);
	}

	public ShardRecord getShard(String shardId) {
		return shards.get(shardId);
	}

	// ============================
	// Mutations (increment mapVersion as needed)
	// ============================

	/**
	 * Initialize shards with the given configuration.
	 * This should only be called once when bootstrapping the cluster.
	 *
	 * @return list of created shard IDs
	 */
	public List<String> initializeShards(int numShards, int replicationFactor) {
		if (!shards.isEmpty()) {
			throw new IllegalStateException("Shards already initialized");
		}
		if (numShards <= 0) {
			throw new IllegalArgumentException("numShards must be positive");
		}

		this.numShards = numShards;
		this.replicationFactor = replicationFactor;

		List<String> nodeIds = new ArrayList<>(nodes.keySet());
		List<String> createdShards = new ArrayList<>();

		for (int i = 0; i < numShards; i++) {
			String shardId = "shard-" + i;
			List<String> replicas = assignReplicas(i, nodeIds, replicationFactor);
			ShardRecord shard = ShardRecord.create(shardId, replicas, i, numShards);
			shards.put(shardId, shard);
			createdShards.add(shardId);
		}

		mapVersion++;
		return createdShards;
	}

	/**
	 * Simple round-robin replica assignment.
	 */
	private List<String> assignReplicas(int shardIndex, List<String> nodeIds, int rf) {
		if (nodeIds.isEmpty()) {
			return List.of();
		}
		List<String> replicas = new ArrayList<>();
		for (int i = 0; i < Math.min(rf, nodeIds.size()); i++) {
			int nodeIndex = (shardIndex + i) % nodeIds.size();
			replicas.add(nodeIds.get(nodeIndex));
		}
		return replicas;
	}

	/**
	 * Register or update a node.
	 */
	public void registerNode(String nodeId, String address, String zone) {
		NodeRecord existing = nodes.get(nodeId);
		if (existing != null) {
			// Update existing node
			nodes.put(
					nodeId,
					new NodeRecord(
							nodeId,
							address,
							zone,
							existing.rack(),
							NodeRecord.NodeStatus.ALIVE,
							System.currentTimeMillis(),
							existing.capacityHints()));
		} else {
			// Create new node
			nodes.put(nodeId, NodeRecord.create(nodeId, address, zone));
		}
		// Node registration doesn't bump mapVersion by default
		// (only routing-affecting changes do)
	}

	/**
	 * Update node status. Bumps mapVersion if the change affects routing.
	 */
	public void setNodeStatus(String nodeId, NodeRecord.NodeStatus status) {
		NodeRecord node = nodes.get(nodeId);
		if (node == null) {
			throw new IllegalArgumentException("Node not found: " + nodeId);
		}

		NodeRecord.NodeStatus oldStatus = node.status();
		nodes.put(nodeId, node.withStatus(status));

		// Bump version if status change affects routing (e.g., node going DEAD)
		if (oldStatus != status
				&& (status == NodeRecord.NodeStatus.DEAD || oldStatus == NodeRecord.NodeStatus.DEAD)) {
			mapVersion++;
		}
	}

	/**
	 * Update shard replicas. Increments shard epoch and mapVersion.
	 */
	public void setShardReplicas(String shardId, List<String> newReplicas) {
		ShardRecord shard = shards.get(shardId);
		if (shard == null) {
			throw new IllegalArgumentException("Shard not found: " + shardId);
		}

		shards.put(shardId, shard.withReplicas(newReplicas));
		mapVersion++;
	}

	/**
	 * Update shard leader hint. Only valid if epoch matches.
	 */
	public void setShardLeader(String shardId, long epoch, String leaderNodeId) {
		ShardRecord shard = shards.get(shardId);
		if (shard == null) {
			throw new IllegalArgumentException("Shard not found: " + shardId);
		}

		shards.put(shardId, shard.withLeader(epoch, leaderNodeId));
		mapVersion++;
	}

	/**
	 * Update node heartbeat (non-Raft operation, doesn't bump mapVersion).
	 */
	public void updateHeartbeat(String nodeId, long timestampMs) {
		NodeRecord node = nodes.get(nodeId);
		if (node != null) {
			nodes.put(nodeId, node.withHeartbeatAlive(timestampMs));
		}
	}

	/**
	 * Creates an immutable snapshot of the current state.
	 */
	public ShardMapSnapshot createSnapshot() {
		return new ShardMapSnapshot(this);
	}
}
