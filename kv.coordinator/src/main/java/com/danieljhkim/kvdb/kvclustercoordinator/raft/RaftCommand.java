package com.danieljhkim.kvdb.kvclustercoordinator.raft;

import com.danieljhkim.kvdb.kvclustercoordinator.state.NodeRecord;

import java.util.List;

/**
 * Sealed interface representing Raft commands that modify the coordinator state machine.
 * Each command type corresponds to a specific mutation in {@link com.danieljhkim.kvdb.kvclustercoordinator.state.ClusterState}.
 *
 * <p>Commands are applied via Raft consensus to ensure consistency across coordinator replicas.
 * For the stub implementation, commands are applied synchronously without replication.
 */
public sealed

interface RaftCommand
permits RaftCommand.InitShards,RaftCommand.RegisterNode,RaftCommand.SetNodeStatus,RaftCommand.SetShardReplicas,RaftCommand.SetShardLeader
{

	/**
	 * Returns a human-readable description of this command.
	 */
	String describe();

	// ============================
	// Command Types
	// ============================

	/**
     * Initializes the shard map with the given configuration.
     * Should only be called once when bootstrapping the cluster.
     *
     * @param numShards         number of shards to create
     * @param replicationFactor number of replicas per shard
     */
    record InitShards(int numShards, int replicationFactor) implements RaftCommand {
        public InitShards {
            if (numShards <= 0) {
                throw new IllegalArgumentException("numShards must be positive");
            }
            if (replicationFactor <= 0) {
                throw new IllegalArgumentException("replicationFactor must be positive");
            }
        }

	@Override
        public String describe() {
            return "InitShards(numShards=" + numShards + ", rf=" + replicationFactor + ")";
        }}

	/**
     * Registers a new node or updates an existing node's address/zone.
     * Marks the node as ALIVE.
     *
     * @param nodeId  unique identifier for the node
     * @param address gRPC address (host:port)
     * @param zone    optional availability zone
     */
    record RegisterNode(String nodeId, String address, String zone) implements RaftCommand {
        public RegisterNode {
            if (nodeId == null || nodeId.isBlank()) {
                throw new IllegalArgumentException("nodeId cannot be null or blank");
            }
            if (address == null || address.isBlank()) {
                throw new IllegalArgumentException("address cannot be null or blank");
            }
        }

	@Override
        public String describe() {
            return "RegisterNode(nodeId=" + nodeId + ", address=" + address + ", zone=" + zone + ")";
        }}

	/**
     * Updates a node's status (ALIVE, SUSPECT, DEAD).
     * Status changes that affect routing (e.g., to/from DEAD) will bump the map version.
     *
     * @param nodeId unique identifier for the node
     * @param status new status for the node
     */
    record SetNodeStatus(String nodeId, NodeRecord.NodeStatus status) implements RaftCommand {
        public SetNodeStatus {
            if (nodeId == null || nodeId.isBlank()) {
                throw new IllegalArgumentException("nodeId cannot be null or blank");
            }
            if (status == null) {
                throw new IllegalArgumentException("status cannot be null");
            }
        }

	@Override
        public String describe() {
            return "SetNodeStatus(nodeId=" + nodeId + ", status=" + status + ")";
        }}

	/**
     * Changes the replica set for a shard.
     * This increments the shard's epoch and bumps the map version.
     *
     * @param shardId  unique identifier for the shard
     * @param replicas new list of node IDs that should host this shard
     */
    record SetShardReplicas(String shardId, List<String> replicas) implements RaftCommand {
        public SetShardReplicas {
            if (shardId == null || shardId.isBlank()) {
                throw new IllegalArgumentException("shardId cannot be null or blank");
            }
            if (replicas == null) {
                throw new IllegalArgumentException("replicas cannot be null");
            }
            replicas = List.copyOf(replicas); // defensive copy
        }

	@Override
        public String describe() {
            return "SetShardReplicas(shardId=" + shardId + ", replicas=" + replicas + ")";
        }}

	/**
     * Updates the leader hint for a shard.
     * Only valid if the provided epoch matches the shard's current epoch.
     *
     * @param shardId      unique identifier for the shard
     * @param epoch        expected current epoch (for validation)
     * @param leaderNodeId new leader node ID
     */
    record SetShardLeader(String shardId, long epoch, String leaderNodeId) implements RaftCommand {
        public SetShardLeader {
            if (shardId == null || shardId.isBlank()) {
                throw new IllegalArgumentException("shardId cannot be null or blank");
            }
            if (epoch < 0) {
                throw new IllegalArgumentException("epoch cannot be negative");
            }
            if (leaderNodeId == null || leaderNodeId.isBlank()) {
                throw new IllegalArgumentException("leaderNodeId cannot be null or blank");
            }
        }

	@Override
        public String describe() {
            return "SetShardLeader(shardId="
                    + shardId
                    + ", epoch="
                    + epoch
                    + ", leader="
                    + leaderNodeId
                    + ")";
        }
}}
