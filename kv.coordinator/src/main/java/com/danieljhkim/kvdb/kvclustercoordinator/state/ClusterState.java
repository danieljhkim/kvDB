package com.danieljhkim.kvdb.kvclustercoordinator.state;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.Getter;

/**
 * Mutable cluster state managed by the coordinator. This class is NOT thread-safe on its own - thread safety is
 * provided by the Raft state machine which serializes all mutations.
 *
 * After each mutation, a new immutable {@link ShardMapSnapshot} should be created for safe concurrent reads.
 */
@Getter
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

    /** Replaces the complete state while the owning Raft state machine holds its write lock. */
    public void restore(
            long mapVersion,
            Map<String, NodeRecord> nodes,
            Map<String, ShardRecord> shards,
            int numShards,
            int replicationFactor) {
        if (mapVersion < 0 || numShards < 0 || replicationFactor < 0) {
            throw new IllegalArgumentException("Snapshot contains negative cluster-state metadata");
        }
        this.mapVersion = mapVersion;
        this.nodes.clear();
        this.nodes.putAll(nodes);
        this.shards.clear();
        this.shards.putAll(shards);
        this.numShards = numShards;
        this.replicationFactor = replicationFactor;
    }

    // ============================
    // Getters
    // ============================

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
     * Initialize shards with the given configuration. This should only be called once when bootstrapping the cluster.
     * This method is idempotent - if shards are already initialized with the same configuration, it returns the existing shards.
     *
     * @return list of created shard IDs
     */
    public List<String> initializeShards(int numShards, int replicationFactor) {
        // Make this idempotent for Raft log replay
        if (!shards.isEmpty()) {
            // If already initialized with the same configuration, return existing shards
            if (this.numShards == numShards && this.replicationFactor == replicationFactor) {
                // Already initialized with same config - return existing (idempotent behavior for log replay)
                return new ArrayList<>(shards.keySet());
            } else {
                throw new RejectedMutationException(String.format(
                        "Shards already initialized with different configuration: existing(numShards=%d, rf=%d) vs requested(numShards=%d, rf=%d)",
                        this.numShards, this.replicationFactor, numShards, replicationFactor));
            }
        }
        if (numShards <= 0) {
            throw new IllegalArgumentException("numShards must be positive");
        }

        this.numShards = numShards;
        this.replicationFactor = replicationFactor;

        List<String> nodeIds = new ArrayList<>(nodes.keySet());
        nodeIds.sort(String::compareTo);
        List<String> createdShards = new ArrayList<>();

        for (int i = 0; i < numShards; i++) {
            String shardId = "shard-" + i;
            List<String> replicas = assignReplicas(i, nodeIds, replicationFactor);
            ShardRecord shard = ShardRecord.create(shardId, replicas);
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
     * Register or update a node. Rejects an address that is not {@code host:port}.
     *
     * <p>
     * A registered node starts (or returns to) {@link NodeRecord.NodeStatus#ALIVE} without waiting for a health probe.
     * Registration is issued by the node's operator or bootstrap once the node is serving, and the gateway only routes
     * to ALIVE nodes, so holding new nodes back would leave a freshly bootstrapped cluster unable to serve until the
     * first probe interval elapsed. The health checker demotes an unreachable node to SUSPECT and then DEAD on
     * consecutive failed probes, and a malformed address can no longer be registered.
     *
     * <p>
     * A new node, or a change of address, zone, or return to {@link NodeRecord.NodeStatus#ALIVE}, publishes a newer
     * map version. Watch and conditional-poll consumers apply a snapshot only when that version advances, so an
     * endpoint or routability change that kept the old version would stay cached indefinitely. An identical
     * re-registration refreshes the heartbeat and retains the version. Raft replay of an already-applied registration
     * is therefore idempotent, and the version does not depend on the clock.
     */
    public void registerNode(String nodeId, String address, String zone) {
        ShardMapValidator.validateNodeAddress(address);
        NodeRecord existing = nodes.get(nodeId);
        if (existing != null) {
            boolean routingChange = !address.equals(existing.address())
                    || !Objects.equals(zone, existing.zone())
                    || existing.status() != NodeRecord.NodeStatus.ALIVE;
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
            if (routingChange) {
                mapVersion++;
            }
        } else {
            nodes.put(nodeId, NodeRecord.create(nodeId, address, zone));
            mapVersion++;
        }
    }

    /**
     * Update node status. Any real transition publishes a newer map version: only ALIVE nodes are eligible for
     * routing, so ALIVE to SUSPECT is as visible as a transition through DEAD. An unchanged status retains the version.
     */
    public void setNodeStatus(String nodeId, NodeRecord.NodeStatus status) {
        NodeRecord node = ShardMapValidator.requireNode(nodeId, nodes);
        if (node.status() == status) {
            return;
        }
        nodes.put(nodeId, node.withStatus(status));
        mapVersion++;
    }

    /**
     * Update shard replicas. Increments shard epoch and mapVersion. Rejects an empty set and unknown or duplicate node
     * IDs.
     */
    public void setShardReplicas(String shardId, List<String> newReplicas) {
        ShardMapValidator.validateShardReplicas(shardId, newReplicas, nodes, shards);
        ShardRecord shard = shards.get(shardId);

        shards.put(shardId, shard.withReplicas(newReplicas));
        mapVersion++;
    }

    /**
     * Update shard leader hint. Only valid if epoch matches and the leader is a registered member of the shard's
     * current replica set.
     */
    public void setShardLeader(String shardId, long epoch, String leaderNodeId) {
        ShardMapValidator.validateShardLeader(shardId, epoch, leaderNodeId, nodes, shards);
        ShardRecord shard = shards.get(shardId);

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
