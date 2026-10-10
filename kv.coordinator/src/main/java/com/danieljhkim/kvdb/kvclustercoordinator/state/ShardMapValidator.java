package com.danieljhkim.kvdb.kvclustercoordinator.state;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Validates shard-map mutations against cluster membership.
 *
 * <p>
 * The coordinator runs these checks twice: on the leader against the latest snapshot before a command is appended to
 * the Raft log, and in {@link ClusterState} when a committed command is applied. The second pass covers commands that
 * became invalid between submission and commit (for example a concurrent replica change) and malformed log entries.
 * Every check throws {@link RejectedMutationException} before any state is touched.
 */
public final class ShardMapValidator {

    // Node addresses are dialed as host:port by the gateway and the health checker, neither of which accepts an IPv6
    // literal, so the host is a hostname or IPv4 address without a colon.
    private static final Pattern HOST = Pattern.compile("[A-Za-z0-9._-]+");
    private static final Pattern PORT = Pattern.compile("[0-9]{1,5}");
    private static final int MAX_PORT = 65535;

    private ShardMapValidator() {}

    /**
     * Requires {@code host:port} with a non-empty host and a port in 1-65535.
     */
    public static void validateNodeAddress(String address) {
        if (address == null || address.isBlank()) {
            throw new RejectedMutationException("Node address is required (expected host:port)");
        }
        int colon = address.lastIndexOf(':');
        String host = colon < 0 ? "" : address.substring(0, colon);
        String port = colon < 0 ? "" : address.substring(colon + 1);
        if (!HOST.matcher(host).matches()
                || !PORT.matcher(port).matches()
                || Integer.parseInt(port) < 1
                || Integer.parseInt(port) > MAX_PORT) {
            throw new RejectedMutationException(
                    "Invalid node address '" + address + "' (expected host:port with port 1-" + MAX_PORT + ")");
        }
    }

    /**
     * Requires a non-empty set of distinct, registered node IDs for an existing shard.
     */
    public static void validateShardReplicas(
            String shardId, List<String> replicas, Map<String, NodeRecord> nodes, Map<String, ShardRecord> shards) {
        requireShard(shardId, shards);
        if (replicas == null || replicas.isEmpty()) {
            throw new RejectedMutationException("Replica set for " + shardId + " cannot be empty");
        }
        Set<String> seen = new HashSet<>();
        for (String replica : replicas) {
            if (replica == null || replica.isBlank()) {
                throw new RejectedMutationException("Replica set for " + shardId + " contains a blank node ID");
            }
            if (!seen.add(replica)) {
                throw new RejectedMutationException(
                        "Replica set for " + shardId + " contains duplicate node " + replica);
            }
            requireNode(replica, nodes);
        }
    }

    /**
     * Requires the expected epoch to match and the new leader to be a registered member of the shard's current replica
     * set.
     */
    public static void validateShardLeader(
            String shardId,
            long epoch,
            String leaderNodeId,
            Map<String, NodeRecord> nodes,
            Map<String, ShardRecord> shards) {
        ShardRecord shard = requireShard(shardId, shards);
        if (shard.epoch() != epoch) {
            throw new RejectedMutationException(
                    "Epoch mismatch for " + shardId + ": expected " + epoch + ", current " + shard.epoch());
        }
        if (leaderNodeId == null || leaderNodeId.isBlank()) {
            throw new RejectedMutationException("Leader node ID for " + shardId + " is required");
        }
        requireNode(leaderNodeId, nodes);
        if (!shard.replicas().contains(leaderNodeId)) {
            throw new RejectedMutationException("Node " + leaderNodeId + " is not a replica of " + shardId
                    + " (replicas: " + shard.replicas() + ")");
        }
    }

    static ShardRecord requireShard(String shardId, Map<String, ShardRecord> shards) {
        ShardRecord shard = shards.get(shardId);
        if (shard == null) {
            throw new RejectedMutationException("Shard not found: " + shardId);
        }
        return shard;
    }

    static NodeRecord requireNode(String nodeId, Map<String, NodeRecord> nodes) {
        NodeRecord node = nodes.get(nodeId);
        if (node == null) {
            throw new RejectedMutationException("Node not found: " + nodeId);
        }
        return node;
    }
}
