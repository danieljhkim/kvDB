package com.danieljhkim.kvdb.kvclustercoordinator.state;

import java.util.Collections;
import java.util.Map;

/**
 * Immutable snapshot of the cluster state for safe concurrent reads.
 * Created after each state mutation to provide a consistent view.
 *
 * <p>Usage pattern:
 * <pre>
 * AtomicReference<ShardMapSnapshot> snapshotRef = new AtomicReference<>(initialSnapshot);
 *
 * // Writer (single thread via Raft)
 * clusterState.registerNode(...);
 * snapshotRef.set(clusterState.createSnapshot());
 *
 * // Readers (multiple threads)
 * ShardMapSnapshot snapshot = snapshotRef.get();
 * ShardRecord shard = snapshot.getShard("shard-0");
 * </pre>
 */
public final class ShardMapSnapshot {

    private final long mapVersion;
    private final Map<String, NodeRecord> nodes;
    private final Map<String, ShardRecord> shards;
    private final int numShards;
    private final int replicationFactor;

    /**
     * Creates an immutable snapshot from the current cluster state.
     */
    public ShardMapSnapshot(ClusterState state) {
        this.mapVersion = state.getMapVersion();
        this.nodes = Collections.unmodifiableMap(Map.copyOf(state.getNodes()));
        this.shards = Collections.unmodifiableMap(Map.copyOf(state.getShards()));
        this.numShards = state.getNumShards();
        this.replicationFactor = state.getReplicationFactor();
    }

    /**
     * Creates an empty initial snapshot.
     */
    public static ShardMapSnapshot empty() {
        return new ShardMapSnapshot(new ClusterState());
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
    // Shard Resolution
    // ============================

    /**
     * Resolves a key to its owning shard using consistent hashing.
     *
     * @param key the key bytes
     * @return the shard record owning this key, or null if shards not initialized
     */
    public ShardRecord resolveShardForKey(byte[] key) {
        if (numShards == 0 || shards.isEmpty()) {
            return null;
        }

        int hash = computeHash(key);
        int shardIndex = Math.abs(hash) % numShards;
        String shardId = "shard-" + shardIndex;
        return shards.get(shardId);
    }

    /**
     * Resolves a key to its owning shard ID.
     */
    public String resolveShardIdForKey(byte[] key) {
        if (numShards == 0) {
            return null;
        }
        int hash = computeHash(key);
        int shardIndex = Math.abs(hash) % numShards;
        return "shard-" + shardIndex;
    }

    /**
     * Simple hash function for key-to-shard mapping.
     * Uses FNV-1a for good distribution.
     */
    private int computeHash(byte[] key) {
        if (key == null || key.length == 0) {
            return 0;
        }
        // FNV-1a hash
        int hash = 0x811c9dc5;
        for (byte b : key) {
            hash ^= (b & 0xff);
            hash *= 0x01000193;
        }
        return hash;
    }

    // ============================
    // Utility
    // ============================

    /**
     * Checks if this snapshot is newer than the given version.
     */
    public boolean isNewerThan(long version) {
        return this.mapVersion > version;
    }

    @Override
    public String toString() {
        return "ShardMapSnapshot{"
                + "mapVersion="
                + mapVersion
                + ", nodes="
                + nodes.size()
                + ", shards="
                + shards.size()
                + ", numShards="
                + numShards
                + ", rf="
                + replicationFactor
                + '}';
    }
}

