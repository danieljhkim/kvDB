package com.danieljhkim.kvdb.kvclustercoordinator.state;

import com.danieljhkim.kvdb.kvcommon.sharding.ShardKeyMapper;
import java.util.Collections;
import java.util.Map;
import lombok.Getter;

/**
 * Immutable snapshot of the cluster state for safe concurrent reads. Created after each state mutation to provide a
 * consistent view.
 *
 * <p>
 * Usage pattern:
 *
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
@Getter
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
     * Resolves a key to its owning shard using the data plane's key-to-shard function.
     *
     * @param key the key bytes
     * @return the shard record owning this key, or null if shards not initialized
     */
    public ShardRecord resolveShardForKey(byte[] key) {
        if (numShards == 0 || shards.isEmpty()) {
            return null;
        }
        return shards.get(ShardKeyMapper.shardId(key, numShards));
    }

    /**
     * Resolves a key to its owning shard ID.
     */
    public String resolveShardIdForKey(byte[] key) {
        if (numShards == 0) {
            return null;
        }
        return ShardKeyMapper.shardId(key, numShards);
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
        return "ShardMapSnapshot{" + "mapVersion=" + mapVersion + ", nodes=" + nodes.size() + ", shards="
                + shards.size() + ", numShards=" + numShards + ", rf=" + replicationFactor + '}';
    }
}
