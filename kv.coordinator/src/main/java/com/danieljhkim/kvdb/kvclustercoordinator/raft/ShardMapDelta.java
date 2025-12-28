package com.danieljhkim.kvdb.kvclustercoordinator.raft;

import com.danieljhkim.kvdb.kvclustercoordinator.state.ShardMapSnapshot;

import java.util.List;

/**
 * Represents a change event in the shard map.
 * Used to notify watchers (e.g., gateways) of updates.
 *
 * @param newMapVersion  the new map version after this change
 * @param changedShards  list of shard IDs that were modified
 * @param changedNodes   list of node IDs that were modified
 * @param fullState      optional full state snapshot (may be null if only delta is needed)
 */
public record ShardMapDelta(
        long newMapVersion, List<String> changedShards, List<String> changedNodes, ShardMapSnapshot fullState) {

    public ShardMapDelta {
        changedShards = changedShards == null ? List.of() : List.copyOf(changedShards);
        changedNodes = changedNodes == null ? List.of() : List.copyOf(changedNodes);
    }

    /**
     * Creates a delta for a shard change.
     */
    public static ShardMapDelta forShardChange(long newVersion, String shardId, ShardMapSnapshot snapshot) {
        return new ShardMapDelta(newVersion, List.of(shardId), List.of(), snapshot);
    }

    /**
     * Creates a delta for a node change.
     */
    public static ShardMapDelta forNodeChange(long newVersion, String nodeId, ShardMapSnapshot snapshot) {
        return new ShardMapDelta(newVersion, List.of(), List.of(nodeId), snapshot);
    }

    /**
     * Creates a delta for multiple shard changes.
     */
    public static ShardMapDelta forShardChanges(long newVersion, List<String> shardIds, ShardMapSnapshot snapshot) {
        return new ShardMapDelta(newVersion, shardIds, List.of(), snapshot);
    }

    /**
     * Creates a full state update delta.
     */
    public static ShardMapDelta fullState(ShardMapSnapshot snapshot) {
        return new ShardMapDelta(snapshot.getMapVersion(), List.of(), List.of(), snapshot);
    }
}
