package com.danieljhkim.kvdb.kvclustercoordinator.state;

import java.util.Collections;
import java.util.List;

/**
 * Immutable record representing a shard's metadata. Used by the coordinator to track shard assignments and leader
 * hints.
 *
 * <p>
 * Keys map to shards by {@code ShardKeyMapper} (modulo hashing), which is not a contiguous range partition, so no key
 * range is tracked here.
 */
public record ShardRecord(
        String shardId, long epoch, List<String> replicas, String leader, ShardConfigState configState) {

    public enum ShardConfigState {
        UNSPECIFIED,
        STABLE,
        MOVING
    }

    /**
     * Canonical constructor with defensive copy of replicas.
     */
    public ShardRecord {
        if (shardId == null || shardId.isBlank()) {
            throw new IllegalArgumentException("shardId cannot be null or blank");
        }
        if (epoch < 0) {
            throw new IllegalArgumentException("epoch cannot be negative");
        }
        if (configState == null) {
            configState = ShardConfigState.UNSPECIFIED;
        }
        replicas = replicas == null ? Collections.emptyList() : List.copyOf(replicas);
    }

    /**
     * Creates a new shard with initial configuration.
     */
    public static ShardRecord create(String shardId, List<String> replicas) {
        String initialLeader = replicas.isEmpty() ? null : replicas.get(0);
        return new ShardRecord(shardId, 1L /* initial epoch */, replicas, initialLeader, ShardConfigState.STABLE);
    }

    /**
     * Returns a new ShardRecord with updated replicas and incremented epoch.
     */
    public ShardRecord withReplicas(List<String> newReplicas) {
        String newLeader = newReplicas.isEmpty() ? null : newReplicas.get(0);
        return new ShardRecord(shardId, epoch + 1, newReplicas, newLeader, configState);
    }

    /**
     * Returns a new ShardRecord with updated leader hint. Only valid if the provided epoch matches the current epoch.
     */
    public ShardRecord withLeader(long expectedEpoch, String newLeader) {
        if (this.epoch != expectedEpoch) {
            throw new IllegalArgumentException("Epoch mismatch: expected " + expectedEpoch + ", current " + this.epoch);
        }
        return new ShardRecord(shardId, epoch, replicas, newLeader, configState);
    }

    /**
     * Returns a new ShardRecord with updated config state.
     */
    public ShardRecord withConfigState(ShardConfigState newState) {
        return new ShardRecord(shardId, epoch, replicas, leader, newState);
    }
}
