package com.danieljhkim.kvdb.kvnode.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.danieljhkim.kvdb.kvcommon.cache.ShardMapCache;
import com.danieljhkim.kvdb.kvcommon.exception.NodeUnavailableException;
import com.danieljhkim.kvdb.kvnode.client.ReplicaWriteClient;
import com.danieljhkim.kvdb.kvnode.storage.ShardKVStore;
import com.danieljhkim.kvdb.kvnode.storage.ShardStoreRegistry;
import com.danieljhkim.kvdb.proto.coordinator.ClusterState;
import com.danieljhkim.kvdb.proto.coordinator.NodeRecord;
import com.danieljhkim.kvdb.proto.coordinator.PartitioningConfig;
import com.danieljhkim.kvdb.proto.coordinator.ShardRecord;
import com.kvdb.proto.kvstore.ReplicaRepairRequest;
import com.kvdb.proto.kvstore.ReplicaRepairResponse;
import com.kvdb.proto.kvstore.ReplicaStateRequest;
import com.kvdb.proto.kvstore.ReplicaStateResponse;
import com.kvdb.proto.kvstore.ReplicateMutationRequest;
import com.kvdb.proto.kvstore.ReplicationAck;
import com.kvdb.proto.kvstore.WriteDurability;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ReplicationManagerTest {

    @TempDir
    Path tempDir;

    private ReplicationManager manager;

    @AfterEach
    void stopRepairWorker() {
        if (manager != null) {
            manager.close();
        }
    }

    @Test
    void partitionedQuorumLeavesNoVisibleMutationOrFailoverState() {
        Fixture fixture = fixture();
        fixture.client.partitioned.addAll(Set.of("node-2:9000", "node-3:9000"));

        assertThrows(
                NodeUnavailableException.class,
                () -> manager.replicateSet(
                        "shard-0",
                        fixture.shard,
                        "key",
                        "uncommitted",
                        "request-partition",
                        WriteDurability.QUORUM_SYNC));
        assertEquals("(nil)", fixture.leader.getOrCreate("shard-0").get("key"));
        fixture.leader.shutdown();

        ShardStoreRegistry restarted = registry("leader");
        assertEquals("(nil)", restarted.getOrCreate("shard-0").get("key"));
        restarted.shutdown();
        fixture.followers.values().forEach(ShardKVStore::shutdown);
    }

    @Test
    void failedCommitQuorumLeavesEveryPreparedCopyHidden() {
        Fixture fixture = fixture();
        fixture.client.commitFailed.addAll(Set.of("node-2:9000", "node-3:9000"));

        assertThrows(
                NodeUnavailableException.class,
                () -> manager.replicateSet(
                        "shard-0",
                        fixture.shard,
                        "key",
                        "not-acknowledged",
                        "request-commit-failure",
                        WriteDurability.QUORUM_SYNC));

        assertEquals("(nil)", fixture.leader.getOrCreate("shard-0").get("key"));
        fixture.followers.values().forEach(store -> assertEquals("(nil)", store.get("key")));
        fixture.leader.shutdown();
        fixture.followers.values().forEach(ShardKVStore::shutdown);
    }

    @Test
    void quorumWriteIsIdempotentAndTimedOutReplicaConvergesAfterRejoin() {
        Fixture fixture = fixture();
        fixture.client.delayed.add("node-3:9000");
        long started = System.nanoTime();
        ReplicationManager.MutationResult first = manager.replicateSet(
                "shard-0", fixture.shard, "key", "value", "request-retry", WriteDurability.QUORUM_SYNC);
        long elapsedMillis = Duration.ofNanos(System.nanoTime() - started).toMillis();

        assertTrue(elapsedMillis < 500, "replication should be bounded by its timeout");
        assertEquals("value", fixture.leader.getOrCreate("shard-0").get("key"));
        assertEquals("value", fixture.followers.get("node-2:9000").get("key"));

        ReplicationManager.MutationResult duplicate = manager.replicateSet(
                "shard-0", fixture.shard, "key", "value", "request-retry", WriteDurability.QUORUM_SYNC);
        assertEquals(first.version(), duplicate.version());

        fixture.client.delayed.clear();
        manager.repairReplicas("shard-0", fixture.shard);
        assertEquals("value", fixture.followers.get("node-3:9000").get("key"));
        assertTrue(manager.repairProgress("node-3:9000", "shard-0").complete());

        fixture.leader.shutdown();
        fixture.followers.values().forEach(ShardKVStore::shutdown);
    }

    @Test
    void laggingReplicaPullsCommittedQuorumStateBeforeTakingLeadership() {
        Fixture fixture = fixture();
        fixture.client.partitioned.add("node-3:9000");
        manager.replicateSet("shard-0", fixture.shard, "key", "value", "request-failover", WriteDurability.QUORUM_SYNC);
        assertEquals("(nil)", fixture.followers.get("node-3:9000").get("key"));
        manager.close();

        ShardRecord promotedShard =
                fixture.shard.toBuilder().setEpoch(4).setLeader("node-3").build();
        ShardMapCache promotedCache = cache(promotedShard);
        FakeReplicaClient promotedClient = new FakeReplicaClient(Map.of(
                "node-1:9000", fixture.leader.getOrCreate("shard-0"),
                "node-2:9000", fixture.followers.get("node-2:9000")));
        ShardKVStore promotedStore = fixture.followers.get("node-3:9000");
        manager = new ReplicationManager(
                "node-3",
                promotedCache,
                new FixedRegistry(tempDir.resolve("promoted-registry"), promotedStore),
                promotedClient,
                Duration.ofMillis(40));

        // The direct local read models EVENTUAL behavior and is stale before promotion.
        assertEquals("(nil)", promotedStore.get("key"));
        // STRONG admission performs a fresh quorum pull before exposing the value.
        manager.ensureStrongReadReady("shard-0", promotedShard);
        assertEquals("value", promotedStore.get("key"));
        assertEquals(1, promotedStore.committedVersion());

        fixture.leader.shutdown();
        fixture.followers.values().forEach(ShardKVStore::shutdown);
    }

    @Test
    void strongReadRefusesPartitionEvenAfterEpochWasPreviouslyReconciled() {
        Fixture fixture = fixture();
        manager.replicateSet(
                "shard-0", fixture.shard, "key", "value", "request-before-partition", WriteDurability.QUORUM_SYNC);
        manager.ensureLeaderReconciled("shard-0", fixture.shard);
        fixture.client.partitioned.addAll(Set.of("node-2:9000", "node-3:9000"));

        // EVENTUAL local reads remain explicit and available during the partition.
        assertEquals("value", fixture.leader.getOrCreate("shard-0").get("key"));
        // STRONG cannot reuse the old reconciliation decision without a fresh quorum.
        assertThrows(NodeUnavailableException.class, () -> manager.ensureStrongReadReady("shard-0", fixture.shard));

        fixture.leader.shutdown();
        fixture.followers.values().forEach(ShardKVStore::shutdown);
    }

    private Fixture fixture() {
        ShardRecord shard = ShardRecord.newBuilder()
                .setShardId("shard-0")
                .setEpoch(3)
                .setLeader("node-1")
                .addReplicas("node-1")
                .addReplicas("node-2")
                .addReplicas("node-3")
                .build();
        ShardMapCache cache = cache(shard);

        ShardStoreRegistry leader = registry("leader");
        Map<String, ShardKVStore> followers = Map.of(
                "node-2:9000", newStore("follower-2"),
                "node-3:9000", newStore("follower-3"));
        FakeReplicaClient client = new FakeReplicaClient(followers);
        manager = new ReplicationManager("node-1", cache, leader, client, Duration.ofMillis(40));
        return new Fixture(shard, leader, followers, client);
    }

    private static ShardMapCache cache(ShardRecord shard) {
        ShardMapCache cache = new ShardMapCache();
        cache.refreshFromFullState(ClusterState.newBuilder()
                .setMapVersion(1)
                .setPartitioning(PartitioningConfig.newBuilder().setNumShards(1).setReplicationFactor(3))
                .putNodes("node-1", node("node-1", "node-1:9000"))
                .putNodes("node-2", node("node-2", "node-2:9000"))
                .putNodes("node-3", node("node-3", "node-3:9000"))
                .putShards("shard-0", shard)
                .build());
        return cache;
    }

    private ShardStoreRegistry registry(String name) {
        return new ShardStoreRegistry(tempDir.resolve(name).toString(), "snapshot.json", "wal.log", 100, false);
    }

    private ShardKVStore newStore(String name) {
        return new ShardKVStore(
                "shard-0",
                tempDir.resolve(name + ".json").toString(),
                tempDir.resolve(name + ".wal").toString(),
                100,
                false);
    }

    private static NodeRecord node(String id, String address) {
        return NodeRecord.newBuilder().setNodeId(id).setAddress(address).build();
    }

    private record Fixture(
            ShardRecord shard,
            ShardStoreRegistry leader,
            Map<String, ShardKVStore> followers,
            FakeReplicaClient client) {}

    private static final class FakeReplicaClient extends ReplicaWriteClient {
        private final Map<String, ShardKVStore> stores;
        private final Set<String> partitioned = ConcurrentHashMap.newKeySet();
        private final Set<String> delayed = ConcurrentHashMap.newKeySet();
        private final Set<String> commitFailed = ConcurrentHashMap.newKeySet();

        private FakeReplicaClient(Map<String, ShardKVStore> stores) {
            super(Duration.ofMillis(20));
            this.stores = stores;
        }

        @Override
        public ReplicationAck replicateMutation(String targetAddress, ReplicateMutationRequest request) {
            unavailableIfNeeded(targetAddress);
            if (request.getPhase() == com.kvdb.proto.kvstore.ReplicationPhase.COMMIT
                    && commitFailed.contains(targetAddress)) {
                throw new IllegalStateException("commit acknowledgement lost");
            }
            ShardKVStore store = stores.get(targetAddress);
            ShardKVStore.MutationStatus status =
                    switch (request.getPhase()) {
                        case PREPARE -> store.prepareMutation(request.getMutation());
                        case COMMIT -> store.commitMutation(request.getMutation());
                        case ABORT -> store.abortMutation(request.getMutation());
                        default ->
                            new ShardKVStore.MutationStatus(false, false, "phase required", store.committedVersion());
                    };
            return ReplicationAck.newBuilder()
                    .setSuccess(status.success())
                    .setDurable(status.durable())
                    .setEpoch(store.shardEpoch())
                    .setCommittedVersion(status.committedVersion())
                    .setMessage(status.message())
                    .build();
        }

        @Override
        public ReplicaRepairResponse repairReplica(String targetAddress, ReplicaRepairRequest request) {
            unavailableIfNeeded(targetAddress);
            ShardKVStore store = stores.get(targetAddress);
            int applied = 0;
            for (var mutation : request.getCommittedMutationsList()) {
                ShardKVStore.MutationStatus status = store.repairMutation(mutation);
                if (!status.success()) {
                    return ReplicaRepairResponse.newBuilder()
                            .setSuccess(false)
                            .setCommittedVersion(store.committedVersion())
                            .build();
                }
                if ("repaired".equals(status.message())) {
                    applied++;
                }
            }
            return ReplicaRepairResponse.newBuilder()
                    .setSuccess(true)
                    .setDurable(true)
                    .setAppliedMutations(applied)
                    .setCommittedVersion(store.committedVersion())
                    .build();
        }

        @Override
        public ReplicaStateResponse fetchReplicaState(String targetAddress, ReplicaStateRequest request) {
            unavailableIfNeeded(targetAddress);
            ShardKVStore store = stores.get(targetAddress);
            var mutations =
                    store.committedMutationsAfter(request.getAfterVersion(), Math.max(1, request.getMaxMutations()));
            long lastVersion = mutations.isEmpty()
                    ? request.getAfterVersion()
                    : mutations.getLast().getVersion();
            return ReplicaStateResponse.newBuilder()
                    .setSuccess(true)
                    .setDurable(true)
                    .addAllCommittedMutations(mutations)
                    .setHasMore(!store.committedMutationsAfter(lastVersion, 1).isEmpty())
                    .setCommittedVersion(store.committedVersion())
                    .build();
        }

        private void unavailableIfNeeded(String targetAddress) {
            if (partitioned.contains(targetAddress)) {
                throw new IllegalStateException("partitioned");
            }
            if (delayed.contains(targetAddress)) {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("interrupted", e);
                }
            }
        }
    }

    private static final class FixedRegistry extends ShardStoreRegistry {
        private final ShardKVStore store;

        private FixedRegistry(Path baseDir, ShardKVStore store) {
            super(baseDir.toString(), "unused.json", "unused.wal", 100, false);
            this.store = store;
        }

        @Override
        public ShardKVStore getOrCreate(String shardId) {
            return store;
        }
    }
}
