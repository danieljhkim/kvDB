package com.danieljhkim.kvdb.kvnode.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.danieljhkim.kvdb.kvcommon.cache.ShardMapCache;
import com.danieljhkim.kvdb.kvcommon.config.AppConfig;
import com.danieljhkim.kvdb.kvcommon.exception.NodeUnavailableException;
import com.danieljhkim.kvdb.kvcommon.limits.KvRequestLimits;
import com.danieljhkim.kvdb.kvnode.client.ReplicaWriteClient;
import com.danieljhkim.kvdb.kvnode.service.KVServiceImpl;
import com.danieljhkim.kvdb.kvnode.storage.ShardKVStore;
import com.danieljhkim.kvdb.kvnode.storage.ShardStoreRegistry;
import com.danieljhkim.kvdb.proto.coordinator.ClusterState;
import com.danieljhkim.kvdb.proto.coordinator.NodeRecord;
import com.danieljhkim.kvdb.proto.coordinator.PartitioningConfig;
import com.danieljhkim.kvdb.proto.coordinator.ShardRecord;
import com.google.protobuf.ByteString;
import com.kvdb.proto.kvstore.MutationKind;
import com.kvdb.proto.kvstore.ReplicaRepairRequest;
import com.kvdb.proto.kvstore.ReplicaRepairResponse;
import com.kvdb.proto.kvstore.ReplicaStateRequest;
import com.kvdb.proto.kvstore.ReplicaStateResponse;
import com.kvdb.proto.kvstore.ReplicateMutationRequest;
import com.kvdb.proto.kvstore.ReplicatedMutation;
import com.kvdb.proto.kvstore.ReplicationAck;
import com.kvdb.proto.kvstore.ReplicationPhase;
import com.kvdb.proto.kvstore.WriteDurability;
import io.grpc.stub.StreamObserver;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
    void lostPrepareResponseAndDroppedAbortDoNotBlockTheNextAllSyncWrite() {
        Fixture fixture = fixture();
        fixture.client.prepareResponseLost.add("node-2:9000");
        fixture.client.abortDropped.add("node-2:9000");

        assertThrows(
                NodeUnavailableException.class,
                () -> manager.replicateSet(
                        "shard-0",
                        fixture.shard,
                        "aborted",
                        "must-remain-hidden",
                        "request-lost-prepare-ack",
                        WriteDurability.ALL_SYNC));
        assertEquals("(nil)", fixture.followers.get("node-2:9000").get("aborted"));

        fixture.client.prepareResponseLost.clear();
        fixture.client.abortDropped.clear();
        manager.replicateSet(
                "shard-0", fixture.shard, "committed", "visible", "request-after-orphan", WriteDurability.ALL_SYNC);

        assertEquals("(nil)", fixture.leader.getOrCreate("shard-0").get("aborted"));
        fixture.followers.values().forEach(store -> {
            assertEquals("(nil)", store.get("aborted"));
            assertEquals("visible", store.get("committed"));
        });
        fixture.leader.shutdown();
        fixture.followers.values().forEach(ShardKVStore::shutdown);
    }

    @Test
    void delayedPrepareAfterAbortIsRecoveredByTheNextAllSyncWrite() throws InterruptedException {
        Fixture fixture = fixture();
        fixture.client.latePrepare.add("node-2:9000");
        fixture.client.partitioned.add("node-3:9000");

        assertThrows(
                NodeUnavailableException.class,
                () -> manager.replicateSet(
                        "shard-0",
                        fixture.shard,
                        "aborted",
                        "must-remain-hidden",
                        "request-late-prepare",
                        WriteDurability.ALL_SYNC));
        assertTrue(fixture.client.latePrepareStarted.await(1, TimeUnit.SECONDS));
        assertTrue(fixture.client.abortCompleted.await(1, TimeUnit.SECONDS));
        fixture.client.releaseLatePrepare();
        assertTrue(fixture.client.latePrepareCompleted.await(1, TimeUnit.SECONDS));
        assertEquals("(nil)", fixture.followers.get("node-2:9000").get("aborted"));

        fixture.client.latePrepare.clear();
        fixture.client.partitioned.clear();
        manager.replicateSet(
                "shard-0",
                fixture.shard,
                "committed",
                "visible",
                "request-after-late-prepare",
                WriteDurability.ALL_SYNC);

        fixture.followers.values().forEach(store -> {
            assertEquals("(nil)", store.get("aborted"));
            assertEquals("visible", store.get("committed"));
        });
        fixture.leader.shutdown();
        fixture.followers.values().forEach(ShardKVStore::shutdown);
    }

    @Test
    void completedPhaseResultDoesNotChangeWhenAnRpcFinishesLate() throws InterruptedException {
        Fixture fixture = fixture();
        fixture.client.latePrepare.add("node-2:9000");
        ReplicatedMutation mutation = mutation(1, MutationKind.SET, "key", "value");

        Set<String> completed = manager.executePhase(List.of("node-2:9000"), mutation, ReplicationPhase.PREPARE);
        assertTrue(completed.isEmpty());
        assertTrue(fixture.client.latePrepareStarted.await(1, TimeUnit.SECONDS));
        fixture.client.releaseLatePrepare();
        assertTrue(fixture.client.latePrepareCompleted.await(1, TimeUnit.SECONDS));
        assertTrue(completed.isEmpty());

        fixture.leader.shutdown();
        fixture.followers.values().forEach(ShardKVStore::shutdown);
    }

    @Test
    void quorumWriteIsIdempotentAndTimedOutReplicaConvergesAfterRejoin() throws InterruptedException {
        Fixture fixture = fixture();
        fixture.client.delayed.add("node-3:9000");
        ReplicationManager.MutationResult first = manager.replicateSet(
                "shard-0", fixture.shard, "key", "value", "request-retry", WriteDurability.QUORUM_SYNC);

        assertTrue(fixture.client.delayedRequestStarted.await(1, TimeUnit.SECONDS));
        assertEquals(2, first.durableAcks());
        assertEquals("value", fixture.leader.getOrCreate("shard-0").get("key"));
        assertEquals("value", fixture.followers.get("node-2:9000").get("key"));

        ReplicationManager.MutationResult duplicate = manager.replicateSet(
                "shard-0", fixture.shard, "key", "value", "request-retry", WriteDurability.QUORUM_SYNC);
        assertEquals(first.version(), duplicate.version());

        fixture.client.delayed.clear();
        fixture.client.releaseDelayedRequest();
        assertTrue(fixture.client.delayedRequestCompleted.await(1, TimeUnit.SECONDS));
        manager.repairReplicas("shard-0", fixture.shard);
        assertEquals("value", fixture.followers.get("node-3:9000").get("key"));
        assertTrue(manager.repairProgress("node-3:9000", "shard-0").complete());

        fixture.leader.shutdown();
        fixture.followers.values().forEach(ShardKVStore::shutdown);
    }

    @Test
    void repairBatchesStayWithinReceiverLimitsAndConvergeNearLimitValuesAndTombstones() {
        ShardRecord shard = repairShard();
        AppConfig.LimitsConfig limits = limits(128, 300, 2);
        ShardStoreRegistry leader = registry("bounded-repair-leader");
        ShardStoreRegistry follower = registry("bounded-repair-follower");
        KVServiceImpl receiver =
                new KVServiceImpl("node-2", cache(shard), follower, null, Duration.ofMillis(40), limits);
        ReceiverValidatingReplicaClient client = new ReceiverValidatingReplicaClient(receiver);
        manager = new ReplicationManager(
                "node-1", cache(shard), leader, client, Duration.ofMillis(40), new KvRequestLimits(limits));
        try {
            String nearLimit = "v".repeat(128);
            manager.replicateSet("shard-0", shard, "first", nearLimit, "near-limit-1", WriteDurability.LOCAL_SYNC);
            manager.replicateSet("shard-0", shard, "removed", nearLimit, "near-limit-2", WriteDurability.LOCAL_SYNC);
            manager.replicateDelete("shard-0", shard, "removed", "near-limit-delete", WriteDurability.LOCAL_SYNC);
            manager.replicateSet("shard-0", shard, "last", nearLimit, "near-limit-3", WriteDurability.LOCAL_SYNC);

            for (int pass = 0; pass < 8; pass++) {
                manager.repairReplicas("shard-0", shard);
                if (manager.repairProgress("node-2:9000", "shard-0").complete()) {
                    break;
                }
            }

            ShardKVStore repaired = follower.getOrCreate("shard-0");
            assertEquals(nearLimit, repaired.get("first"));
            assertEquals("(nil)", repaired.get("removed"));
            assertEquals(nearLimit, repaired.get("last"));
            assertTrue(manager.repairProgress("node-2:9000", "shard-0").complete());
            assertTrue(client.repairRequests.size() >= 2);
            client.repairRequests.forEach(request -> {
                assertTrue(request.getCommittedMutationsCount() <= 2);
                assertTrue(request.getSerializedSize() <= 300);
            });
        } finally {
            receiver.shutdownReplication();
            leader.shutdown();
            follower.shutdown();
        }
    }

    @Test
    void repairSkipsAnIndividuallyUntransferableMutationAndAdvancesToLaterMutations() {
        ShardRecord shard = repairShard();
        AppConfig.LimitsConfig limits = limits(128, 150, 1);
        ShardStoreRegistry leader = registry("untransferable-repair-leader");
        ShardStoreRegistry follower = registry("untransferable-repair-follower");
        KVServiceImpl receiver =
                new KVServiceImpl("node-2", cache(shard), follower, null, Duration.ofMillis(40), limits);
        ReceiverValidatingReplicaClient client = new ReceiverValidatingReplicaClient(receiver);
        manager = new ReplicationManager(
                "node-1", cache(shard), leader, client, Duration.ofMillis(40), new KvRequestLimits(limits));
        try {
            manager.replicateSet(
                    "shard-0", shard, "too-large", "v".repeat(128), "untransferable", WriteDurability.LOCAL_SYNC);
            manager.replicateSet("shard-0", shard, "after", "ok", "after-untransferable", WriteDurability.LOCAL_SYNC);

            manager.repairReplicas("shard-0", shard);
            assertTrue(client.repairRequests.isEmpty());
            assertFalse(manager.repairProgress("node-2:9000", "shard-0").complete());

            manager.repairReplicas("shard-0", shard);
            assertEquals("ok", follower.getOrCreate("shard-0").get("after"));
            assertEquals("(nil)", follower.getOrCreate("shard-0").get("too-large"));
            assertEquals(1, client.repairRequests.size());
            assertEquals(
                    "after",
                    client.repairRequests
                            .getFirst()
                            .getCommittedMutations(0)
                            .getKey()
                            .toStringUtf8());
            assertTrue(manager.repairProgress("node-2:9000", "shard-0").complete());
        } finally {
            receiver.shutdownReplication();
            leader.shutdown();
            follower.shutdown();
        }
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
    void promotedReplicaReconcilesConflictingOrphanAndPreservesCommittedStateAcrossRestart() {
        ShardRecord promotedShard = promotedShard();
        ShardKVStore promotedStore = newStore("orphan-promoted");
        ReplicatedMutation orphan = mutation(1, MutationKind.SET, "aborted", "hidden");
        ReplicatedMutation committed = orphan.toBuilder()
                .setRequestId("request-committed-before-promotion")
                .setKey(ByteString.copyFromUtf8("preserved"))
                .setValue(ByteString.copyFromUtf8("committed"))
                .build();
        assertTrue(promotedStore.prepareMutation(orphan).success());

        ShardKVStore peer1 = newStore("orphan-peer-1");
        ShardKVStore peer2 = newStore("orphan-peer-2");
        assertTrue(peer1.repairMutation(committed).success());
        assertTrue(peer2.repairMutation(committed).success());
        FakeReplicaClient promotedClient = new FakeReplicaClient(Map.of(
                "node-1:9000", peer1,
                "node-2:9000", peer2));
        manager = new ReplicationManager(
                "node-3",
                cache(promotedShard),
                new FixedRegistry(tempDir.resolve("orphan-promoted-registry"), promotedStore),
                promotedClient,
                Duration.ofMillis(40));

        manager.ensureLeaderReconciled("shard-0", promotedShard);
        assertEquals("(nil)", promotedStore.get("aborted"));
        assertEquals("committed", promotedStore.get("preserved"));
        manager.replicateSet(
                "shard-0", promotedShard, "new", "value", "request-after-promotion", WriteDurability.ALL_SYNC);
        assertEquals("committed", promotedStore.get("preserved"));
        assertEquals("value", promotedStore.get("new"));
        assertFalse(promotedStore.commitMutation(orphan).success());

        manager.close();
        manager = null;
        promotedStore.shutdown();
        ShardKVStore restarted = newStore("orphan-promoted");
        assertEquals("(nil)", restarted.get("aborted"));
        assertEquals("committed", restarted.get("preserved"));
        assertEquals("value", restarted.get("new"));
        restarted.shutdown();
        peer1.shutdown();
        peer2.shutdown();
    }

    @Test
    void promotedLeaderCompletesAllSyncWithFollowerOnlyOlderEpochPrepareAndStableRetry() {
        ShardRecord promotedShard = promotedShard();
        ShardKVStore promotedStore = newStore("promoted-empty");
        ShardKVStore emptyPeer = newStore("promoted-empty-peer");
        ShardKVStore orphanPeer = newStore("promoted-orphan-peer");
        ReplicatedMutation orphan = mutation(1, MutationKind.SET, "orphan", "hidden");
        assertTrue(orphanPeer.prepareMutation(orphan).success());

        FakeReplicaClient promotedClient = new FakeReplicaClient(Map.of(
                "node-1:9000", emptyPeer,
                "node-2:9000", orphanPeer));
        manager = new ReplicationManager(
                "node-3",
                cache(promotedShard),
                new FixedRegistry(tempDir.resolve("promoted-empty-registry"), promotedStore),
                promotedClient,
                Duration.ofMillis(40));

        manager.ensureLeaderReconciled("shard-0", promotedShard);
        assertEquals("(nil)", promotedStore.get("orphan"));
        assertEquals("(nil)", orphanPeer.get("orphan"));

        ReplicationManager.MutationResult first = manager.replicateSet(
                "shard-0", promotedShard, "successor", "visible", "stable-request", WriteDurability.ALL_SYNC);
        ReplicationManager.MutationResult retry = manager.replicateSet(
                "shard-0", promotedShard, "successor", "visible", "stable-request", WriteDurability.ALL_SYNC);

        assertEquals(first.version(), retry.version());
        assertEquals(1, first.version());
        List.of(promotedStore, emptyPeer, orphanPeer).forEach(store -> {
            assertEquals("(nil)", store.get("orphan"));
            assertEquals("visible", store.get("successor"));
            assertFalse(store.commitMutation(orphan).success());
        });

        manager.close();
        manager = null;
        promotedStore.shutdown();
        emptyPeer.shutdown();
        orphanPeer.shutdown();

        ShardKVStore restartedOrphanPeer = newStore("promoted-orphan-peer");
        assertEquals("(nil)", restartedOrphanPeer.get("orphan"));
        assertEquals("visible", restartedOrphanPeer.get("successor"));
        assertFalse(restartedOrphanPeer.commitMutation(orphan).success());
        restartedOrphanPeer.shutdown();
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

    @Test
    void promotedReplicaRestoresOlderVersionHolesAndTombstonesAcrossPeerOrderAndRestart() {
        ShardRecord promotedShard = promotedShard();
        ShardKVStore promotedStore = newStore("holes-promoted");
        ReplicatedMutation staleValue = mutation(1, MutationKind.SET, "deleted", "stale");
        ReplicatedMutation tombstone = mutation(2, MutationKind.DELETE, "deleted", "");
        ReplicatedMutation missingValue = mutation(3, MutationKind.SET, "missing", "restored");
        ReplicatedMutation highWatermark = mutation(4, MutationKind.SET, "marker", "latest");
        assertTrue(promotedStore.repairMutation(staleValue).success());
        assertTrue(promotedStore.repairMutation(highWatermark).success());

        FakeReplicaClient promotedClient = new FakeReplicaClient(Map.of());
        // node-1 is visited first and only confirms the high watermark. The older entries exist on node-2.
        promotedClient.replicaStates.put("node-1:9000", List.of(highWatermark));
        promotedClient.replicaStates.put("node-2:9000", List.of(staleValue, tombstone, missingValue));
        manager = new ReplicationManager(
                "node-3",
                cache(promotedShard),
                new FixedRegistry(tempDir.resolve("holes-registry"), promotedStore),
                promotedClient,
                Duration.ofMillis(40));

        assertEquals(4, promotedStore.committedVersion());
        assertEquals("stale", promotedStore.get("deleted"));
        manager.ensureStrongReadReady("shard-0", promotedShard);
        assertEquals("(nil)", promotedStore.get("deleted"));
        assertEquals("restored", promotedStore.get("missing"));
        assertTrue(promotedStore.committedMutations().stream()
                .anyMatch(mutation -> mutation.getKey().equals(ByteString.copyFromUtf8("deleted"))
                        && mutation.getKind() == MutationKind.DELETE));

        manager.close();
        manager = null;
        promotedStore.shutdown();
        ShardKVStore restartedStore = newStore("holes-promoted");
        assertEquals("(nil)", restartedStore.get("deleted"));
        assertEquals("restored", restartedStore.get("missing"));
        assertEquals(4, restartedStore.committedVersion());

        manager = new ReplicationManager(
                "node-3",
                cache(promotedShard),
                new FixedRegistry(tempDir.resolve("holes-restarted-registry"), restartedStore),
                promotedClient,
                Duration.ofMillis(40));
        manager.ensureStrongReadReady("shard-0", promotedShard);
        assertEquals("(nil)", restartedStore.get("deleted"));
        restartedStore.shutdown();
    }

    @Test
    void paginatedHoleTransferRefusesStrongServiceUntilCompletenessIsEstablished() {
        ShardRecord promotedShard = promotedShard();
        ShardKVStore promotedStore = newStore("paginated-promoted");
        List<ReplicatedMutation> retainedState = new ArrayList<>();
        for (int version = 1; version <= 1_025; version++) {
            retainedState.add(mutation(version, MutationKind.SET, "key-" + version, "value-" + version));
        }

        FakeReplicaClient promotedClient = new FakeReplicaClient(Map.of());
        promotedClient.replicaStates.put("node-1:9000", retainedState);
        promotedClient.partitioned.add("node-2:9000");
        manager = new ReplicationManager(
                "node-3",
                cache(promotedShard),
                new FixedRegistry(tempDir.resolve("paginated-registry"), promotedStore),
                promotedClient,
                Duration.ofSeconds(10));

        assertThrows(NodeUnavailableException.class, () -> manager.ensureStrongReadReady("shard-0", promotedShard));
        assertEquals(8, promotedClient.stateFetchCount("node-1:9000"));
        assertEquals(128, promotedClient.largestRequestedStateBatch.get());
        assertEquals("(nil)", promotedStore.get("key-1025"));

        manager.ensureStrongReadReady("shard-0", promotedShard);
        assertEquals(9, promotedClient.stateFetchCount("node-1:9000"));
        assertEquals("value-1025", promotedStore.get("key-1025"));
        assertEquals(1_025, promotedStore.committedVersion());
        promotedStore.shutdown();
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

    private static ShardRecord promotedShard() {
        return ShardRecord.newBuilder()
                .setShardId("shard-0")
                .setEpoch(4)
                .setLeader("node-3")
                .addReplicas("node-3")
                .addReplicas("node-1")
                .addReplicas("node-2")
                .build();
    }

    private static ShardRecord repairShard() {
        return ShardRecord.newBuilder()
                .setShardId("shard-0")
                .setEpoch(3)
                .setLeader("node-1")
                .addReplicas("node-1")
                .addReplicas("node-2")
                .build();
    }

    private static AppConfig.LimitsConfig limits(int maxValueBytes, int maxMessageBytes, int maxBatchEntries) {
        AppConfig.LimitsConfig limits = new AppConfig.LimitsConfig();
        limits.setMaxKeyBytes(64);
        limits.setMaxValueBytes(maxValueBytes);
        limits.setMaxMessageBytes(maxMessageBytes);
        limits.setMaxBatchEntries(maxBatchEntries);
        return limits;
    }

    private static ReplicatedMutation mutation(long version, MutationKind kind, String key, String value) {
        return ReplicatedMutation.newBuilder()
                .setRequestId("request-" + version)
                .setShardId("shard-0")
                .setEpoch(3)
                .setVersion(version)
                .setKind(kind)
                .setKey(ByteString.copyFromUtf8(key))
                .setValue(ByteString.copyFromUtf8(value))
                .setOriginNodeId("node-1")
                .setCreateTimeMs(1_000 + version)
                .setUpdateTimeMs(1_000 + version)
                .build();
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
        private final Map<String, List<ReplicatedMutation>> replicaStates = new ConcurrentHashMap<>();
        private final Map<String, AtomicInteger> stateFetchCounts = new ConcurrentHashMap<>();
        private final AtomicInteger largestRequestedStateBatch = new AtomicInteger();
        private final Set<String> partitioned = ConcurrentHashMap.newKeySet();
        private final Set<String> delayed = ConcurrentHashMap.newKeySet();
        private final Set<String> commitFailed = ConcurrentHashMap.newKeySet();
        private final Set<String> prepareResponseLost = ConcurrentHashMap.newKeySet();
        private final Set<String> abortDropped = ConcurrentHashMap.newKeySet();
        private final Set<String> latePrepare = ConcurrentHashMap.newKeySet();
        private final CountDownLatch delayedRequestStarted = new CountDownLatch(1);
        private final CountDownLatch delayedRequestRelease = new CountDownLatch(1);
        private final CountDownLatch delayedRequestCompleted = new CountDownLatch(1);
        private final CountDownLatch latePrepareStarted = new CountDownLatch(1);
        private final CountDownLatch latePrepareRelease = new CountDownLatch(1);
        private final CountDownLatch latePrepareCompleted = new CountDownLatch(1);
        private final CountDownLatch abortCompleted = new CountDownLatch(1);

        private FakeReplicaClient(Map<String, ShardKVStore> stores) {
            super(Duration.ofMillis(20));
            this.stores = stores;
        }

        @Override
        public ReplicationAck replicateMutation(String targetAddress, ReplicateMutationRequest request) {
            unavailableIfNeeded(targetAddress, true);
            if (request.getPhase() == ReplicationPhase.ABORT && abortDropped.contains(targetAddress)) {
                throw new IllegalStateException("abort dropped");
            }
            if (request.getPhase() == ReplicationPhase.PREPARE && latePrepare.contains(targetAddress)) {
                latePrepareStarted.countDown();
                awaitIgnoringInterrupts(latePrepareRelease);
            }
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
            if (request.getPhase() == ReplicationPhase.PREPARE && latePrepare.contains(targetAddress)) {
                latePrepareCompleted.countDown();
            }
            if (request.getPhase() == ReplicationPhase.ABORT) {
                abortCompleted.countDown();
            }
            if (request.getPhase() == ReplicationPhase.PREPARE && prepareResponseLost.contains(targetAddress)) {
                throw new IllegalStateException("prepare acknowledgement lost");
            }
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
            unavailableIfNeeded(targetAddress, false);
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
            unavailableIfNeeded(targetAddress, false);
            stateFetchCounts
                    .computeIfAbsent(targetAddress, ignored -> new AtomicInteger())
                    .incrementAndGet();
            largestRequestedStateBatch.accumulateAndGet(request.getMaxMutations(), Math::max);
            List<ReplicatedMutation> configuredState = replicaStates.get(targetAddress);
            ShardKVStore store = stores.get(targetAddress);
            var mutations = configuredState == null
                    ? store.committedMutationsAfter(request.getAfterVersion(), Math.max(1, request.getMaxMutations()))
                    : configuredState.stream()
                            .filter(mutation -> mutation.getVersion() > request.getAfterVersion())
                            .limit(Math.max(1, request.getMaxMutations()))
                            .toList();
            long lastVersion = mutations.isEmpty()
                    ? request.getAfterVersion()
                    : mutations.getLast().getVersion();
            boolean hasMore = configuredState == null
                    ? !store.committedMutationsAfter(lastVersion, 1).isEmpty()
                    : configuredState.stream().anyMatch(mutation -> mutation.getVersion() > lastVersion);
            long committedVersion = configuredState == null
                    ? store.committedVersion()
                    : configuredState.stream()
                            .mapToLong(ReplicatedMutation::getVersion)
                            .max()
                            .orElse(0);
            return ReplicaStateResponse.newBuilder()
                    .setSuccess(true)
                    .setDurable(true)
                    .addAllCommittedMutations(mutations)
                    .setHasMore(hasMore)
                    .setCommittedVersion(committedVersion)
                    .build();
        }

        private int stateFetchCount(String targetAddress) {
            return stateFetchCounts
                    .getOrDefault(targetAddress, new AtomicInteger())
                    .get();
        }

        private void unavailableIfNeeded(String targetAddress, boolean delayReplication) {
            if (partitioned.contains(targetAddress)) {
                throw new IllegalStateException("partitioned");
            }
            if (delayReplication && delayed.contains(targetAddress)) {
                delayedRequestStarted.countDown();
                awaitIgnoringInterrupts(delayedRequestRelease);
                delayedRequestCompleted.countDown();
            }
        }

        private void releaseDelayedRequest() {
            delayedRequestRelease.countDown();
        }

        private void releaseLatePrepare() {
            latePrepareRelease.countDown();
        }

        private static void awaitIgnoringInterrupts(CountDownLatch release) {
            while (true) {
                try {
                    release.await();
                    break;
                } catch (InterruptedException e) {
                    // Model a transport that does not honor cancellation and completes after the phase deadline.
                }
            }
        }
    }

    private static final class ReceiverValidatingReplicaClient extends ReplicaWriteClient {
        private final KVServiceImpl receiver;
        private final List<ReplicaRepairRequest> repairRequests = new ArrayList<>();

        private ReceiverValidatingReplicaClient(KVServiceImpl receiver) {
            super(Duration.ofMillis(20));
            this.receiver = receiver;
        }

        @Override
        public ReplicaRepairResponse repairReplica(String targetAddress, ReplicaRepairRequest request) {
            repairRequests.add(request);
            CapturingObserver<ReplicaRepairResponse> observer = new CapturingObserver<>();
            receiver.repairReplica(request, observer);
            return observer.value;
        }

        @Override
        public ReplicaStateResponse fetchReplicaState(String targetAddress, ReplicaStateRequest request) {
            return ReplicaStateResponse.newBuilder()
                    .setSuccess(true)
                    .setDurable(true)
                    .build();
        }
    }

    private static final class CapturingObserver<T> implements StreamObserver<T> {
        private T value;

        @Override
        public void onNext(T value) {
            this.value = value;
        }

        @Override
        public void onError(Throwable throwable) {
            throw new AssertionError(throwable);
        }

        @Override
        public void onCompleted() {}
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
