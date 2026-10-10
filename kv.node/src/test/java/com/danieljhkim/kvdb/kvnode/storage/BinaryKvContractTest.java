package com.danieljhkim.kvdb.kvnode.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.danieljhkim.kvdb.kvcommon.cache.ShardMapCache;
import com.danieljhkim.kvdb.kvcommon.exception.RequestIdConflictException;
import com.danieljhkim.kvdb.kvnode.cluster.ReplicationManager;
import com.danieljhkim.kvdb.proto.coordinator.ClusterState;
import com.danieljhkim.kvdb.proto.coordinator.PartitioningConfig;
import com.danieljhkim.kvdb.proto.coordinator.ShardRecord;
import com.google.protobuf.ByteString;
import com.kvdb.proto.kvstore.MutationKind;
import com.kvdb.proto.kvstore.MutationOutcome;
import com.kvdb.proto.kvstore.ReplicatedMutation;
import com.kvdb.proto.kvstore.WriteDurability;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BinaryKvContractTest {

    @TempDir
    Path tempDir;

    @Test
    void binaryPayloadSurvivesReplicationWalSnapshotAndBothRecoveryPaths() throws Exception {
        ByteString key = ByteString.copyFrom(new byte[] {0, (byte) 0xff, (byte) 0x80, 0x41});
        ByteString value = ByteString.copyFrom(new byte[] {(byte) 0xfe, 0, (byte) 0xc3, 0x28});
        String snapshot = tempDir.resolve("binary.json").toString();
        String wal = tempDir.resolve("binary.wal").toString();
        ShardKVStore store = new ShardKVStore("shard-0", snapshot, wal, 100, false);

        ReplicatedMutation mutation = store.prepareNewMutation(
                "binary-request",
                1,
                MutationKind.SET,
                key,
                value,
                "leader",
                0,
                OptionalLong.empty(),
                false,
                System.currentTimeMillis());
        assertTrue(store.commitMutation(mutation).success());
        assertEquals(mutation, ReplicatedMutation.parseFrom(mutation.toByteArray()));
        assertRead(store, key, value, 1);

        ShardKVStore walRecovered = new ShardKVStore("shard-0", snapshot, wal, 100, false);
        assertRead(walRecovered, key, value, 1);
        walRecovered.persistNow();
        walRecovered.shutdown();
        store.shutdown();

        ShardKVStore snapshotRecovered = new ShardKVStore("shard-0", snapshot, wal, 100, false);
        assertRead(snapshotRecovered, key, value, 1);
        snapshotRecovered.shutdown();
    }

    @Test
    void versionsConditionsIdempotencyDeleteAndExpiryAreExplicit() {
        ShardKVStore store = newStore("conditions");
        ByteString key = ByteString.copyFromUtf8("key");
        long now = System.currentTimeMillis();

        ReplicatedMutation first = store.prepareNewMutation(
                "request-1",
                1,
                MutationKind.SET,
                key,
                ByteString.copyFromUtf8("one"),
                "leader",
                0,
                OptionalLong.empty(),
                true,
                now);
        assertTrue(store.commitMutation(first).success());
        assertEquals(1, first.getVersion());
        assertEquals(
                first.getVersion(),
                store.prepareNewMutation(
                                "request-1",
                                1,
                                MutationKind.SET,
                                key,
                                ByteString.copyFromUtf8("one"),
                                "leader",
                                0,
                                OptionalLong.empty(),
                                true,
                                now)
                        .getVersion());

        ShardKVStore.ConditionalMutationException exists = assertThrows(
                ShardKVStore.ConditionalMutationException.class,
                () -> store.prepareNewMutation(
                        "request-create",
                        1,
                        MutationKind.SET,
                        key,
                        ByteString.copyFromUtf8("two"),
                        "leader",
                        0,
                        OptionalLong.empty(),
                        true,
                        now + 1));
        assertEquals(MutationOutcome.ALREADY_EXISTS, exists.outcome());

        ShardKVStore.ConditionalMutationException mismatch = assertThrows(
                ShardKVStore.ConditionalMutationException.class,
                () -> store.prepareNewMutation(
                        "request-cas-bad",
                        1,
                        MutationKind.SET,
                        key,
                        ByteString.copyFromUtf8("two"),
                        "leader",
                        0,
                        OptionalLong.of(99),
                        false,
                        now + 2));
        assertEquals(MutationOutcome.VERSION_MISMATCH, mismatch.outcome());

        ReplicatedMutation second = store.prepareNewMutation(
                "request-2",
                1,
                MutationKind.SET,
                key,
                ByteString.copyFromUtf8("two"),
                "leader",
                0,
                OptionalLong.of(1),
                false,
                now + 3);
        assertTrue(store.commitMutation(second).success());
        ReplicatedMutation deleted = store.prepareNewMutation(
                "request-3",
                1,
                MutationKind.DELETE,
                key,
                ByteString.EMPTY,
                "leader",
                0,
                OptionalLong.of(2),
                false,
                now + 4);
        assertTrue(store.commitMutation(deleted).success());
        assertFalse(store.read(key).found());
        assertEquals(3, store.read(key).version());

        ReplicatedMutation expired = store.prepareNewMutation(
                "request-4",
                1,
                MutationKind.SET,
                key,
                ByteString.copyFromUtf8("short-lived"),
                "leader",
                1,
                OptionalLong.empty(),
                false,
                now - 1_000);
        assertTrue(store.commitMutation(expired).success());
        assertFalse(store.read(key).found());
        store.shutdown();
    }

    @Test
    void identicalCommittedReplayAfterHigherEpochReturnsOriginalVersionAcrossRestart() {
        ByteString key = ByteString.copyFromUtf8("epoch-key");
        ByteString value = ByteString.copyFromUtf8("epoch-value");
        ByteString deletedKey = ByteString.copyFromUtf8("epoch-deleted");
        long now = System.currentTimeMillis();
        ShardKVStore store = newStore("epoch-retry");

        ReplicatedMutation put = store.prepareNewMutation(
                "epoch-put", 1, MutationKind.SET, key, value, "old-leader", 60_000, OptionalLong.empty(), true, now);
        assertTrue(store.commitMutation(put).success());
        ReplicatedMutation seed = store.prepareNewMutation(
                "epoch-seed",
                1,
                MutationKind.SET,
                deletedKey,
                value,
                "old-leader",
                0,
                OptionalLong.empty(),
                false,
                now);
        assertTrue(store.commitMutation(seed).success());
        ReplicatedMutation delete = store.prepareNewMutation(
                "epoch-delete",
                1,
                MutationKind.DELETE,
                deletedKey,
                ByteString.EMPTY,
                "old-leader",
                0,
                OptionalLong.of(seed.getVersion()),
                false,
                now);
        assertTrue(store.commitMutation(delete).success());
        // A replica-set change advances the shard epoch before the client retries.
        ReplicatedMutation newerEpoch = store.prepareNewMutation(
                "epoch-2-write",
                2,
                MutationKind.SET,
                ByteString.copyFromUtf8("other"),
                value,
                "new-leader",
                0,
                OptionalLong.empty(),
                false,
                now + 1);
        assertTrue(store.commitMutation(newerEpoch).success());
        assertEquals(2, store.shardEpoch());
        assertEquals(4, store.committedVersion());

        assertIdenticalReplaysReturnOriginals(store, put, delete, now + 5);
        assertConflictingReplaysAreRejected(store, now + 5);
        store.shutdown();

        ShardKVStore restarted = newStore("epoch-retry");
        assertIdenticalReplaysReturnOriginals(restarted, put, delete, now + 10);
        assertConflictingReplaysAreRejected(restarted, now + 10);
        assertEquals(4, restarted.committedVersion());
        assertRead(restarted, key, value, put.getVersion());
        assertFalse(restarted.read(deletedKey).found());
        assertEquals(delete.getVersion(), restarted.read(deletedKey).version());
        restarted.shutdown();
    }

    @Test
    void crossEpochReplayOfAnUncommittedPrepareStaysFenced() {
        ByteString key = ByteString.copyFromUtf8("orphan");
        ByteString value = ByteString.copyFromUtf8("hidden");
        long now = System.currentTimeMillis();
        ShardKVStore store = newStore("epoch-orphan");

        ReplicatedMutation orphan = store.prepareNewMutation(
                "epoch-orphan", 1, MutationKind.SET, key, value, "old-leader", 0, OptionalLong.empty(), false, now);
        ReplicatedMutation successor = store.prepareNewMutation(
                "epoch-successor",
                2,
                MutationKind.SET,
                ByteString.copyFromUtf8("successor"),
                value,
                "new-leader",
                0,
                OptionalLong.empty(),
                false,
                now);
        assertTrue(store.commitMutation(successor).success());

        // The replay keeps the old mutation's identity, so the newer commit still fences it.
        assertThrows(
                IllegalStateException.class,
                () -> store.prepareNewMutation(
                        "epoch-orphan",
                        3,
                        MutationKind.SET,
                        key,
                        value,
                        "newest-leader",
                        0,
                        OptionalLong.empty(),
                        false,
                        now + 1));
        assertFalse(store.commitMutation(orphan).success());
        assertFalse(store.isCommitted("epoch-orphan"));
        assertFalse(store.read(key).found());
        assertFalse(store.prepareMutation(orphan.toBuilder()
                        .setRequestId("epoch-stale")
                        .setVersion(successor.getVersion() + 1)
                        .build())
                .success());
        assertEquals(successor.getVersion(), store.committedVersion());
        store.shutdown();
    }

    private static void assertIdenticalReplaysReturnOriginals(
            ShardKVStore store, ReplicatedMutation put, ReplicatedMutation delete, long nowMs) {
        long committedVersion = store.committedVersion();
        ReplicatedMutation replayedPut = store.prepareNewMutation(
                "epoch-put",
                3,
                MutationKind.SET,
                put.getKey(),
                put.getValue(),
                "newest-leader",
                put.getTtlMs(),
                OptionalLong.empty(),
                true,
                nowMs);
        ReplicatedMutation replayedDelete = store.prepareNewMutation(
                "epoch-delete",
                3,
                MutationKind.DELETE,
                delete.getKey(),
                ByteString.EMPTY,
                "newest-leader",
                0,
                OptionalLong.of(delete.getIfVersionEquals()),
                false,
                nowMs);

        assertEquals(put, replayedPut);
        assertEquals(delete, replayedDelete);
        assertTrue(store.isCommitted("epoch-put"));
        assertTrue(store.isCommitted("epoch-delete"));
        assertEquals("already committed", store.commitMutation(replayedPut).message());
        assertEquals("already committed", store.commitMutation(replayedDelete).message());
        assertEquals(committedVersion, store.committedVersion());
        assertRead(store, put.getKey(), put.getValue(), put.getVersion());
        assertFalse(store.read(delete.getKey()).found());
        // A stale or rewritten copy of the committed mutation is still fenced as a different mutation.
        assertFalse(store.prepareMutation(put.toBuilder().setEpoch(3).build()).success());
        assertFalse(store.repairMutation(put.toBuilder().setEpoch(3).build()).success());
    }

    private static void assertConflictingReplaysAreRejected(ShardKVStore store, long nowMs) {
        ByteString key = ByteString.copyFromUtf8("epoch-key");
        ByteString value = ByteString.copyFromUtf8("epoch-value");
        List<ConflictingReplay> conflicts = List.of(
                new ConflictingReplay(
                        MutationKind.SET,
                        ByteString.copyFromUtf8("other-key"),
                        value,
                        60_000,
                        OptionalLong.empty(),
                        true),
                new ConflictingReplay(
                        MutationKind.SET, key, ByteString.copyFromUtf8("other"), 60_000, OptionalLong.empty(), true),
                new ConflictingReplay(MutationKind.DELETE, key, ByteString.EMPTY, 0, OptionalLong.empty(), false),
                new ConflictingReplay(MutationKind.SET, key, value, 1, OptionalLong.empty(), true),
                new ConflictingReplay(MutationKind.SET, key, value, 60_000, OptionalLong.empty(), false),
                new ConflictingReplay(MutationKind.SET, key, value, 60_000, OptionalLong.of(0), true));
        for (ConflictingReplay conflict : conflicts) {
            for (long epoch : new long[] {1, 3}) {
                assertThrows(
                        RequestIdConflictException.class,
                        () -> store.prepareNewMutation(
                                "epoch-put",
                                epoch,
                                conflict.kind(),
                                conflict.key(),
                                conflict.value(),
                                "newest-leader",
                                conflict.ttlMs(),
                                conflict.expectedVersion(),
                                conflict.ifNotExists(),
                                nowMs));
            }
        }
        assertThrows(
                RequestIdConflictException.class,
                () -> store.prepareNewMutation(
                        "epoch-delete",
                        3,
                        MutationKind.DELETE,
                        ByteString.copyFromUtf8("epoch-deleted"),
                        ByteString.EMPTY,
                        "newest-leader",
                        0,
                        OptionalLong.empty(),
                        false,
                        nowMs));
    }

    private record ConflictingReplay(
            MutationKind kind,
            ByteString key,
            ByteString value,
            long ttlMs,
            OptionalLong expectedVersion,
            boolean ifNotExists) {}

    @Test
    void concurrentCreateOnlyRaceHasExactlyOneWinner() throws Exception {
        ShardRecord shard = ShardRecord.newBuilder()
                .setShardId("shard-0")
                .setEpoch(1)
                .setLeader("node-1")
                .addReplicas("node-1")
                .build();
        ShardMapCache cache = new ShardMapCache();
        cache.refreshFromFullState(ClusterState.newBuilder()
                .setMapVersion(1)
                .setPartitioning(PartitioningConfig.newBuilder().setNumShards(1).setReplicationFactor(1))
                .putShards("shard-0", shard)
                .build());
        ShardStoreRegistry registry =
                new ShardStoreRegistry(tempDir.resolve("race").toString(), "snapshot.json", "wal.log", 100, false);
        ReplicationManager manager = new ReplicationManager("node-1", cache, registry, null, Duration.ofMillis(50));
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger applied = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();

        try (var executor = Executors.newFixedThreadPool(2)) {
            for (int i = 0; i < 2; i++) {
                int request = i;
                executor.submit(() -> {
                    start.await();
                    try {
                        manager.replicateSet(
                                "shard-0",
                                shard,
                                ByteString.copyFromUtf8("race-key"),
                                ByteString.copyFromUtf8("value-" + request),
                                "race-" + request,
                                WriteDurability.LOCAL_SYNC,
                                0,
                                OptionalLong.empty(),
                                true);
                        applied.incrementAndGet();
                    } catch (ShardKVStore.ConditionalMutationException expected) {
                        rejected.incrementAndGet();
                    }
                    return null;
                });
            }
            start.countDown();
        }

        assertEquals(1, applied.get());
        assertEquals(1, rejected.get());
        assertEquals(
                1,
                registry.getOrCreate("shard-0")
                        .read(ByteString.copyFromUtf8("race-key"))
                        .version());
        manager.close();
        registry.shutdown();
    }

    private ShardKVStore newStore(String name) {
        return new ShardKVStore(
                "shard-0",
                tempDir.resolve(name + ".json").toString(),
                tempDir.resolve(name + ".wal").toString(),
                100,
                false);
    }

    private static void assertRead(ShardKVStore store, ByteString key, ByteString value, long version) {
        ShardKVStore.ReadResult read = store.read(key);
        assertTrue(read.found());
        assertEquals(value, read.value());
        assertEquals(version, read.version());
    }
}
