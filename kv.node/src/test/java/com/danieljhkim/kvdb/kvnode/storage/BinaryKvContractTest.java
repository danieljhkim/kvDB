package com.danieljhkim.kvdb.kvnode.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.danieljhkim.kvdb.kvcommon.cache.ShardMapCache;
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
