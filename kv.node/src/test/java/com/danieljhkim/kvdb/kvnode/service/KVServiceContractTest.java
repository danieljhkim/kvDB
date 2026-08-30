package com.danieljhkim.kvdb.kvnode.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.danieljhkim.kvdb.kvcommon.cache.ShardMapCache;
import com.danieljhkim.kvdb.kvcommon.config.AppConfig;
import com.danieljhkim.kvdb.kvcommon.exception.InvalidRequestException;
import com.danieljhkim.kvdb.kvcommon.exception.PayloadTooLargeException;
import com.danieljhkim.kvdb.kvnode.storage.ShardStoreRegistry;
import com.danieljhkim.kvdb.proto.coordinator.ClusterState;
import com.danieljhkim.kvdb.proto.coordinator.PartitioningConfig;
import com.danieljhkim.kvdb.proto.coordinator.ShardRecord;
import com.google.protobuf.ByteString;
import com.kvdb.proto.kvstore.KeyRequest;
import com.kvdb.proto.kvstore.KeyValueRequest;
import com.kvdb.proto.kvstore.MutationOutcome;
import com.kvdb.proto.kvstore.ReplicaRepairRequest;
import com.kvdb.proto.kvstore.ReplicatedMutation;
import com.kvdb.proto.kvstore.SetResponse;
import com.kvdb.proto.kvstore.ValueResponse;
import com.kvdb.proto.kvstore.WriteDurability;
import io.grpc.stub.StreamObserver;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class KVServiceContractTest {

    @TempDir
    Path tempDir;

    private ShardStoreRegistry registry;
    private KVServiceImpl service;

    @BeforeEach
    void setUp() {
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
        registry = new ShardStoreRegistry(tempDir.toString(), "snapshot.json", "wal.log", 100, false);
        AppConfig.LimitsConfig limits = new AppConfig.LimitsConfig();
        limits.setMaxKeyBytes(2);
        limits.setMaxValueBytes(3);
        limits.setMaxMessageBytes(256);
        limits.setMaxBatchEntries(1);
        service = new KVServiceImpl("node-1", cache, registry, null, Duration.ofMillis(50), limits);
    }

    @AfterEach
    void tearDown() {
        service.shutdownReplication();
        registry.shutdown();
    }

    @Test
    void binaryEmptyValueHeadOnlyVersionsAndIdempotencyArePreserved() {
        ByteString key = ByteString.copyFrom(new byte[] {0, (byte) 0xff});
        ByteString value = ByteString.copyFrom(new byte[] {0, (byte) 0xfe, 1});
        KeyValueRequest request = KeyValueRequest.newBuilder()
                .setKey(key)
                .setValue(value)
                .setRequestId("request-1")
                .setDurability(WriteDurability.LOCAL_SYNC)
                .build();

        CapturingObserver<SetResponse> first = new CapturingObserver<>();
        service.set(request, first);
        CapturingObserver<SetResponse> duplicate = new CapturingObserver<>();
        service.set(request, duplicate);
        assertEquals(MutationOutcome.APPLIED, first.value.getOutcome());
        assertEquals(first.value.getVersion(), duplicate.value.getVersion());

        CapturingObserver<ValueResponse> read = new CapturingObserver<>();
        service.get(KeyRequest.newBuilder().setKey(key).build(), read);
        assertTrue(read.value.getFound());
        assertEquals(value, read.value.getValue());
        assertEquals(first.value.getVersion(), read.value.getVersion());

        CapturingObserver<ValueResponse> head = new CapturingObserver<>();
        service.get(KeyRequest.newBuilder().setKey(key).setHeadOnly(true).build(), head);
        assertTrue(head.value.getFound());
        assertTrue(head.value.getValue().isEmpty());
        assertEquals(read.value.getVersion(), head.value.getVersion());

        CapturingObserver<SetResponse> cas = new CapturingObserver<>();
        service.set(
                KeyValueRequest.newBuilder()
                        .setKey(key)
                        .setValue(ByteString.EMPTY)
                        .setRequestId("request-2")
                        .setDurability(WriteDurability.LOCAL_SYNC)
                        .setIfVersionEquals(first.value.getVersion())
                        .build(),
                cas);
        assertEquals(first.value.getVersion() + 1, cas.value.getVersion());

        CapturingObserver<ValueResponse> empty = new CapturingObserver<>();
        service.get(KeyRequest.newBuilder().setKey(key).build(), empty);
        assertTrue(empty.value.getFound());
        assertTrue(empty.value.getValue().isEmpty());
    }

    @Test
    void fieldAndBatchLimitsFailWithResourceExhaustedBeforeProcessing() {
        assertThrows(
                PayloadTooLargeException.class,
                () -> service.get(
                        KeyRequest.newBuilder()
                                .setKey(ByteString.copyFrom(new byte[] {1, 2, 3}))
                                .build(),
                        new CapturingObserver<>()));

        ReplicatedMutation mutation = ReplicatedMutation.newBuilder()
                .setRequestId("request")
                .setShardId("shard-0")
                .setEpoch(1)
                .setVersion(1)
                .setKey(ByteString.copyFromUtf8("k"))
                .setOriginNodeId("leader")
                .build();
        assertThrows(
                PayloadTooLargeException.class,
                () -> service.repairReplica(
                        ReplicaRepairRequest.newBuilder()
                                .setShardId("shard-0")
                                .setEpoch(1)
                                .addCommittedMutations(mutation)
                                .addCommittedMutations(mutation)
                                .build(),
                        new CapturingObserver<>()));
    }

    @Test
    void unknownInternalDurabilityIsRejectedInsteadOfDefaulted() {
        assertThrows(
                InvalidRequestException.class,
                () -> service.set(
                        KeyValueRequest.newBuilder()
                                .setKey(ByteString.copyFromUtf8("k"))
                                .setValue(ByteString.copyFromUtf8("v"))
                                .setRequestId("unknown-durability")
                                .setDurabilityValue(999)
                                .build(),
                        new CapturingObserver<>()));
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
}
