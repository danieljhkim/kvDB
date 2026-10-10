package com.danieljhkim.kvdb.kvnode.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.danieljhkim.kvdb.kvcommon.cache.ShardMapCache;
import com.danieljhkim.kvdb.kvcommon.grpc.GlobalExceptionInterceptor;
import com.danieljhkim.kvdb.kvnode.client.ReplicaWriteClient;
import com.danieljhkim.kvdb.kvnode.storage.ShardStoreRegistry;
import com.danieljhkim.kvdb.proto.coordinator.ClusterState;
import com.danieljhkim.kvdb.proto.coordinator.NodeRecord;
import com.danieljhkim.kvdb.proto.coordinator.PartitioningConfig;
import com.danieljhkim.kvdb.proto.coordinator.ShardRecord;
import com.google.protobuf.ByteString;
import com.kvdb.proto.kvstore.KVServiceGrpc;
import com.kvdb.proto.kvstore.KeyValueRequest;
import com.kvdb.proto.kvstore.ReplicaStateRequest;
import com.kvdb.proto.kvstore.ReplicaStateResponse;
import com.kvdb.proto.kvstore.ReplicateMutationRequest;
import com.kvdb.proto.kvstore.ReplicationAck;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WriteAdmissionOutcomeTest {
    @TempDir
    Path tempDir;

    @Test
    void rfTwoOutageRejectsTenWritesOverGrpcBeforeAnyPrepare() throws Exception {
        ShardRecord shard = ShardRecord.newBuilder()
                .setShardId("shard-0")
                .setEpoch(2)
                .setLeader("node-1")
                .addReplicas("node-1")
                .addReplicas("node-2")
                .build();
        ShardMapCache cache = new ShardMapCache();
        cache.refreshFromFullState(ClusterState.newBuilder()
                .setMapVersion(1)
                .setPartitioning(PartitioningConfig.newBuilder().setNumShards(1).setReplicationFactor(2))
                .putShards("shard-0", shard)
                .putNodes(
                        "node-1",
                        NodeRecord.newBuilder()
                                .setNodeId("node-1")
                                .setAddress("node-1:9000")
                                .build())
                .putNodes(
                        "node-2",
                        NodeRecord.newBuilder()
                                .setNodeId("node-2")
                                .setAddress("node-2:9000")
                                .build())
                .build());
        AtomicInteger prepares = new AtomicInteger();
        ReplicaWriteClient offlineReplica = new ReplicaWriteClient(Duration.ofMillis(50)) {
            @Override
            public ReplicaStateResponse fetchReplicaState(String target, ReplicaStateRequest request) {
                throw Status.UNAVAILABLE.withDescription("replica offline").asRuntimeException();
            }

            @Override
            public ReplicationAck replicateMutation(String target, ReplicateMutationRequest request) {
                prepares.incrementAndGet();
                throw new AssertionError("admission failure must not send any replication phase");
            }
        };
        ShardStoreRegistry stores = registry();
        KVServiceImpl service = new KVServiceImpl("node-1", cache, stores, offlineReplica, Duration.ofMillis(100));
        Server server = ServerBuilder.forPort(0)
                .directExecutor()
                .intercept(new GlobalExceptionInterceptor())
                .addService(service)
                .build()
                .start();
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", server.getPort())
                .usePlaintext()
                .build();
        try {
            KVServiceGrpc.KVServiceBlockingStub stub = KVServiceGrpc.newBlockingStub(channel);
            for (int i = 0; i < 10; i++) {
                KeyValueRequest request = KeyValueRequest.newBuilder()
                        .setKey(ByteString.copyFromUtf8("down" + i))
                        .setValue(ByteString.copyFromUtf8("value"))
                        .setRequestId("request-" + i)
                        .build();
                StatusRuntimeException failure =
                        assertThrows(StatusRuntimeException.class, () -> stub.withDeadlineAfter(5, TimeUnit.SECONDS)
                                .set(request));
                assertEquals(Status.Code.UNAVAILABLE, failure.getStatus().getCode());
                assertTrue(failure.getStatus().getDescription().contains("Leader reconciliation quorum not reached"));
                assertEquals(
                        GlobalExceptionInterceptor.WRITE_NOT_APPLIED,
                        failure.getTrailers().get(GlobalExceptionInterceptor.WRITE_OUTCOME_KEY));
                assertEquals("(nil)", stores.getOrCreate("shard-0").get("down" + i));
            }
            assertEquals(0, prepares.get());
            assertEquals(0, stores.getOrCreate("shard-0").committedVersion());
        } finally {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            service.shutdownReplication();
            stores.shutdown();
            offlineReplica.shutdown();
        }
        ShardStoreRegistry restarted = registry();
        try {
            for (int i = 0; i < 10; i++) {
                assertEquals("(nil)", restarted.getOrCreate("shard-0").get("down" + i));
            }
        } finally {
            restarted.shutdown();
        }
    }

    private ShardStoreRegistry registry() {
        return new ShardStoreRegistry(tempDir.toString(), "snapshot.json", "wal.log", 100, false);
    }
}
