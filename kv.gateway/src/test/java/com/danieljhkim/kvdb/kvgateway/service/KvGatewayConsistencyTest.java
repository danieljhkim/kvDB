package com.danieljhkim.kvdb.kvgateway.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.danieljhkim.kvdb.kvcommon.cache.ShardMapCache;
import com.danieljhkim.kvdb.kvcommon.config.AppConfig;
import com.danieljhkim.kvdb.kvcommon.exception.NodeUnavailableException;
import com.danieljhkim.kvdb.kvcommon.grpc.GlobalExceptionInterceptor;
import com.danieljhkim.kvdb.kvcommon.grpc.GrpcIdentity;
import com.danieljhkim.kvdb.kvcommon.grpc.GrpcPeerIdentity;
import com.danieljhkim.kvdb.kvcommon.limits.KvRequestLimits;
import com.danieljhkim.kvdb.kvgateway.cache.NodeFailureTracker;
import com.danieljhkim.kvdb.kvgateway.client.NodeConnectionPool;
import com.danieljhkim.kvdb.kvgateway.retry.RequestExecutor;
import com.danieljhkim.kvdb.kvgateway.retry.RetryPolicy;
import com.danieljhkim.kvdb.proto.coordinator.ClusterState;
import com.danieljhkim.kvdb.proto.coordinator.NodeRecord;
import com.danieljhkim.kvdb.proto.coordinator.NodeStatus;
import com.danieljhkim.kvdb.proto.coordinator.PartitioningConfig;
import com.danieljhkim.kvdb.proto.coordinator.ShardRecord;
import com.danieljhkim.kvdb.proto.gateway.BatchGetRequest;
import com.danieljhkim.kvdb.proto.gateway.BatchGetResponse;
import com.danieljhkim.kvdb.proto.gateway.Consistency;
import com.danieljhkim.kvdb.proto.gateway.DeleteRequest;
import com.danieljhkim.kvdb.proto.gateway.DeleteResponse;
import com.danieljhkim.kvdb.proto.gateway.GetRequest;
import com.danieljhkim.kvdb.proto.gateway.GetResponse;
import com.danieljhkim.kvdb.proto.gateway.PutRequest;
import com.danieljhkim.kvdb.proto.gateway.PutResponse;
import com.danieljhkim.kvdb.proto.gateway.ReadOptions;
import com.danieljhkim.kvdb.proto.gateway.RequestContext;
import com.danieljhkim.kvdb.proto.gateway.Status;
import com.danieljhkim.kvdb.proto.gateway.WriteOptions;
import com.google.protobuf.ByteString;
import com.kvdb.proto.kvstore.KVServiceGrpc;
import com.kvdb.proto.kvstore.KeyValueRequest;
import com.kvdb.proto.kvstore.SetResponse;
import com.kvdb.proto.kvstore.ValueResponse;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

class KvGatewayConsistencyTest {

    @Test
    void strongReadUsesOnlyLeaderWhileEventualReadPrefersFollower() {
        CapturingExecutor executor = new CapturingExecutor();
        KvGatewayServiceImpl service = new KvGatewayServiceImpl(cache(), executor);

        List<NodeRecord> strong = service.getNodesForRead("shard-0", Consistency.STRONG);
        List<NodeRecord> eventual = service.getNodesForRead("shard-0", Consistency.EVENTUAL);

        assertEquals(
                List.of("node-1"), strong.stream().map(NodeRecord::getNodeId).toList());
        assertEquals(
                List.of("node-2", "node-1"),
                eventual.stream().map(NodeRecord::getNodeId).toList());
    }

    @Test
    void eventualReadExposesServingReplicaAppliedVersion() {
        CapturingExecutor executor = new CapturingExecutor();
        executor.nextResult = RequestExecutor.ExecutionResult.success(
                ValueResponse.newBuilder()
                        .setValue(ByteString.copyFromUtf8("value"))
                        .setFound(true)
                        .setVersion(7)
                        .setAppliedVersion(11)
                        .build(),
                "node-2:9000");
        KvGatewayServiceImpl service = new KvGatewayServiceImpl(cache(), executor);
        CapturingObserver<GetResponse> observer = new CapturingObserver<>();

        service.get(
                GetRequest.newBuilder()
                        .setKey(ByteString.copyFromUtf8("key"))
                        .setOptions(ReadOptions.newBuilder().setConsistency(Consistency.EVENTUAL))
                        .build(),
                observer);

        assertEquals(Status.Code.OK, observer.value.getStatus().getCode());
        assertEquals(7, observer.value.getKv().getVersion());
        assertEquals(11, observer.value.getAppliedVersion());
        assertEquals("node-2", executor.candidates.getFirst().getNodeId());
    }

    @Test
    void batchGetUsesTheSameStrongAndEventualRoutingSeamAsUnaryGet() {
        CapturingExecutor executor = new CapturingExecutor();
        executor.nextResult = RequestExecutor.ExecutionResult.success(
                ValueResponse.newBuilder()
                        .setFound(true)
                        .setVersion(9)
                        .setAppliedVersion(12)
                        .build(),
                "node:9000");
        AppConfig.LimitsConfig config = new AppConfig.LimitsConfig();
        config.setMaxBatchGetConcurrency(1);
        KvGatewayServiceImpl service = new KvGatewayServiceImpl(cache(), executor, new KvRequestLimits(config));

        CapturingObserver<BatchGetResponse> strong = new CapturingObserver<>();
        service.batchGet(
                BatchGetRequest.newBuilder()
                        .addKeys(ByteString.copyFromUtf8("strong"))
                        .setOptions(ReadOptions.newBuilder().setConsistency(Consistency.STRONG))
                        .build(),
                strong);
        assertEquals(
                List.of("node-1"),
                executor.candidates.stream().map(NodeRecord::getNodeId).toList());
        assertEquals(12, strong.value.getResults(0).getAppliedVersion());

        CapturingObserver<BatchGetResponse> eventual = new CapturingObserver<>();
        service.batchGet(
                BatchGetRequest.newBuilder()
                        .addKeys(ByteString.copyFromUtf8("eventual"))
                        .setOptions(ReadOptions.newBuilder().setConsistency(Consistency.EVENTUAL))
                        .build(),
                eventual);
        assertEquals(
                List.of("node-2", "node-1"),
                executor.candidates.stream().map(NodeRecord::getNodeId).toList());
        assertEquals(12, eventual.value.getResults(0).getAppliedVersion());
    }

    @Test
    void writeRequiresStableRequestIdBeforeExecution() {
        CapturingExecutor executor = new CapturingExecutor();
        KvGatewayServiceImpl service = new KvGatewayServiceImpl(cache(), executor);
        CapturingObserver<PutResponse> observer = new CapturingObserver<>();

        service.put(
                PutRequest.newBuilder()
                        .setKey(ByteString.copyFromUtf8("key"))
                        .setValue(ByteString.copyFromUtf8("value"))
                        .build(),
                observer);

        assertEquals(Status.Code.INVALID_ARGUMENT, observer.value.getStatus().getCode());
        assertEquals(0, executor.calls);
        assertEquals("stable-id", KvGatewayServiceImpl.requireWriteRequestId("stable-id"));
    }

    @Test
    void nonIdempotentAmbiguousWriteReturnsDocumentedOutcomeWithoutReplay() {
        CapturingExecutor executor = new CapturingExecutor();
        executor.nextResult = RequestExecutor.ExecutionResult.ambiguous(
                io.grpc.Status.Code.DEADLINE_EXCEEDED,
                "Write outcome is unknown after timeout; request was not replayed",
                "node-1:9000");
        KvGatewayServiceImpl service = new KvGatewayServiceImpl(cache(), executor);
        CapturingObserver<PutResponse> observer = new CapturingObserver<>();

        runAsClient(() -> service.put(
                PutRequest.newBuilder()
                        .setCtx(RequestContext.newBuilder().setRequestId("stable-id"))
                        .setKey(ByteString.copyFromUtf8("key"))
                        .setValue(ByteString.copyFromUtf8("value"))
                        .setOptions(WriteOptions.newBuilder().setRequireIdempotency(false))
                        .build(),
                observer));

        assertEquals(
                Status.Code.WRITE_OUTCOME_UNKNOWN, observer.value.getStatus().getCode());
        assertFalse(executor.replaySafe);
        assertEquals(1, executor.calls);
    }

    @Test
    void nodeWireOutcomesMapToPutAndDeleteApplicationStatuses() throws Exception {
        for (boolean definite : List.of(true, false)) {
            AtomicInteger calls = new AtomicInteger();
            Server server = ServerBuilder.forPort(0)
                    .directExecutor()
                    .intercept(new GlobalExceptionInterceptor())
                    .addService(new KVServiceGrpc.KVServiceImplBase() {
                        private void reject() {
                            calls.incrementAndGet();
                            if (definite) {
                                throw NodeUnavailableException.rejectedBeforeMutation(
                                        "Leader reconciliation quorum not reached", "shard-0");
                            }
                            throw new NodeUnavailableException("Replication commit quorum not reached", "shard-0");
                        }

                        @Override
                        public void set(KeyValueRequest request, StreamObserver<SetResponse> observer) {
                            reject();
                        }

                        @Override
                        public void delete(
                                com.kvdb.proto.kvstore.DeleteRequest request,
                                StreamObserver<com.kvdb.proto.kvstore.DeleteResponse> observer) {
                            reject();
                        }
                    })
                    .build()
                    .start();
            ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", server.getPort())
                    .usePlaintext()
                    .build();
            try {
                NodeConnectionPool pool = new NodeConnectionPool() {
                    @Override
                    public KVServiceGrpc.KVServiceBlockingStub getStub(String address) {
                        return KVServiceGrpc.newBlockingStub(channel);
                    }
                };
                RequestExecutor executor = new RequestExecutor(
                        pool,
                        new NodeFailureTracker(5000),
                        RetryPolicy.builder()
                                .maxAttempts(2)
                                .initialBackoffMs(0)
                                .jitterPercent(0)
                                .build(),
                        5000);
                KvGatewayServiceImpl service = new KvGatewayServiceImpl(cache(), executor);
                CapturingObserver<PutResponse> put = new CapturingObserver<>();
                runAsClient(() -> service.put(
                        PutRequest.newBuilder()
                                .setCtx(RequestContext.newBuilder().setRequestId("put-id"))
                                .setKey(ByteString.copyFromUtf8("key"))
                                .setValue(ByteString.copyFromUtf8("value"))
                                .setOptions(WriteOptions.newBuilder().setRequireIdempotency(false))
                                .build(),
                        put));
                CapturingObserver<DeleteResponse> delete = new CapturingObserver<>();
                runAsClient(() -> service.delete(
                        DeleteRequest.newBuilder()
                                .setCtx(RequestContext.newBuilder().setRequestId("delete-id"))
                                .setKey(ByteString.copyFromUtf8("key"))
                                .setOptions(WriteOptions.newBuilder().setRequireIdempotency(false))
                                .build(),
                        delete));
                Status.Code expected = definite ? Status.Code.UNAVAILABLE : Status.Code.WRITE_OUTCOME_UNKNOWN;
                assertEquals(expected, put.value.getStatus().getCode());
                assertEquals(expected, delete.value.getStatus().getCode());
                assertEquals(definite ? 4 : 2, calls.get());
            } finally {
                channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
                server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void idempotentWriteEnablesRetryOnlyWithCallerRequestId() {
        CapturingExecutor executor = new CapturingExecutor();
        executor.nextResult = RequestExecutor.ExecutionResult.success(
                SetResponse.newBuilder().setSuccess(true).setVersion(3).build(), "node-1:9000");
        KvGatewayServiceImpl service = new KvGatewayServiceImpl(cache(), executor);
        CapturingObserver<PutResponse> observer = new CapturingObserver<>();

        runAsClient(() -> service.put(
                PutRequest.newBuilder()
                        .setCtx(RequestContext.newBuilder().setRequestId("stable-id"))
                        .setKey(ByteString.copyFromUtf8("key"))
                        .setValue(ByteString.copyFromUtf8("value"))
                        .setOptions(WriteOptions.newBuilder().setRequireIdempotency(true))
                        .build(),
                observer));

        assertEquals(Status.Code.OK, observer.value.getStatus().getCode());
        assertTrue(executor.replaySafe);
    }

    private static ShardMapCache cache() {
        ShardMapCache cache = new ShardMapCache();
        cache.refreshFromFullState(ClusterState.newBuilder()
                .setMapVersion(1)
                .setPartitioning(PartitioningConfig.newBuilder().setNumShards(1).setReplicationFactor(2))
                .putNodes("node-1", node("node-1", "node-1:9000"))
                .putNodes("node-2", node("node-2", "node-2:9000"))
                .putShards(
                        "shard-0",
                        ShardRecord.newBuilder()
                                .setShardId("shard-0")
                                .setEpoch(2)
                                .setLeader("node-1")
                                .addReplicas("node-1")
                                .addReplicas("node-2")
                                .build())
                .build());
        return cache;
    }

    private static NodeRecord node(String id, String address) {
        return NodeRecord.newBuilder()
                .setNodeId(id)
                .setAddress(address)
                .setStatus(NodeStatus.ALIVE)
                .build();
    }

    private static void runAsClient(Runnable operation) {
        Context.current()
                .withValue(
                        GrpcPeerIdentity.CURRENT,
                        new GrpcIdentity(GrpcIdentity.Role.EXTERNAL_CLIENT, "tenant", "alice"))
                .run(operation);
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

    private static final class CapturingExecutor extends RequestExecutor {
        private RequestExecutor.ExecutionResult<?> nextResult;
        private List<NodeRecord> candidates = List.of();
        private boolean replaySafe;
        private int calls;

        private CapturingExecutor() {
            super(new NodeConnectionPool(), new NodeFailureTracker(), RetryPolicy.defaults(), 100);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> ExecutionResult<T> executeWithRetry(
                String shardId,
                boolean isWrite,
                boolean replaySafe,
                Function<KVServiceGrpc.KVServiceBlockingStub, T> operation,
                Supplier<List<NodeRecord>> nodeSupplier) {
            calls++;
            this.replaySafe = replaySafe;
            this.candidates = nodeSupplier.get();
            return (ExecutionResult<T>) nextResult;
        }
    }
}
