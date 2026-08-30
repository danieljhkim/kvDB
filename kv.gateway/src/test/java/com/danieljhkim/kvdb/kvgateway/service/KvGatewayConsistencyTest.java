package com.danieljhkim.kvdb.kvgateway.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.danieljhkim.kvdb.kvcommon.cache.ShardMapCache;
import com.danieljhkim.kvdb.kvcommon.grpc.GrpcIdentity;
import com.danieljhkim.kvdb.kvcommon.grpc.GrpcPeerIdentity;
import com.danieljhkim.kvdb.kvgateway.cache.NodeFailureTracker;
import com.danieljhkim.kvdb.kvgateway.client.NodeConnectionPool;
import com.danieljhkim.kvdb.kvgateway.retry.RequestExecutor;
import com.danieljhkim.kvdb.kvgateway.retry.RetryPolicy;
import com.danieljhkim.kvdb.proto.coordinator.ClusterState;
import com.danieljhkim.kvdb.proto.coordinator.NodeRecord;
import com.danieljhkim.kvdb.proto.coordinator.NodeStatus;
import com.danieljhkim.kvdb.proto.coordinator.PartitioningConfig;
import com.danieljhkim.kvdb.proto.coordinator.ShardRecord;
import com.danieljhkim.kvdb.proto.gateway.Consistency;
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
import com.kvdb.proto.kvstore.SetResponse;
import com.kvdb.proto.kvstore.ValueResponse;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import java.util.List;
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
