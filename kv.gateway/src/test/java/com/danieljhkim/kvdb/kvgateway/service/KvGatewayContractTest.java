package com.danieljhkim.kvdb.kvgateway.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.danieljhkim.kvdb.kvcommon.cache.ShardMapCache;
import com.danieljhkim.kvdb.kvcommon.config.AppConfig;
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
import com.danieljhkim.kvdb.proto.gateway.GetRequest;
import com.danieljhkim.kvdb.proto.gateway.GetResponse;
import com.danieljhkim.kvdb.proto.gateway.PutRequest;
import com.danieljhkim.kvdb.proto.gateway.PutResponse;
import com.danieljhkim.kvdb.proto.gateway.ReadOptions;
import com.danieljhkim.kvdb.proto.gateway.RequestContext;
import com.danieljhkim.kvdb.proto.gateway.Status;
import com.danieljhkim.kvdb.proto.gateway.WriteDurability;
import com.danieljhkim.kvdb.proto.gateway.WriteOptions;
import com.google.protobuf.ByteString;
import com.kvdb.proto.kvstore.KVServiceGrpc;
import com.kvdb.proto.kvstore.KeyValueRequest;
import com.kvdb.proto.kvstore.MutationOutcome;
import com.kvdb.proto.kvstore.SetResponse;
import com.kvdb.proto.kvstore.ValueResponse;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

class KvGatewayContractTest {

    @Test
    void binaryPutAndAdvertisedOptionsReachTheNodeContractExactly() {
        ByteString key = ByteString.copyFrom(new byte[] {0, (byte) 0xff, (byte) 0x80});
        ByteString value = ByteString.copyFrom(new byte[] {(byte) 0xfe, 0, (byte) 0xc3, 0x28});
        CapturingExecutor executor = new CapturingExecutor();
        executor.invokeOperation = true;
        KvGatewayServiceImpl service = new KvGatewayServiceImpl(cache(), executor);
        CapturingObserver<PutResponse> observer = new CapturingObserver<>();

        runAsClient(() -> service.put(
                PutRequest.newBuilder()
                        .setCtx(RequestContext.newBuilder().setRequestId("binary-write"))
                        .setKey(key)
                        .setValue(value)
                        .setOptions(WriteOptions.newBuilder()
                                .setDurability(WriteDurability.WAL_SYNC)
                                .setRequireIdempotency(true)
                                .setTtlMs(5_000)
                                .setIfVersionEquals(0)
                                .setIfNotExists(true))
                        .build(),
                observer));

        assertEquals(Status.Code.OK, observer.value.getStatus().getCode());
        assertEquals(42, observer.value.getVersion());
        assertEquals(key, executor.capturedSet.getKey());
        assertEquals(value, executor.capturedSet.getValue());
        assertEquals("binary-write", executor.capturedSet.getRequestId());
        assertEquals(com.kvdb.proto.kvstore.WriteDurability.LOCAL_SYNC, executor.capturedSet.getDurability());
        assertEquals(5_000, executor.capturedSet.getTtlMs());
        assertTrue(executor.capturedSet.hasIfVersionEquals());
        assertEquals(0, executor.capturedSet.getIfVersionEquals());
        assertTrue(executor.capturedSet.getIfNotExists());
    }

    @Test
    void binaryReadAndHeadMetadataAreReturnedWithoutUtf8Conversion() {
        ByteString binary = ByteString.copyFrom(new byte[] {0, (byte) 0xff, (byte) 0x80});
        CapturingExecutor executor = new CapturingExecutor();
        executor.nextResult = RequestExecutor.ExecutionResult.success(
                ValueResponse.newBuilder()
                        .setFound(true)
                        .setValue(binary)
                        .setVersion(9)
                        .setAppliedVersion(12)
                        .setCreateTimeMs(10)
                        .setUpdateTimeMs(20)
                        .setExpireTimeMs(30)
                        .build(),
                "node-1:9000");
        KvGatewayServiceImpl service = new KvGatewayServiceImpl(cache(), executor);
        CapturingObserver<GetResponse> observer = new CapturingObserver<>();

        service.get(
                GetRequest.newBuilder()
                        .setKey(ByteString.copyFrom(new byte[] {0, (byte) 0xfe}))
                        .build(),
                observer);

        assertEquals(Status.Code.OK, observer.value.getStatus().getCode());
        assertEquals(binary, observer.value.getKv().getValue());
        assertEquals(9, observer.value.getKv().getVersion());
        assertEquals(10, observer.value.getKv().getCreateTimeMs());
        assertEquals(30, observer.value.getKv().getExpireTimeMs());
    }

    @Test
    void configuredFieldLimitsReturnStablePayloadTooLargeStatus() {
        AppConfig.LimitsConfig config = new AppConfig.LimitsConfig();
        config.setMaxKeyBytes(2);
        config.setMaxValueBytes(3);
        config.setMaxMessageBytes(64);
        CapturingExecutor executor = new CapturingExecutor();
        KvGatewayServiceImpl service = new KvGatewayServiceImpl(cache(), executor, new KvRequestLimits(config));
        CapturingObserver<PutResponse> observer = new CapturingObserver<>();

        service.put(
                PutRequest.newBuilder()
                        .setCtx(RequestContext.newBuilder().setRequestId("request"))
                        .setKey(ByteString.copyFrom(new byte[] {1, 2, 3}))
                        .build(),
                observer);

        assertEquals(Status.Code.PAYLOAD_TOO_LARGE, observer.value.getStatus().getCode());
        assertEquals(0, executor.calls);
    }

    @Test
    void unsupportedDurabilityAndMalformedContextAreRejectedBeforeNodeCall() {
        CapturingExecutor executor = new CapturingExecutor();
        KvGatewayServiceImpl service = new KvGatewayServiceImpl(cache(), executor);
        CapturingObserver<PutResponse> unsupported = new CapturingObserver<>();
        runAsClient(() -> service.put(
                request("request-1")
                        .setOptions(WriteOptions.newBuilder().setDurability(WriteDurability.WAL_ASYNC))
                        .build(),
                unsupported));
        assertEquals(Status.Code.INVALID_ARGUMENT, unsupported.value.getStatus().getCode());

        CapturingObserver<PutResponse> malformed = new CapturingObserver<>();
        service.put(
                request("request-2")
                        .setCtx(RequestContext.newBuilder()
                                .setRequestId("request-2")
                                .setTraceparent("not-a-traceparent"))
                        .build(),
                malformed);
        assertEquals(Status.Code.INVALID_ARGUMENT, malformed.value.getStatus().getCode());
        assertEquals(0, executor.calls);
    }

    @Test
    void unknownReadAndWriteEnumsAreRejectedInsteadOfDefaulted() {
        CapturingExecutor executor = new CapturingExecutor();
        KvGatewayServiceImpl service = new KvGatewayServiceImpl(cache(), executor);
        CapturingObserver<GetResponse> read = new CapturingObserver<>();
        service.get(
                GetRequest.newBuilder()
                        .setKey(ByteString.copyFromUtf8("key"))
                        .setOptions(ReadOptions.newBuilder().setConsistencyValue(999))
                        .build(),
                read);
        assertEquals(Status.Code.INVALID_ARGUMENT, read.value.getStatus().getCode());

        CapturingObserver<PutResponse> write = new CapturingObserver<>();
        runAsClient(() -> service.put(
                request("request-unknown")
                        .setOptions(WriteOptions.newBuilder().setDurabilityValue(999))
                        .build(),
                write));
        assertEquals(Status.Code.INVALID_ARGUMENT, write.value.getStatus().getCode());
        assertEquals(0, executor.calls);
    }

    @Test
    void conditionalNodeOutcomesMapToStablePublicCodes() {
        CapturingExecutor executor = new CapturingExecutor();
        executor.nextResult = RequestExecutor.ExecutionResult.success(
                SetResponse.newBuilder()
                        .setSuccess(false)
                        .setOutcome(MutationOutcome.VERSION_MISMATCH)
                        .setMessage("version mismatch")
                        .build(),
                "node-1:9000");
        KvGatewayServiceImpl service = new KvGatewayServiceImpl(cache(), executor);
        CapturingObserver<PutResponse> observer = new CapturingObserver<>();

        runAsClient(() -> service.put(request("request-cas").build(), observer));

        assertEquals(Status.Code.VERSION_MISMATCH, observer.value.getStatus().getCode());
        assertFalse(observer.value.getStatus().getMessage().isBlank());
        assertEquals(1, executor.calls);
    }

    @Test
    void authenticatedIdentityIsRequiredForWriteAuditCorrelation() {
        CapturingExecutor executor = new CapturingExecutor();
        KvGatewayServiceImpl service = new KvGatewayServiceImpl(cache(), executor);
        assertThrows(
                IllegalStateException.class,
                () -> service.put(request("request").build(), new CapturingObserver<>()));
    }

    private static PutRequest.Builder request(String requestId) {
        return PutRequest.newBuilder()
                .setCtx(RequestContext.newBuilder().setRequestId(requestId))
                .setKey(ByteString.copyFromUtf8("key"))
                .setValue(ByteString.copyFromUtf8("value"));
    }

    private static void runAsClient(Runnable operation) {
        Context.current()
                .withValue(
                        GrpcPeerIdentity.CURRENT,
                        new GrpcIdentity(GrpcIdentity.Role.EXTERNAL_CLIENT, "tenant-a", "alice"))
                .run(operation);
    }

    private static ShardMapCache cache() {
        ShardMapCache cache = new ShardMapCache();
        cache.refreshFromFullState(ClusterState.newBuilder()
                .setMapVersion(1)
                .setPartitioning(PartitioningConfig.newBuilder().setNumShards(1).setReplicationFactor(1))
                .putNodes(
                        "node-1",
                        NodeRecord.newBuilder()
                                .setNodeId("node-1")
                                .setAddress("node-1:9000")
                                .setStatus(NodeStatus.ALIVE)
                                .build())
                .putShards(
                        "shard-0",
                        ShardRecord.newBuilder()
                                .setShardId("shard-0")
                                .setEpoch(1)
                                .setLeader("node-1")
                                .addReplicas("node-1")
                                .build())
                .build());
        return cache;
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
        private int calls;
        private boolean invokeOperation;
        private KeyValueRequest capturedSet;

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
            assertTrue(!nodeSupplier.get().isEmpty());
            if (invokeOperation) {
                KVServiceGrpc.KVServiceBlockingStub stub = KVServiceGrpc.newBlockingStub(new CapturingChannel(this));
                return ExecutionResult.success(operation.apply(stub), "node-1:9000");
            }
            return (ExecutionResult<T>) nextResult;
        }
    }

    private static final class CapturingChannel extends Channel {
        private final CapturingExecutor executor;

        private CapturingChannel(CapturingExecutor executor) {
            this.executor = executor;
        }

        @Override
        public String authority() {
            return "capture";
        }

        @Override
        public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(
                MethodDescriptor<RequestT, ResponseT> method, CallOptions callOptions) {
            return new ClientCall<>() {
                private Listener<ResponseT> listener;

                @Override
                public void start(Listener<ResponseT> responseListener, Metadata headers) {
                    listener = responseListener;
                }

                @Override
                public void request(int numMessages) {}

                @Override
                public void cancel(String message, Throwable cause) {}

                @Override
                @SuppressWarnings("unchecked")
                public void halfClose() {
                    if (!method.getFullMethodName().endsWith("/Set")) {
                        throw new AssertionError("Unexpected RPC: " + method.getFullMethodName());
                    }
                    listener.onMessage((ResponseT) SetResponse.newBuilder()
                            .setSuccess(true)
                            .setOutcome(MutationOutcome.APPLIED)
                            .setVersion(42)
                            .build());
                    listener.onClose(io.grpc.Status.OK, new Metadata());
                }

                @Override
                public void sendMessage(RequestT message) {
                    executor.capturedSet = (KeyValueRequest) message;
                }
            };
        }
    }
}
