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
import com.danieljhkim.kvdb.proto.gateway.BatchGetOutcome;
import com.danieljhkim.kvdb.proto.gateway.BatchGetRequest;
import com.danieljhkim.kvdb.proto.gateway.BatchGetResponse;
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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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

    @Test
    void batchGetPreservesOrderDuplicatesBinaryKeysAndMixedPerKeyOutcomes() {
        ByteString found = ByteString.copyFrom(new byte[] {(byte) 0xff, 0});
        ByteString missing = ByteString.copyFrom(new byte[] {0});
        ByteString unavailable = ByteString.copyFrom(new byte[] {1});
        KeyAwareExecutor executor = new KeyAwareExecutor(Duration.ZERO, null, null);
        KvGatewayServiceImpl service = new KvGatewayServiceImpl(cache(), executor, batchLimits(2, 4096));
        CapturingObserver<BatchGetResponse> observer = new CapturingObserver<>();

        service.batchGet(
                BatchGetRequest.newBuilder()
                        .addKeys(found)
                        .addKeys(missing)
                        .addKeys(found)
                        .addKeys(unavailable)
                        .setHeadOnly(true)
                        .build(),
                observer);

        assertEquals(Status.Code.OK, observer.value.getStatus().getCode());
        assertEquals(
                List.of(found, missing, found, unavailable),
                observer.value.getResultsList().stream()
                        .map(result -> result.getKey())
                        .toList());
        assertEquals(
                List.of(Status.Code.OK, Status.Code.NOT_FOUND, Status.Code.OK, Status.Code.UNAVAILABLE),
                observer.value.getResultsList().stream()
                        .map(result -> result.getStatus().getCode())
                        .toList());
        assertEquals(255, observer.value.getResults(0).getKv().getVersion());
        assertEquals(1_255, observer.value.getResults(0).getAppliedVersion());
        assertTrue(observer.value.getResults(0).getKv().getValue().isEmpty());
        assertTrue(executor.requests.stream().allMatch(com.kvdb.proto.kvstore.KeyRequest::getHeadOnly));
        assertTrue(observer.value.getResultsList().stream()
                .allMatch(result -> result.getOutcome() == BatchGetOutcome.COMPLETED));
    }

    @Test
    void batchGetLimitsRejectBeforeDispatchAndResponseBudgetMarksEveryRemainingKey() {
        AppConfig.LimitsConfig countConfig = new AppConfig.LimitsConfig();
        countConfig.setMaxBatchEntries(2);
        countConfig.setMaxBatchAggregateKeyBytes(3);
        KeyAwareExecutor rejectedExecutor = new KeyAwareExecutor(Duration.ZERO, null, null);
        KvGatewayServiceImpl rejectingService =
                new KvGatewayServiceImpl(cache(), rejectedExecutor, new KvRequestLimits(countConfig));
        CapturingObserver<BatchGetResponse> tooMany = new CapturingObserver<>();
        rejectingService.batchGet(batchRequest(1, 2, 3), tooMany);

        assertEquals(Status.Code.PAYLOAD_TOO_LARGE, tooMany.value.getStatus().getCode());
        assertEquals(0, rejectedExecutor.calls.get());

        AppConfig.LimitsConfig aggregateConfig = new AppConfig.LimitsConfig();
        aggregateConfig.setMaxBatchEntries(4);
        aggregateConfig.setMaxBatchAggregateKeyBytes(2);
        KeyAwareExecutor aggregateExecutor = new KeyAwareExecutor(Duration.ZERO, null, null);
        KvGatewayServiceImpl aggregateService =
                new KvGatewayServiceImpl(cache(), aggregateExecutor, new KvRequestLimits(aggregateConfig));
        CapturingObserver<BatchGetResponse> tooLarge = new CapturingObserver<>();
        aggregateService.batchGet(batchRequest(2, 3, 4), tooLarge);

        assertEquals(Status.Code.PAYLOAD_TOO_LARGE, tooLarge.value.getStatus().getCode());
        assertEquals(0, aggregateExecutor.calls.get());

        KeyAwareExecutor budgetExecutor = new KeyAwareExecutor(Duration.ZERO, null, null);
        KvGatewayServiceImpl budgetService = new KvGatewayServiceImpl(cache(), budgetExecutor, batchLimits(1, 125));
        CapturingObserver<BatchGetResponse> bounded = new CapturingObserver<>();
        budgetService.batchGet(batchRequest(5, 6, 7), bounded);

        assertTrue(bounded.value.getSerializedSize() <= 125);
        assertEquals(3, bounded.value.getResultsCount());
        assertTrue(bounded.value.getResultsList().stream()
                .allMatch(result -> result.getOutcome() == BatchGetOutcome.RESPONSE_BUDGET_EXHAUSTED));
        assertEquals(1, budgetExecutor.calls.get());
    }

    @Test
    void batchGetBoundsActiveReadsAndExpiredDeadlineDispatchesNothing() throws Exception {
        CountDownLatch entered = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        KeyAwareExecutor executor = new KeyAwareExecutor(Duration.ZERO, entered, release);
        KvGatewayServiceImpl service = new KvGatewayServiceImpl(cache(), executor, batchLimits(2, 4096));
        CapturingObserver<BatchGetResponse> observer = new CapturingObserver<>();

        try (var caller = Executors.newVirtualThreadPerTaskExecutor()) {
            var rpc = caller.submit(() -> service.batchGet(batchRequest(2, 3, 4, 5, 6), observer));
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            assertEquals(2, executor.maxActive.get());
            assertEquals(2, executor.calls.get());
            release.countDown();
            rpc.get(2, TimeUnit.SECONDS);
        }

        assertEquals(5, observer.value.getResultsCount());
        assertEquals(2, executor.maxActive.get());

        KeyAwareExecutor expiredExecutor = new KeyAwareExecutor(Duration.ZERO, null, null);
        KvGatewayServiceImpl expiredService = new KvGatewayServiceImpl(cache(), expiredExecutor, batchLimits(2, 4096));
        CapturingObserver<BatchGetResponse> expired = new CapturingObserver<>();
        try (var scheduler = Executors.newSingleThreadScheduledExecutor()) {
            Context.current()
                    .withDeadlineAfter(0, TimeUnit.NANOSECONDS, scheduler)
                    .run(() -> expiredService.batchGet(batchRequest(2, 3, 4), expired));
        }

        assertEquals(0, expiredExecutor.calls.get());
        assertEquals(3, expired.value.getResultsCount());
        assertTrue(expired.value.getResultsList().stream()
                .allMatch(result -> result.getOutcome() == BatchGetOutcome.DEADLINE_EXCEEDED));
    }

    @Test
    void batchGetCancellationStopsQueuedReadsAndMarksEveryKey() throws Exception {
        CountDownLatch entered = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        KeyAwareExecutor executor = new KeyAwareExecutor(Duration.ZERO, entered, release);
        KvGatewayServiceImpl service = new KvGatewayServiceImpl(cache(), executor, batchLimits(2, 4096));
        CapturingObserver<BatchGetResponse> observer = new CapturingObserver<>();
        Context.CancellableContext cancellable = Context.current().withCancellation();

        try (var caller = Executors.newVirtualThreadPerTaskExecutor()) {
            var rpc = caller.submit(() -> cancellable.run(() -> service.batchGet(batchRequest(2, 3, 4, 5), observer)));
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            cancellable.cancel(new RuntimeException("client cancelled"));
            rpc.get(2, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            cancellable.close();
        }

        assertEquals(2, executor.calls.get());
        assertEquals(4, observer.value.getResultsCount());
        assertTrue(observer.value.getResultsList().stream()
                .allMatch(result -> result.getOutcome() == BatchGetOutcome.CANCELLED));
        assertTrue(observer.value.getResultsList().stream()
                .allMatch(result -> result.getStatus().getCode() == Status.Code.CANCELLED));
    }

    @Test
    void fixedMultiShardBaselineShowsOneClientRpcWithEqualBackendReadsAndBoundedFanout() {
        List<Integer> keys = List.of(1, 2, 3, 4, 5, 6, 7, 8);
        KeyAwareExecutor unaryExecutor = new KeyAwareExecutor(Duration.ofMillis(15), null, null);
        KvGatewayServiceImpl unaryService =
                new KvGatewayServiceImpl(multiShardCache(), unaryExecutor, batchLimits(4, 4096));
        long unaryStart = System.nanoTime();
        for (int key : keys) {
            unaryService.get(
                    GetRequest.newBuilder()
                            .setKey(ByteString.copyFrom(new byte[] {(byte) key}))
                            .build(),
                    new CapturingObserver<>());
        }
        long unaryMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - unaryStart);

        KeyAwareExecutor batchExecutor = new KeyAwareExecutor(Duration.ofMillis(15), null, null);
        KvGatewayServiceImpl batchService =
                new KvGatewayServiceImpl(multiShardCache(), batchExecutor, batchLimits(4, 4096));
        CapturingObserver<BatchGetResponse> batch = new CapturingObserver<>();
        long batchStart = System.nanoTime();
        batchService.batchGet(
                batchRequest(keys.stream().mapToInt(Integer::intValue).toArray()), batch);
        long batchMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - batchStart);

        System.out.printf(
                "BatchGet baseline: unary_client_rpcs=8 batch_client_rpcs=1 unary_ms=%d batch_ms=%d unary_backend_calls=%d batch_backend_calls=%d batch_max_active=%d%n",
                unaryMillis,
                batchMillis,
                unaryExecutor.calls.get(),
                batchExecutor.calls.get(),
                batchExecutor.maxActive.get());
        assertEquals(8, unaryExecutor.calls.get());
        assertEquals(8, batchExecutor.calls.get());
        assertEquals(4, batchExecutor.maxActive.get());
        assertTrue(batchMillis < unaryMillis);
        assertEquals(8, batch.value.getResultsCount());
    }

    private static BatchGetRequest batchRequest(int... keys) {
        BatchGetRequest.Builder request = BatchGetRequest.newBuilder();
        for (int key : keys) {
            request.addKeys(ByteString.copyFrom(new byte[] {(byte) key}));
        }
        return request.build();
    }

    private static KvRequestLimits batchLimits(int concurrency, int responseBytes) {
        AppConfig.LimitsConfig config = new AppConfig.LimitsConfig();
        config.setMaxBatchEntries(16);
        config.setMaxBatchAggregateKeyBytes(64);
        config.setMaxBatchGetConcurrency(concurrency);
        config.setMaxBatchGetResponseBytes(responseBytes);
        return new KvRequestLimits(config);
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

    private static ShardMapCache multiShardCache() {
        ClusterState.Builder state = ClusterState.newBuilder()
                .setMapVersion(1)
                .setPartitioning(PartitioningConfig.newBuilder().setNumShards(4).setReplicationFactor(1));
        for (int index = 0; index < 4; index++) {
            String nodeId = "node-" + index;
            String shardId = "shard-" + index;
            state.putNodes(
                            nodeId,
                            NodeRecord.newBuilder()
                                    .setNodeId(nodeId)
                                    .setAddress(nodeId + ":9000")
                                    .setStatus(NodeStatus.ALIVE)
                                    .build())
                    .putShards(
                            shardId,
                            ShardRecord.newBuilder()
                                    .setShardId(shardId)
                                    .setEpoch(1)
                                    .setLeader(nodeId)
                                    .addReplicas(nodeId)
                                    .build());
        }
        ShardMapCache cache = new ShardMapCache();
        cache.refreshFromFullState(state.build());
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

    private static final class KeyAwareExecutor extends RequestExecutor {
        private final Duration delay;
        private final CountDownLatch entered;
        private final CountDownLatch release;
        private final AtomicInteger calls = new AtomicInteger();
        private final AtomicInteger active = new AtomicInteger();
        private final AtomicInteger maxActive = new AtomicInteger();
        private final List<com.kvdb.proto.kvstore.KeyRequest> requests =
                Collections.synchronizedList(new ArrayList<>());

        private KeyAwareExecutor(Duration delay, CountDownLatch entered, CountDownLatch release) {
            super(new NodeConnectionPool(), new NodeFailureTracker(), RetryPolicy.defaults(), 100);
            this.delay = delay;
            this.entered = entered;
            this.release = release;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> ExecutionResult<T> executeWithRetry(
                String shardId,
                boolean isWrite,
                boolean replaySafe,
                Function<KVServiceGrpc.KVServiceBlockingStub, T> operation,
                Supplier<List<NodeRecord>> nodeSupplier) {
            calls.incrementAndGet();
            assertFalse(nodeSupplier.get().isEmpty());
            int current = active.incrementAndGet();
            maxActive.accumulateAndGet(current, Math::max);
            if (entered != null) {
                entered.countDown();
            }
            try {
                if (release != null && !release.await(2, TimeUnit.SECONDS)) {
                    return ExecutionResult.failure(io.grpc.Status.Code.DEADLINE_EXCEEDED, "test gate timeout", null);
                }
                if (!delay.isZero()) {
                    Thread.sleep(delay);
                }
                KVServiceGrpc.KVServiceBlockingStub stub = KVServiceGrpc.newBlockingStub(new KeyAwareChannel(requests));
                return ExecutionResult.success(operation.apply(stub), "node:9000");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return ExecutionResult.failure(io.grpc.Status.Code.CANCELLED, "interrupted", null);
            } catch (io.grpc.StatusRuntimeException e) {
                return ExecutionResult.failure(
                        e.getStatus().getCode(), e.getStatus().getDescription(), null);
            } finally {
                active.decrementAndGet();
            }
        }
    }

    private static final class KeyAwareChannel extends Channel {
        private final List<com.kvdb.proto.kvstore.KeyRequest> requests;

        private KeyAwareChannel(List<com.kvdb.proto.kvstore.KeyRequest> requests) {
            this.requests = requests;
        }

        @Override
        public String authority() {
            return "key-aware";
        }

        @Override
        public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(
                MethodDescriptor<RequestT, ResponseT> method, CallOptions callOptions) {
            return new ClientCall<>() {
                private Listener<ResponseT> listener;
                private com.kvdb.proto.kvstore.KeyRequest request;

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
                    requests.add(request);
                    int marker = Byte.toUnsignedInt(request.getKey().byteAt(0));
                    if (marker == 1) {
                        listener.onClose(
                                io.grpc.Status.UNAVAILABLE.withDescription("fixture unavailable"), new Metadata());
                        return;
                    }
                    ValueResponse.Builder response = ValueResponse.newBuilder().setFound(marker != 0);
                    if (marker != 0) {
                        ByteString value = marker >= 5 ? ByteString.copyFrom(new byte[96]) : request.getKey();
                        response.setValue(request.getHeadOnly() ? ByteString.EMPTY : value)
                                .setVersion(marker)
                                .setAppliedVersion(1_000L + marker);
                    }
                    listener.onMessage((ResponseT) response.build());
                    listener.onClose(io.grpc.Status.OK, new Metadata());
                }

                @Override
                public void sendMessage(RequestT message) {
                    request = (com.kvdb.proto.kvstore.KeyRequest) message;
                }
            };
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
