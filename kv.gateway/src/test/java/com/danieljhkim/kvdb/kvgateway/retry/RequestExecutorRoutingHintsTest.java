package com.danieljhkim.kvdb.kvgateway.retry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.danieljhkim.kvdb.kvcommon.grpc.GlobalExceptionInterceptor;
import com.danieljhkim.kvdb.kvgateway.cache.NodeFailureTracker;
import com.danieljhkim.kvdb.kvgateway.client.NodeConnectionPool;
import com.danieljhkim.kvdb.kvgateway.retry.RequestExecutor.ExecutionResult;
import com.danieljhkim.kvdb.proto.coordinator.NodeRecord;
import com.danieljhkim.kvdb.proto.coordinator.NodeStatus;
import com.kvdb.proto.kvstore.KVServiceGrpc;
import io.grpc.*;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class RequestExecutorRoutingHintsTest {

    private static final class NoopChannel extends Channel {
        @Override
        public String authority() {
            return "noop";
        }

        @Override
        public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(
                MethodDescriptor<RequestT, ResponseT> methodDescriptor, CallOptions callOptions) {
            throw new UnsupportedOperationException("NoopChannel does not support RPCs");
        }
    }

    private static final class FakeNodeConnectionPool extends NodeConnectionPool {
        private final KVServiceGrpc.KVServiceBlockingStub stub = KVServiceGrpc.newBlockingStub(new NoopChannel());

        @Override
        public KVServiceGrpc.KVServiceBlockingStub getStub(String nodeAddress) {
            return stub;
        }
    }

    @Test
    void notLeader_withLeaderHint_retriesHintedLeaderOnce() {
        FakeNodeConnectionPool nodePool = new FakeNodeConnectionPool();
        NodeFailureTracker nodeFailureTracker = new NodeFailureTracker(5000);
        RetryPolicy retryPolicy = RetryPolicy.builder()
                .maxAttempts(1)
                .retryableStatusCodes(Set.of())
                .build();

        RequestExecutor executor = new RequestExecutor(nodePool, nodeFailureTracker, retryPolicy, 50);

        NodeRecord nodeA = NodeRecord.newBuilder()
                .setNodeId("node-a")
                .setAddress("nodeA:123")
                .setStatus(NodeStatus.ALIVE)
                .build();

        AtomicInteger calls = new AtomicInteger(0);
        Function<KVServiceGrpc.KVServiceBlockingStub, String> op = stub -> {
            if (calls.getAndIncrement() == 0) {
                Metadata trailers = new Metadata();
                trailers.put(GlobalExceptionInterceptor.SHARD_ID_KEY, "shard-1");
                trailers.put(GlobalExceptionInterceptor.LEADER_HINT_KEY, "leader:456");
                throw new StatusRuntimeException(Status.FAILED_PRECONDITION.withDescription("NOT_LEADER"), trailers);
            }
            return "ok";
        };

        ExecutionResult<String> result = executor.executeWithRetry("shard-1", true, true, op, () -> List.of(nodeA));

        assertTrue(result.isSuccess());
        assertEquals("leader:456", result.getLastNodeAddress());
    }

    @Test
    void shardMoved_withNewNodeHint_returnsFailure() {
        FakeNodeConnectionPool nodePool = new FakeNodeConnectionPool();
        NodeFailureTracker nodeFailureTracker = new NodeFailureTracker(5000);
        RetryPolicy retryPolicy = RetryPolicy.builder()
                .maxAttempts(1)
                .retryableStatusCodes(Set.of())
                .build();

        RequestExecutor executor = new RequestExecutor(nodePool, nodeFailureTracker, retryPolicy, 50);

        NodeRecord nodeA = NodeRecord.newBuilder()
                .setNodeId("node-a")
                .setAddress("nodeA:123")
                .setStatus(NodeStatus.ALIVE)
                .build();

        Function<KVServiceGrpc.KVServiceBlockingStub, String> op = stub -> {
            Metadata trailers = new Metadata();
            trailers.put(GlobalExceptionInterceptor.SHARD_ID_KEY, "shard-1");
            trailers.put(GlobalExceptionInterceptor.NEW_NODE_HINT_KEY, "nodeB:999");
            throw new StatusRuntimeException(Status.FAILED_PRECONDITION.withDescription("SHARD_MOVED"), trailers);
        };

        ExecutionResult<String> result = executor.executeWithRetry("shard-1", true, true, op, () -> List.of(nodeA));

        // SHARD_MOVED should result in failure since we have maxAttempts=1
        assertFalse(result.isSuccess());
    }

    @Test
    void nonIdempotentWriteTimeoutIsAmbiguousAndIsNotReplayed() {
        AtomicInteger calls = new AtomicInteger();
        RequestExecutor executor = executor(RetryPolicy.builder()
                .maxAttempts(3)
                .initialBackoffMs(0)
                .jitterPercent(0)
                .build());

        ExecutionResult<String> result = executor.executeWithRetry(
                "shard-1",
                true,
                false,
                stub -> {
                    calls.incrementAndGet();
                    throw Status.DEADLINE_EXCEEDED
                            .withDescription("response lost")
                            .asRuntimeException();
                },
                () -> List.of(node("node-a", "nodeA:123")));

        assertFalse(result.isSuccess());
        assertTrue(result.isAmbiguous());
        assertEquals(Status.Code.DEADLINE_EXCEEDED, result.getErrorCode());
        assertEquals(1, calls.get());
    }

    @Test
    void idempotentWriteTimeoutIsRetriedWithTheSameOperation() {
        AtomicInteger calls = new AtomicInteger();
        RequestExecutor executor = executor(RetryPolicy.builder()
                .maxAttempts(2)
                .initialBackoffMs(0)
                .jitterPercent(0)
                .build());
        String stableRequestId = "stable-request-id";

        ExecutionResult<String> result = executor.executeWithRetry(
                "shard-1",
                true,
                true,
                stub -> {
                    if (calls.getAndIncrement() == 0) {
                        throw Status.DEADLINE_EXCEEDED
                                .withDescription("response lost")
                                .asRuntimeException();
                    }
                    return stableRequestId;
                },
                () -> List.of(node("node-a", "nodeA:123")));

        assertTrue(result.isSuccess());
        assertEquals(stableRequestId, result.getResponse());
        assertEquals(2, calls.get());
    }

    @Test
    void inboundDeadlineStopsRetryBackoff() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        RequestExecutor executor = executor(RetryPolicy.builder()
                .maxAttempts(3)
                .initialBackoffMs(1_000)
                .jitterPercent(0)
                .build());
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        Context.CancellableContext context =
                Context.current().withDeadlineAfter(40, java.util.concurrent.TimeUnit.MILLISECONDS, scheduler);
        long started = System.nanoTime();
        try {
            ExecutionResult<String> result = context.call(() -> executor.executeWithRetry(
                    "shard-1",
                    false,
                    true,
                    stub -> {
                        calls.incrementAndGet();
                        throw Status.UNAVAILABLE.asRuntimeException();
                    },
                    () -> List.of(node("node-a", "nodeA:123"))));

            assertFalse(result.isSuccess());
            assertEquals(Status.Code.DEADLINE_EXCEEDED, result.getErrorCode());
            assertEquals(1, calls.get());
            assertTrue(Duration.ofNanos(System.nanoTime() - started).toMillis() < 500);
        } finally {
            context.cancel(null);
            scheduler.shutdownNow();
        }
    }

    @Test
    void cancelledInboundCallDoesNotStartAnAttempt() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        RequestExecutor executor = executor(RetryPolicy.defaults());
        Context.CancellableContext context = Context.current().withCancellation();
        context.cancel(null);

        ExecutionResult<String> result = context.call(() -> executor.executeWithRetry(
                "shard-1",
                false,
                true,
                stub -> {
                    calls.incrementAndGet();
                    return "unexpected";
                },
                () -> List.of(node("node-a", "nodeA:123"))));

        assertFalse(result.isSuccess());
        assertEquals(Status.Code.CANCELLED, result.getErrorCode());
        assertEquals(0, calls.get());
    }

    @Test
    void inboundDeadlineCapsNodeRpcDeadline() throws Exception {
        RequestExecutor executor = executor(RetryPolicy.defaults());
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        Context.CancellableContext context =
                Context.current().withDeadlineAfter(2, java.util.concurrent.TimeUnit.SECONDS, scheduler);
        AtomicReference<Deadline> observedDeadline = new AtomicReference<>();
        try {
            ExecutionResult<String> result = context.call(() -> executor.executeWithRetry(
                    "shard-1",
                    false,
                    true,
                    stub -> {
                        observedDeadline.set(stub.getCallOptions().getDeadline());
                        return "ok";
                    },
                    () -> List.of(node("node-a", "nodeA:123"))));

            assertTrue(result.isSuccess());
            assertEquals(context.getDeadline(), observedDeadline.get());
        } finally {
            context.cancel(null);
            scheduler.shutdownNow();
        }
    }

    private static RequestExecutor executor(RetryPolicy policy) {
        return new RequestExecutor(new FakeNodeConnectionPool(), new NodeFailureTracker(5000), policy, 5_000);
    }

    private static NodeRecord node(String nodeId, String address) {
        return NodeRecord.newBuilder()
                .setNodeId(nodeId)
                .setAddress(address)
                .setStatus(NodeStatus.ALIVE)
                .build();
    }
}
