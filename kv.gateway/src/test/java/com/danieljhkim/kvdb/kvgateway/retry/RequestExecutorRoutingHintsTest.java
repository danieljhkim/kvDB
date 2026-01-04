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
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
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

        ExecutionResult<String> result = executor.executeWithRetry("shard-1", true, op, () -> List.of(nodeA));

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

        ExecutionResult<String> result = executor.executeWithRetry("shard-1", true, op, () -> List.of(nodeA));

        // SHARD_MOVED should result in failure since we have maxAttempts=1
        assertFalse(result.isSuccess());
    }
}
