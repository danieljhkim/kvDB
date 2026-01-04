package com.danieljhkim.kvdb.kvgateway.retry;

import com.danieljhkim.kvdb.kvgateway.cache.NodeFailureTracker;
import com.danieljhkim.kvdb.kvgateway.client.NodeConnectionPool;
import com.danieljhkim.kvdb.proto.coordinator.NodeRecord;
import com.kvdb.proto.kvstore.KVServiceGrpc;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes requests to storage nodes with retry logic and backoff.
 */
public class RequestExecutor {

    private static final Logger logger = LoggerFactory.getLogger(RequestExecutor.class);

    private final NodeConnectionPool nodePool;
    private final NodeFailureTracker failureTracker;
    private final RetryPolicy retryPolicy;
    private final int defaultTimeoutMs;

    public RequestExecutor(
            NodeConnectionPool nodePool,
            NodeFailureTracker failureTracker,
            RetryPolicy retryPolicy,
            int defaultTimeoutMs) {
        this.nodePool = nodePool;
        this.failureTracker = failureTracker;
        this.retryPolicy = retryPolicy;
        this.defaultTimeoutMs = defaultTimeoutMs;
    }

    /**
     * Result of a request execution.
     */
    public static class ExecutionResult<T> {
        private final T response;
        private final Status.Code errorCode;
        private final String errorMessage;
        private final String lastNodeAddress;

        private ExecutionResult(T response, Status.Code errorCode, String errorMessage, String lastNodeAddress) {
            this.response = response;
            this.errorCode = errorCode;
            this.errorMessage = errorMessage;
            this.lastNodeAddress = lastNodeAddress;
        }

        public static <T> ExecutionResult<T> success(T response, String nodeAddress) {
            return new ExecutionResult<>(response, null, null, nodeAddress);
        }

        public static <T> ExecutionResult<T> failure(Status.Code code, String message, String nodeAddress) {
            return new ExecutionResult<>(null, code, message, nodeAddress);
        }

        public boolean isSuccess() {
            return errorCode == null;
        }

        public T getResponse() {
            return response;
        }

        public Status.Code getErrorCode() {
            return errorCode;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        public String getLastNodeAddress() {
            return lastNodeAddress;
        }
    }

    /**
     * Executes a request with retry logic.
     */
    @SuppressWarnings("all")
    public <T> ExecutionResult<T> executeWithRetry(
            String shardId,
            boolean isWrite,
            Function<KVServiceGrpc.KVServiceBlockingStub, T> operation,
            Supplier<List<NodeRecord>> nodeSupplier) {

        StatusRuntimeException lastException = null;
        String lastNodeAddress = null;

        for (int attempt = 1; attempt <= retryPolicy.getMaxAttempts(); attempt++) {
            List<NodeRecord> candidates = nodeSupplier.get();
            if (candidates == null || candidates.isEmpty()) {
                logger.warn("No candidate nodes available for shard: {}", shardId);
                return ExecutionResult.failure(
                        Status.Code.UNAVAILABLE, "No available nodes for shard: " + shardId, null);
            }

            NodeRecord targetNode = selectNode(candidates, isWrite);
            lastNodeAddress = targetNode.getAddress();

            try {
                KVServiceGrpc.KVServiceBlockingStub stub = nodePool.getStub(lastNodeAddress);
                T response = operation.apply(stub.withDeadlineAfter(defaultTimeoutMs, TimeUnit.MILLISECONDS));
                failureTracker.clearFailure(lastNodeAddress);
                return ExecutionResult.success(response, lastNodeAddress);

            } catch (StatusRuntimeException e) {
                lastException = e;
                Status.Code code = e.getStatus().getCode();
                logger.warn(
                        "Request failed (attempt {}/{}, node={}, code={}): {}",
                        attempt,
                        retryPolicy.getMaxAttempts(),
                        lastNodeAddress,
                        code,
                        e.getStatus().getDescription());

                failureTracker.recordFailure(lastNodeAddress);
                // Try leader hint if available
                if (code == Status.Code.FAILED_PRECONDITION) {
                    ExecutionResult<T> hintResult = tryLeaderHint(operation, e);
                    if (hintResult != null) {
                        return hintResult;
                    }
                }

                if (!retryPolicy.isRetryable(code) && code != Status.Code.FAILED_PRECONDITION) {
                    break;
                }

                if (attempt < retryPolicy.getMaxAttempts()) {
                    sleepWithBackoff(attempt);
                }
            }
        }

        return ExecutionResult.failure(
                lastException != null ? lastException.getStatus().getCode() : Status.Code.UNAVAILABLE,
                lastException != null ? lastException.getStatus().getDescription() : "All retry attempts exhausted",
                lastNodeAddress);
    }

    private <T> ExecutionResult<T> tryLeaderHint(
            Function<KVServiceGrpc.KVServiceBlockingStub, T> operation, StatusRuntimeException e) {
        GrpcRoutingHints.RoutingHints hints = GrpcRoutingHints.from(e);

        if (hints.leaderHint().isEmpty()) {
            return null;
        }

        String hintedAddress = hints.leaderHint().get();
        if (failureTracker.isRecentlyFailed(hintedAddress)) {
            return null;
        }

        try {
            KVServiceGrpc.KVServiceBlockingStub stub = nodePool.getStub(hintedAddress);
            T response = operation.apply(stub.withDeadlineAfter(defaultTimeoutMs, TimeUnit.MILLISECONDS));
            failureTracker.clearFailure(hintedAddress);
            return ExecutionResult.success(response, hintedAddress);
        } catch (StatusRuntimeException hintedEx) {
            logger.warn(
                    "Leader-hint retry failed (node={}): {}",
                    hintedAddress,
                    hintedEx.getStatus().getDescription());
            failureTracker.recordFailure(hintedAddress);
            return null;
        }
    }

    private void sleepWithBackoff(int attempt) {
        long backoffMs = retryPolicy.calculateBackoff(attempt);
        try {
            Thread.sleep(backoffMs);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    private NodeRecord selectNode(List<NodeRecord> candidates, boolean isWrite) {
        // Writes must go to leader (first candidate)
        if (isWrite) {
            return candidates.getFirst();
        }
        // Reads prefer non-failed nodes
        for (NodeRecord node : candidates) {
            if (!failureTracker.isRecentlyFailed(node.getAddress())) {
                return node;
            }
        }
        // All failed recently - clear and use first
        candidates.forEach(n -> failureTracker.clearFailure(n.getAddress()));
        return candidates.getFirst();
    }
}
