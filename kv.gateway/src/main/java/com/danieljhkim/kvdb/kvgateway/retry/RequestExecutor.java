package com.danieljhkim.kvdb.kvgateway.retry;

import com.danieljhkim.kvdb.kvcommon.observability.Metrics;
import com.danieljhkim.kvdb.kvgateway.cache.NodeFailureTracker;
import com.danieljhkim.kvdb.kvgateway.client.NodeConnectionPool;
import com.danieljhkim.kvdb.proto.coordinator.NodeRecord;
import com.kvdb.proto.kvstore.KVServiceGrpc;
import io.grpc.Context;
import io.grpc.Deadline;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
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
        private final boolean ambiguous;

        private ExecutionResult(
                T response, Status.Code errorCode, String errorMessage, String lastNodeAddress, boolean ambiguous) {
            this.response = response;
            this.errorCode = errorCode;
            this.errorMessage = errorMessage;
            this.lastNodeAddress = lastNodeAddress;
            this.ambiguous = ambiguous;
        }

        public static <T> ExecutionResult<T> success(T response, String nodeAddress) {
            return new ExecutionResult<>(response, null, null, nodeAddress, false);
        }

        public static <T> ExecutionResult<T> failure(Status.Code code, String message, String nodeAddress) {
            return new ExecutionResult<>(null, code, message, nodeAddress, false);
        }

        public static <T> ExecutionResult<T> ambiguous(Status.Code code, String message, String nodeAddress) {
            return new ExecutionResult<>(null, code, message, nodeAddress, true);
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

        public boolean isAmbiguous() {
            return ambiguous;
        }
    }

    /**
     * Executes a request with retry logic.
     */
    public <T> ExecutionResult<T> executeWithRetry(
            String shardId,
            boolean isWrite,
            boolean replaySafe,
            Function<KVServiceGrpc.KVServiceBlockingStub, T> operation,
            Supplier<List<NodeRecord>> nodeSupplier) {

        Context context = Context.current();
        Deadline callerDeadline = context.getDeadline();
        StatusRuntimeException lastException = null;
        String lastNodeAddress = null;

        for (int attempt = 1; attempt <= retryPolicy.getMaxAttempts(); attempt++) {
            ExecutionResult<T> stopped = stoppedResult(context, callerDeadline, lastNodeAddress);
            if (stopped != null) {
                return stopped;
            }

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
                T response = operation.apply(stub.withDeadline(effectiveDeadline(callerDeadline)));
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

                ExecutionResult<T> stoppedAfterCall = stoppedResult(context, callerDeadline, lastNodeAddress);
                if (stoppedAfterCall != null) {
                    return stoppedAfterCall;
                }

                failureTracker.recordFailure(lastNodeAddress);
                // Try leader hint if available
                if (code == Status.Code.FAILED_PRECONDITION) {
                    ExecutionResult<T> hintResult = tryLeaderHint(operation, e, replaySafe, context, callerDeadline);
                    if (hintResult != null) {
                        return hintResult;
                    }
                }

                if (isWrite && !replaySafe && isAmbiguousWriteFailure(code)) {
                    return ExecutionResult.ambiguous(
                            code,
                            "Write outcome is unknown after " + code + "; request was not replayed",
                            lastNodeAddress);
                }

                if (!retryPolicy.isRetryable(code) && code != Status.Code.FAILED_PRECONDITION) {
                    break;
                }

                if (attempt < retryPolicy.getMaxAttempts()) {
                    Metrics.increment("kvdb_retries_total", "gateway", "node_rpc", "retry");
                    if (!sleepWithBackoff(attempt, context, callerDeadline)) {
                        ExecutionResult<T> stoppedDuringBackoff =
                                stoppedResult(context, callerDeadline, lastNodeAddress);
                        return stoppedDuringBackoff != null
                                ? stoppedDuringBackoff
                                : ExecutionResult.failure(
                                        Status.Code.CANCELLED,
                                        "Request interrupted during retry backoff",
                                        lastNodeAddress);
                    }
                }
            }
        }

        Metrics.increment("kvdb_retries_total", "gateway", "node_rpc", "exhausted");
        return ExecutionResult.failure(
                lastException != null ? lastException.getStatus().getCode() : Status.Code.UNAVAILABLE,
                lastException != null ? lastException.getStatus().getDescription() : "All retry attempts exhausted",
                lastNodeAddress);
    }

    private <T> ExecutionResult<T> tryLeaderHint(
            Function<KVServiceGrpc.KVServiceBlockingStub, T> operation,
            StatusRuntimeException e,
            boolean replaySafe,
            Context context,
            Deadline callerDeadline) {
        GrpcRoutingHints.RoutingHints hints = GrpcRoutingHints.from(e);

        if (hints.leaderHint().isEmpty()) {
            return null;
        }

        String hintedAddress = hints.leaderHint().get();
        if (failureTracker.isRecentlyFailed(hintedAddress)) {
            return null;
        }

        ExecutionResult<T> stopped = stoppedResult(context, callerDeadline, hintedAddress);
        if (stopped != null) {
            return stopped;
        }

        try {
            KVServiceGrpc.KVServiceBlockingStub stub = nodePool.getStub(hintedAddress);
            T response = operation.apply(stub.withDeadline(effectiveDeadline(callerDeadline)));
            failureTracker.clearFailure(hintedAddress);
            return ExecutionResult.success(response, hintedAddress);
        } catch (StatusRuntimeException hintedEx) {
            ExecutionResult<T> stoppedAfterCall = stoppedResult(context, callerDeadline, hintedAddress);
            if (stoppedAfterCall != null) {
                return stoppedAfterCall;
            }
            Metrics.increment("kvdb_retries_total", "gateway", "leader_hint", "failed");
            logger.warn(
                    "Leader-hint retry failed (node={}): {}",
                    hintedAddress,
                    hintedEx.getStatus().getDescription());
            failureTracker.recordFailure(hintedAddress);
            Status.Code code = hintedEx.getStatus().getCode();
            if (!replaySafe && isAmbiguousWriteFailure(code)) {
                return ExecutionResult.ambiguous(
                        code,
                        "Write outcome is unknown after leader-hint retry " + code + "; request was not replayed",
                        hintedAddress);
            }
            if (!retryPolicy.isRetryable(code) && code != Status.Code.FAILED_PRECONDITION) {
                return ExecutionResult.failure(code, hintedEx.getStatus().getDescription(), hintedAddress);
            }
            return null;
        }
    }

    private boolean sleepWithBackoff(int attempt, Context context, Deadline callerDeadline) {
        long backoffNanos = TimeUnit.MILLISECONDS.toNanos(retryPolicy.calculateBackoff(attempt));
        if (callerDeadline != null) {
            backoffNanos = Math.min(backoffNanos, Math.max(0, callerDeadline.timeRemaining(TimeUnit.NANOSECONDS)));
        }
        if (context.isCancelled() || (callerDeadline != null && callerDeadline.isExpired())) {
            return false;
        }
        if (backoffNanos <= 0) {
            return true;
        }

        CountDownLatch cancelled = new CountDownLatch(1);
        Context.CancellationListener listener = ignored -> cancelled.countDown();
        context.addListener(listener, Runnable::run);
        try {
            cancelled.await(backoffNanos, TimeUnit.NANOSECONDS);
            return !context.isCancelled() && (callerDeadline == null || !callerDeadline.isExpired());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return false;
        } finally {
            context.removeListener(listener);
        }
    }

    private Deadline effectiveDeadline(Deadline callerDeadline) {
        Deadline policyDeadline = Deadline.after(defaultTimeoutMs, TimeUnit.MILLISECONDS);
        return callerDeadline == null ? policyDeadline : policyDeadline.minimum(callerDeadline);
    }

    private static <T> ExecutionResult<T> stoppedResult(
            Context context, Deadline callerDeadline, String lastNodeAddress) {
        if (callerDeadline != null && callerDeadline.isExpired()) {
            return ExecutionResult.failure(
                    Status.Code.DEADLINE_EXCEEDED, "Inbound request deadline expired", lastNodeAddress);
        }
        if (context.isCancelled()) {
            return ExecutionResult.failure(Status.Code.CANCELLED, "Inbound request was cancelled", lastNodeAddress);
        }
        return null;
    }

    private boolean isAmbiguousWriteFailure(Status.Code code) {
        return retryPolicy.isRetryable(code)
                || code == Status.Code.CANCELLED
                || code == Status.Code.UNKNOWN
                || code == Status.Code.DEADLINE_EXCEEDED
                || code == Status.Code.UNAVAILABLE
                || code == Status.Code.INTERNAL;
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
