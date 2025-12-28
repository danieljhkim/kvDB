package com.danieljhkim.kvdb.kvgateway.retry;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.danieljhkim.kvdb.kvgateway.cache.ShardRoutingFailureTracker;
import com.danieljhkim.kvdb.kvgateway.cache.NodeFailureTracker;
import com.danieljhkim.kvdb.kvgateway.cache.ShardMapCache;
import com.danieljhkim.kvdb.kvgateway.client.NodeConnectionPool;
import com.danieljhkim.kvdb.proto.coordinator.NodeRecord;
import com.kvdb.proto.kvstore.KVServiceGrpc;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * Executes requests to storage nodes with retry logic, backoff, and cache
 * invalidation.
 * Handles gRPC errors and attempts retries based on the configured RetryPolicy.
 */
public class RequestExecutor {

	private static final Logger LOGGER = Logger.getLogger(RequestExecutor.class.getName());

	private final ShardMapCache shardMapCache;
	private final NodeConnectionPool nodePool;
	private final NodeFailureTracker failureTracker;
	private final ShardRoutingFailureTracker shardRoutingFailureTracker;
	private final RetryPolicy retryPolicy;
	private final int defaultTimeoutMs;

	public RequestExecutor(
			ShardMapCache shardMapCache,
			NodeConnectionPool nodePool,
			NodeFailureTracker failureTracker,
			RetryPolicy retryPolicy) {
		this(shardMapCache, nodePool, failureTracker, new ShardRoutingFailureTracker(), retryPolicy, 5000);
	}

	public RequestExecutor(
			ShardMapCache shardMapCache,
			NodeConnectionPool nodePool,
			NodeFailureTracker failureTracker,
			ShardRoutingFailureTracker shardRoutingFailureTracker,
			RetryPolicy retryPolicy,
			int defaultTimeoutMs) {
		this.shardMapCache = shardMapCache;
		this.nodePool = nodePool;
		this.failureTracker = failureTracker;
		this.shardRoutingFailureTracker = shardRoutingFailureTracker != null
				? shardRoutingFailureTracker
				: new ShardRoutingFailureTracker();
		this.retryPolicy = retryPolicy;
		this.defaultTimeoutMs = defaultTimeoutMs;
	}

	/**
	 * Result of a request execution containing either a response or an error.
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
	 *
	 * @param shardId
	 *            the shard ID for this request
	 * @param isWrite
	 *            true for write operations (leader only), false for reads
	 * @param operation
	 *            the operation to execute on the stub
	 * @param nodeSupplier
	 *            supplies candidate nodes for the request
	 * @param <T>
	 *            the response type
	 * @return the execution result
	 */
	public <T> ExecutionResult<T> executeWithRetry(
			String shardId,
			boolean isWrite,
			Function<KVServiceGrpc.KVServiceBlockingStub, T> operation,
			Supplier<List<NodeRecord>> nodeSupplier) {

		int maxAttempts = retryPolicy.getMaxAttempts();
		StatusRuntimeException lastException = null;
		String lastNodeAddress = null;

		for (int attempt = 1; attempt <= maxAttempts; attempt++) {
			// If we recently saw a NOT_LEADER for this shard, schedule a refresh to
			// converge faster (still gated by ShardMapCache unless forced).
			if (isWrite && shardRoutingFailureTracker.isRecentlyNotLeader(shardId)) {
				shardMapCache.scheduleRefreshIfStale();
			}

			// Get candidate nodes
			List<NodeRecord> candidates = nodeSupplier.get();
			if (candidates == null || candidates.isEmpty()) {
				LOGGER.warning("No candidate nodes available for shard: " + shardId);
				return ExecutionResult.failure(
						Status.Code.UNAVAILABLE,
						"No available nodes for shard: " + shardId,
						null);
			}

			// Select a node (skip recently failed ones for reads)
			NodeRecord targetNode = selectNode(candidates, isWrite);
			if (targetNode == null) {
				LOGGER.warning("All candidate nodes recently failed for shard: " + shardId);
				// Clear failure records and try again
				for (NodeRecord node : candidates) {
					failureTracker.clearFailure(node.getAddress());
				}
				targetNode = candidates.get(0);
			}

			lastNodeAddress = targetNode.getAddress();

			try {
				// Execute the operation
				KVServiceGrpc.KVServiceBlockingStub stub = nodePool.getStub(lastNodeAddress);
				T response = operation.apply(
						stub.withDeadlineAfter(defaultTimeoutMs, TimeUnit.MILLISECONDS));

				// Success - clear any failure record
				failureTracker.clearFailure(lastNodeAddress);
				return ExecutionResult.success(response, lastNodeAddress);

			} catch (StatusRuntimeException e) {
				lastException = e;
				Status.Code code = e.getStatus().getCode();

				LOGGER.log(Level.WARNING,
						"Request failed (attempt " + attempt + "/" + maxAttempts
								+ ", node=" + lastNodeAddress + ", code=" + code + "): "
								+ e.getStatus().getDescription());

				// Record the failure
				failureTracker.recordFailure(lastNodeAddress);

				// Handle specific error codes
				if (code == Status.Code.FAILED_PRECONDITION) {
					GrpcRoutingHints.RoutingHints hints = GrpcRoutingHints.from(e);

					// NOT_LEADER: leader hint present -> retry hinted leader once
					if (hints.leaderHint().isPresent()) {
						String hintedLeaderAddress = hints.leaderHint().get();
						String hintedShardId = hints.shardId().orElse(shardId);
						shardRoutingFailureTracker.recordNotLeader(hintedShardId);

						if (!failureTracker.isRecentlyFailed(hintedLeaderAddress)) {
							try {
								KVServiceGrpc.KVServiceBlockingStub hintedStub = nodePool.getStub(hintedLeaderAddress);
								T hintedResponse = operation.apply(
										hintedStub.withDeadlineAfter(defaultTimeoutMs, TimeUnit.MILLISECONDS));
								failureTracker.clearFailure(hintedLeaderAddress);
								shardRoutingFailureTracker.clear(hintedShardId);
								return ExecutionResult.success(hintedResponse, hintedLeaderAddress);
							} catch (StatusRuntimeException hintedEx) {
								// fall through to regular retry loop
								LOGGER.log(Level.WARNING,
										"Leader-hint retry failed (node={0}, code={1}): {2}",
										new Object[] {
												hintedLeaderAddress,
												hintedEx.getStatus().getCode(),
												hintedEx.getStatus().getDescription()
										});
								failureTracker.recordFailure(hintedLeaderAddress);
								lastException = hintedEx;
							}
						}

						// Hint retry didn't work or was skipped -> schedule refresh (gated)
						shardMapCache.scheduleRefreshIfStale();

					} else if (hints.newNodeHint().isPresent()) {
						// SHARD_MOVED signal -> force refresh
						LOGGER.info("SHARD_MOVED hint received, forcing shard map refresh");
						shardMapCache.forceRefreshAsync();
					} else {
						// Generic stale routing - schedule refresh (gated)
						LOGGER.info("FAILED_PRECONDITION received, scheduling shard map refresh");
						shardMapCache.scheduleRefreshIfStale();
					}
				}

				// Check if we should retry
				boolean retryable = retryPolicy.isRetryable(code) || code == Status.Code.FAILED_PRECONDITION;
				if (!retryable) {
					LOGGER.fine("Error code " + code + " is not retryable");
					break;
				}

				// Check if we have more attempts
				if (attempt < maxAttempts) {
					// Apply backoff before retry
					long backoffMs = retryPolicy.calculateBackoff(attempt);
					LOGGER.fine("Backing off for " + backoffMs + "ms before retry");
					try {
						Thread.sleep(backoffMs);
					} catch (InterruptedException ie) {
						Thread.currentThread().interrupt();
						return ExecutionResult.failure(
								Status.Code.CANCELLED,
								"Interrupted during backoff",
								lastNodeAddress);
					}
				}
			}
		}

		// All retries exhausted
		Status.Code finalCode = lastException != null
				? lastException.getStatus().getCode()
				: Status.Code.UNAVAILABLE;
		String finalMessage = lastException != null
				? lastException.getStatus().getDescription()
				: "All retry attempts exhausted";

		return ExecutionResult.failure(finalCode, finalMessage, lastNodeAddress);
	}

	/**
	 * Selects a node from candidates, preferring nodes that haven't failed
	 * recently.
	 */
	private NodeRecord selectNode(List<NodeRecord> candidates, boolean isWrite) {
		// For writes, prefer the first candidate (usually the leader)
		if (isWrite && !candidates.isEmpty()) {
			NodeRecord leader = candidates.get(0);
			// For writes, we must use the leader even if it recently failed
			// (other nodes can't accept writes)
			return leader;
		}

		// For reads, skip recently failed nodes
		for (NodeRecord node : candidates) {
			if (!failureTracker.isRecentlyFailed(node.getAddress())) {
				return node;
			}
		}

		// All nodes have failed recently - return null to trigger retry logic
		return null;
	}
}
