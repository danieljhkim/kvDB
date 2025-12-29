package com.danieljhkim.kvdb.kvcommon.grpc;

import java.io.UncheckedIOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.danieljhkim.kvdb.kvcommon.exception.KvException;
import com.danieljhkim.kvdb.kvcommon.exception.NotLeaderException;
import com.danieljhkim.kvdb.kvcommon.exception.ShardMovedException;
import com.google.protobuf.ServiceException;

import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

/**
 * Global gRPC interceptor that catches exceptions and maps them to appropriate
 * gRPC status codes. Handles KvException hierarchy and standard Java
 * exceptions.
 */
public class GlobalExceptionInterceptor implements ServerInterceptor {

	private static final Logger logger = LoggerFactory.getLogger(GlobalExceptionInterceptor.class);

	// Metadata keys for client routing hints
	public static final Metadata.Key<String> SHARD_ID_KEY = Metadata.Key.of("x-shard-id",
			Metadata.ASCII_STRING_MARSHALLER);
	public static final Metadata.Key<String> LEADER_HINT_KEY = Metadata.Key.of("x-leader-hint",
			Metadata.ASCII_STRING_MARSHALLER);
	public static final Metadata.Key<String> NEW_NODE_HINT_KEY = Metadata.Key.of("x-new-node-hint",
			Metadata.ASCII_STRING_MARSHALLER);

	@Override
	public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
			ServerCall<ReqT, RespT> call,
			Metadata headers,
			ServerCallHandler<ReqT, RespT> next) {

		ServerCall.Listener<ReqT> delegate = next.startCall(call, headers);

		return new SimpleForwardingServerCallListener<ReqT>(delegate) {

			@Override
			public void onHalfClose() {
				try {
					super.onHalfClose();
				} catch (Throwable t) {
					// Map to Status + log centrally
					Status status = mapExceptionToStatus(t);
					Metadata trailers = buildTrailers(t);

					logException(status, t);
					call.close(status, trailers);
				}
			}
		};
	}

	/**
	 * Logs the exception based on its severity.
	 */
	private void logException(Status status, Throwable t) {
		switch (status.getCode()) {
			case INTERNAL -> logger.error("Unhandled exception in gRPC call", t);
			case UNAVAILABLE, DEADLINE_EXCEEDED ->
				logger.warn("gRPC call failed: {} - {}", status.getCode(), status.getDescription());
			default -> logger.debug("gRPC exception: {} - {}", status.getCode(), status.getDescription());
		}
	}

	/**
	 * Maps exceptions to gRPC Status.
	 */
	private Status mapExceptionToStatus(Throwable t) {
		// Unwrap UncheckedIOException if needed
		if (t instanceof UncheckedIOException uioe && uioe.getCause() != null) {
			t = uioe.getCause();
		}

		// ----- KvDB domain exceptions -----
		if (t instanceof KvException kvEx) {
			return kvEx.toGrpcStatus();
		}

		// ----- Standard Java exceptions -----

		// IllegalArgumentException (e.g., request validation failures)
		if (t instanceof IllegalArgumentException e) {
			return Status.INVALID_ARGUMENT
					.withDescription(e.getMessage());
		}

		// IllegalStateException (e.g., uninitialized state)
		if (t instanceof IllegalStateException e) {
			return Status.FAILED_PRECONDITION
					.withDescription(e.getMessage());
		}

		// NullPointerException
		if (t instanceof NullPointerException e) {
			return Status.INTERNAL
					.withDescription("Null pointer: " + e.getMessage());
		}

		// ServiceException from protobuf
		if (t instanceof ServiceException e) {
			return Status.INTERNAL
					.withDescription(e.getMessage());
		}

		// I/O wrapped in UncheckedIOException
		if (t instanceof UncheckedIOException e) {
			return Status.INTERNAL
					.withDescription("I/O error: " + e.getMessage());
		}

		// Generic fallback → INTERNAL
		return Status.INTERNAL
				.withDescription("Internal server error");
	}

	/**
	 * Builds trailers with additional metadata for the client.
	 */
	private Metadata buildTrailers(Throwable t) {
		Metadata trailers = new Metadata();

		if (t instanceof KvException kvEx) {
			// Add shard ID if available
			if (kvEx.getShardId() != null) {
				trailers.put(SHARD_ID_KEY, kvEx.getShardId());
			}

			// Add leader hint for NotLeaderException
			if (t instanceof NotLeaderException nle && nle.getLeaderHint() != null) {
				trailers.put(LEADER_HINT_KEY, nle.getLeaderHint());
			}

			// Add new node hint for ShardMovedException
			if (t instanceof ShardMovedException sme && sme.getNewNodeHint() != null) {
				trailers.put(NEW_NODE_HINT_KEY, sme.getNewNodeHint());
			}
		}

		return trailers;
	}
}
