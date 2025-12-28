package com.danieljhkim.kvdb.kvgateway.retry;

import java.util.Optional;

import com.danieljhkim.kvdb.kvcommon.grpc.GlobalExceptionInterceptor;

import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;

/**
 * Extracts routing hints from gRPC trailers for gateway retry/invalidation
 * logic.
 *
 * <p>
 * The {@link GlobalExceptionInterceptor} is responsible for attaching these
 * keys
 * to trailers when services throw domain exceptions.
 */
public final class GrpcRoutingHints {

	private GrpcRoutingHints() {
	}

	public static RoutingHints from(StatusRuntimeException e) {
		if (e == null) {
			return RoutingHints.empty();
		}
		Metadata trailers = e.getTrailers();
		if (trailers == null) {
			return RoutingHints.empty();
		}

		return new RoutingHints(
				Optional.ofNullable(trailers.get(GlobalExceptionInterceptor.SHARD_ID_KEY)),
				Optional.ofNullable(trailers.get(GlobalExceptionInterceptor.LEADER_HINT_KEY)),
				Optional.ofNullable(trailers.get(GlobalExceptionInterceptor.NEW_NODE_HINT_KEY)));
	}

	public record RoutingHints(
			Optional<String> shardId,
			Optional<String> leaderHint,
			Optional<String> newNodeHint) {

		public static RoutingHints empty() {
			return new RoutingHints(Optional.empty(), Optional.empty(), Optional.empty());
		}
	}
}
