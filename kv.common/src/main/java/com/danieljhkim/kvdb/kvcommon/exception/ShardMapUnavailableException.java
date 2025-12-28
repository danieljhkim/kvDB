package com.danieljhkim.kvdb.kvcommon.exception;

import io.grpc.Status;

/**
 * Exception thrown when the shard map is not available.
 * Maps to gRPC UNAVAILABLE.
 */
public class ShardMapUnavailableException extends KvException {

	public ShardMapUnavailableException(String message) {
		super(message);
	}

	public ShardMapUnavailableException(String message, Throwable cause) {
		super(message, cause);
	}

	@Override
	public Status.Code getGrpcStatusCode() {
		return Status.Code.UNAVAILABLE;
	}
}
