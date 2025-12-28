package com.danieljhkim.kvdb.kvcommon.exception;

import io.grpc.Status;

/**
 * Exception thrown when a request is invalid (e.g., empty key, malformed data).
 * Maps to gRPC INVALID_ARGUMENT.
 */
public class InvalidRequestException extends KvException {

	public InvalidRequestException(String message) {
		super(message);
	}

	public InvalidRequestException(String message, String shardId) {
		super(message, shardId);
	}

	@Override
	public Status.Code getGrpcStatusCode() {
		return Status.Code.INVALID_ARGUMENT;
	}
}
