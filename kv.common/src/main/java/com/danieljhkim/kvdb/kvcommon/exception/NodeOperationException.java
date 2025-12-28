package com.danieljhkim.kvdb.kvcommon.exception;

import io.grpc.Status;

/**
 * Exception thrown when a storage node operation fails.
 * Maps to gRPC INTERNAL.
 */
public class NodeOperationException extends KvException {

	public NodeOperationException(String message) {
		super(message);
	}

	public NodeOperationException(String message, String shardId) {
		super(message, shardId);
	}

	public NodeOperationException(String message, Throwable cause) {
		super(message, cause);
	}

	@Override
	public Status.Code getGrpcStatusCode() {
		return Status.Code.INTERNAL;
	}
}
