package com.danieljhkim.kvdb.kvcommon.exception;

import io.grpc.Status;

/**
 * Base exception for all KvDB domain exceptions.
 * Each subclass maps to a specific gRPC status code.
 */
public abstract class KvException extends RuntimeException {

	private final String shardId;

	protected KvException(String message) {
		super(message);
		this.shardId = null;
	}

	protected KvException(String message, String shardId) {
		super(message);
		this.shardId = shardId;
	}

	protected KvException(String message, Throwable cause) {
		super(message, cause);
		this.shardId = null;
	}

	protected KvException(String message, String shardId, Throwable cause) {
		super(message, cause);
		this.shardId = shardId;
	}

	/**
	 * Returns the gRPC status code for this exception.
	 */
	public abstract Status.Code getGrpcStatusCode();

	/**
	 * Returns the shard ID associated with this exception, if any.
	 */
	public String getShardId() {
		return shardId;
	}

	/**
	 * Builds the gRPC Status for this exception.
	 */
	public Status toGrpcStatus() {
		return Status.fromCode(getGrpcStatusCode())
				.withDescription(getMessage())
				.withCause(getCause());
	}
}
