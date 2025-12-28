package com.danieljhkim.kvdb.kvcommon.exception;

import io.grpc.Status;

/**
 * Exception thrown when a key is not found.
 * Maps to gRPC NOT_FOUND.
 */
public class KeyNotFoundException extends KvException {

	private final String key;

	public KeyNotFoundException(String key) {
		super("Key not found: " + key);
		this.key = key;
	}

	public KeyNotFoundException(String key, String shardId) {
		super("Key not found: " + key, shardId);
		this.key = key;
	}

	public String getKey() {
		return key;
	}

	@Override
	public Status.Code getGrpcStatusCode() {
		return Status.Code.NOT_FOUND;
	}
}
