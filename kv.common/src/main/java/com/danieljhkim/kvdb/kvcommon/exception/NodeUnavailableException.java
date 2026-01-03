package com.danieljhkim.kvdb.kvcommon.exception;

import io.grpc.Status;

/**
 * Exception thrown when no nodes are available for a shard. Maps to gRPC UNAVAILABLE.
 */
public class NodeUnavailableException extends KvException {

    private final io.grpc.Status.Code originalGrpcCode;

    public NodeUnavailableException(String message, String shardId) {
        super(message, shardId);
        this.originalGrpcCode = null;
    }

    public NodeUnavailableException(String message, String shardId, io.grpc.Status.Code originalCode) {
        super(message, shardId);
        this.originalGrpcCode = originalCode;
    }

    /**
     * Gets the original gRPC error code if this exception wraps a node error.
     */
    public io.grpc.Status.Code getOriginalGrpcCode() {
        return originalGrpcCode;
    }

    @Override
    public Status.Code getGrpcStatusCode() {
        // Map original code if available
        if (originalGrpcCode != null) {
            return switch (originalGrpcCode) {
                case DEADLINE_EXCEEDED -> Status.Code.DEADLINE_EXCEEDED;
                case RESOURCE_EXHAUSTED -> Status.Code.RESOURCE_EXHAUSTED;
                default -> Status.Code.UNAVAILABLE;
            };
        }
        return Status.Code.UNAVAILABLE;
    }
}
