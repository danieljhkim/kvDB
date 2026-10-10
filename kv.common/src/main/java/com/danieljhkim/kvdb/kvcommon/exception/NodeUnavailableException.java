package com.danieljhkim.kvdb.kvcommon.exception;

import io.grpc.Status;

/**
 * Exception thrown when no nodes are available for a shard, or wrapping a definitive node RPC failure. Defaults to
 * gRPC UNAVAILABLE; wrapped failures retain their original status.
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
        // Do not turn a permanent request error into a retryable availability failure.
        return originalGrpcCode != null ? originalGrpcCode : Status.Code.UNAVAILABLE;
    }
}
