package com.danieljhkim.kvdb.kvcommon.exception;

import io.grpc.Status;

/**
 * Availability failure, or a definitive node RPC status forwarded by the gateway. Defaults to gRPC UNAVAILABLE
 * without an outcome guarantee; only the explicit pre-mutation factory guarantees rejection. Wrapped failures
 * retain their original status.
 */
public class NodeUnavailableException extends KvException {

    private final io.grpc.Status.Code originalGrpcCode;
    private final boolean rejectedBeforeMutation;

    public NodeUnavailableException(String message, String shardId) {
        super(message, shardId);
        this.originalGrpcCode = null;
        this.rejectedBeforeMutation = false;
    }

    public NodeUnavailableException(String message, String shardId, io.grpc.Status.Code originalCode) {
        super(message, shardId);
        this.originalGrpcCode = originalCode;
        this.rejectedBeforeMutation = false;
    }

    private NodeUnavailableException(String message, String shardId, boolean rejectedBeforeMutation) {
        super(message, shardId);
        this.originalGrpcCode = null;
        this.rejectedBeforeMutation = rejectedBeforeMutation;
    }

    /** Only use before this request can prepare or commit a mutation. */
    public static NodeUnavailableException rejectedBeforeMutation(String message, String shardId) {
        return new NodeUnavailableException(message, shardId, true);
    }

    public boolean isRejectedBeforeMutation() {
        return rejectedBeforeMutation;
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
