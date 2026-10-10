package com.danieljhkim.kvdb.kvcommon.exception;

import io.grpc.Status;

/**
 * Exception thrown when a verified peer is not authorized for the requested operation. Maps to gRPC PERMISSION_DENIED.
 */
public class PermissionDeniedException extends KvException {

    public PermissionDeniedException(String message) {
        super(message);
    }

    public PermissionDeniedException(String message, String shardId) {
        super(message, shardId);
    }

    @Override
    public Status.Code getGrpcStatusCode() {
        return Status.Code.PERMISSION_DENIED;
    }
}
