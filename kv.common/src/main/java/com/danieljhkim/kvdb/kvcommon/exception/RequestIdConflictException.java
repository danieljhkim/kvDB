package com.danieljhkim.kvdb.kvcommon.exception;

import io.grpc.Status;

/** A request ID cannot identify two different mutations. Retrying the conflicting request cannot succeed. */
public final class RequestIdConflictException extends KvException {

    public RequestIdConflictException(String shardId) {
        super("request_id was already used for a different mutation", shardId);
    }

    @Override
    public Status.Code getGrpcStatusCode() {
        return Status.Code.INVALID_ARGUMENT;
    }
}
