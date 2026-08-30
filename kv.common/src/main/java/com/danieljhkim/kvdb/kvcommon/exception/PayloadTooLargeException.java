package com.danieljhkim.kvdb.kvcommon.exception;

import io.grpc.Status;

/** A configured request bound was exceeded. */
public final class PayloadTooLargeException extends KvException {

    public PayloadTooLargeException(String message) {
        super(message);
    }

    @Override
    public Status.Code getGrpcStatusCode() {
        return Status.Code.RESOURCE_EXHAUSTED;
    }
}
