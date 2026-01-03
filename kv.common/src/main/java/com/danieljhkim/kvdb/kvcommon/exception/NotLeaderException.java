package com.danieljhkim.kvdb.kvcommon.exception;

import io.grpc.Status;

/**
 * Exception thrown when a write operation is attempted on a non-leader coordinator. Maps to gRPC FAILED_PRECONDITION.
 */
public class NotLeaderException extends KvException {

    private final String leaderHint;

    public NotLeaderException() {
        super("Not the leader");
        this.leaderHint = null;
    }

    public NotLeaderException(String leaderHint) {
        super("Not the leader. Leader hint: " + leaderHint);
        this.leaderHint = leaderHint;
    }

    /**
     * Gets the leader hint, if available.
     */
    public String getLeaderHint() {
        return leaderHint;
    }

    @Override
    public Status.Code getGrpcStatusCode() {
        return Status.Code.FAILED_PRECONDITION;
    }
}
