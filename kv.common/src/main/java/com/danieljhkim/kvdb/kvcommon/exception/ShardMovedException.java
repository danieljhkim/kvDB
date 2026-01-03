package com.danieljhkim.kvdb.kvcommon.exception;

import io.grpc.Status;

/**
 * Exception thrown when a shard has moved to a different node. Maps to gRPC FAILED_PRECONDITION.
 */
public class ShardMovedException extends KvException {

    private final String newNodeHint;

    public ShardMovedException(String shardId) {
        super("Shard has moved", shardId);
        this.newNodeHint = null;
    }

    public ShardMovedException(String shardId, String newNodeHint) {
        super("Shard has moved to: " + newNodeHint, shardId);
        this.newNodeHint = newNodeHint;
    }

    /**
     * Gets the new node hint, if available.
     */
    public String getNewNodeHint() {
        return newNodeHint;
    }

    @Override
    public Status.Code getGrpcStatusCode() {
        return Status.Code.FAILED_PRECONDITION;
    }
}
