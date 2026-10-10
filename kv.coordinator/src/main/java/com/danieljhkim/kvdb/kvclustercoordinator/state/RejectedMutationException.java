package com.danieljhkim.kvdb.kvclustercoordinator.state;

/**
 * Thrown when a cluster-state mutation is invalid against the current membership or shard map. The state is left
 * unchanged.
 *
 * <p>
 * Rejection is a pure function of the replicated state and the command, so every coordinator replica rejects the same
 * committed log entry identically. The Raft applier therefore consumes a rejected entry as a no-op instead of halting
 * the node. Extends {@link IllegalArgumentException} so gRPC callers receive {@code INVALID_ARGUMENT}.
 */
public class RejectedMutationException extends IllegalArgumentException {

    public RejectedMutationException(String message) {
        super(message);
    }
}
