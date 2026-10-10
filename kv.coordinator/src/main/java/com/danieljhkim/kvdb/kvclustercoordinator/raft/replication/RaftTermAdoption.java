package com.danieljhkim.kvdb.kvclustercoordinator.raft.replication;

import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftPersistentStateStore;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.state.RaftNodeState;
import java.io.IOException;

/**
 * Adopts a higher term learned from an RPC response.
 *
 * <p>The term and the cleared vote are made durable before the in-memory term changes or the node steps down, so a
 * restart can never roll back a term the live node already exposed. If persistence fails the in-memory state is left
 * untouched and the {@link IOException} is propagated to the caller.
 */
final class RaftTermAdoption {

    private RaftTermAdoption() {}

    /**
     * Persists {@code responseTerm} with a cleared vote and then steps down to follower in that term.
     *
     * @return true if the node adopted the term, false if its term was already at least {@code responseTerm}
     * @throws IOException if the term could not be made durable; no in-memory change was made
     */
    static boolean adoptHigherTerm(RaftNodeState state, RaftPersistentStateStore persistentStore, long responseTerm)
            throws IOException {
        synchronized (state) {
            if (responseTerm <= state.getCurrentTerm()) {
                return false;
            }
            persistentStore.save(responseTerm, null);
            state.becomeFollower(responseTerm, null);
            return true;
        }
    }
}
