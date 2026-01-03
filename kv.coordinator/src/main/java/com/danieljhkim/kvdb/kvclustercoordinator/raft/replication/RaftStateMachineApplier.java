package com.danieljhkim.kvdb.kvclustercoordinator.raft.replication;

import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftLog;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftLogEntry;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.state.RaftNodeState;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.statemachine.RaftStateMachine;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Applies committed log entries to the state machine.
 *
 * <p>Raft paper §5.3: "Once a follower learns that a log entry is committed,
 * it applies the entry to its local state machine (in log order)."
 *
 * <p>This class monitors commitIndex and applies entries when commitIndex > lastApplied.
 */
@Slf4j
public class RaftStateMachineApplier {

    private final String nodeId;
    private final RaftNodeState state;
    private final RaftStateMachine stateMachine;
    private final Executor applyExecutor;

    /**
     * -- GETTER --
     *  Returns true if the applier is running.
     */
    @Getter
    private volatile boolean running = false;
    private final Object applyLock = new Object();

    public RaftStateMachineApplier(String nodeId, RaftNodeState state, RaftStateMachine stateMachine) {
        this(nodeId, state, stateMachine, Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "raft-applier-" + nodeId);
            t.setDaemon(true);
            return t;
        }));
    }

    public RaftStateMachineApplier(
            String nodeId,
            RaftNodeState state,
            RaftStateMachine stateMachine,
            Executor applyExecutor) {
        this.nodeId = nodeId;
        this.state = state;
        this.stateMachine = stateMachine;
        this.applyExecutor = applyExecutor;
    }

    /**
     * Starts the applier. This should be called once during initialization.
     */
    public void start() {
        running = true;
        log.info("[{}] Started state machine applier", nodeId);
    }

    /**
     * Stops the applier.
     */
    public void stop() {
        running = false;
        log.info("[{}] Stopped state machine applier", nodeId);
    }

    /**
     * Checks if there are committed entries to apply and applies them.
     * This method is idempotent and can be called multiple times.
     *
     * @return CompletableFuture that completes when all pending entries are applied
     */
    public CompletableFuture<Void> applyCommittedEntries() {
        if (!running) {
            return CompletableFuture.completedFuture(null);
        }

        return CompletableFuture.runAsync(this::doApply, applyExecutor);
    }

    /**
     * Applies all committed but not yet applied entries.
     */
    private void doApply() {
        synchronized (applyLock) {
            long commitIndex = state.getCommitIndex();
            long lastApplied = state.getLastApplied();

            if (commitIndex <= lastApplied) {
                // Nothing to apply
                return;
            }

            RaftLog raftLog = state.getLog();

            try {
                List<RaftLogEntry> entriesToApply = getEntriesToApply(raftLog, lastApplied + 1, commitIndex);

                for (RaftLogEntry entry : entriesToApply) {
                    applyEntry(entry);
                }

            } catch (Exception e) {
                log.error("[{}] Error applying committed entries: {}", nodeId, e.getMessage(), e);
            }
        }
    }

    /**
     * Gets entries that need to be applied (from lastApplied+1 to commitIndex).
     */
    private List<RaftLogEntry> getEntriesToApply(RaftLog raftLog, long fromIndex, long toIndex) throws IOException {
        List<RaftLogEntry> entries = new ArrayList<>();
        long lastApplied = state.getLastApplied();
        long commitIndex = state.getCommitIndex();

        for (long i = fromIndex; i <= toIndex; i++) {
            final long index = i;
            raftLog.getEntry(i).ifPresentOrElse(
                    entries::add,
                    () -> log.error("[{}] Missing log entry at index {} (lastApplied={}, commitIndex={})",
                            nodeId, index, lastApplied, commitIndex)
            );
        }

        return entries;
    }

    /**
     * Applies a single log entry to the state machine.
     */
    private void applyEntry(RaftLogEntry entry) {
        try {
            log.debug("[{}] Applying entry at index {} (term={}) to state machine",
                    nodeId, entry.index(), entry.term());

            // Apply the command to state machine
            stateMachine.apply(entry.command());

            // Update lastApplied
            state.advanceLastApplied(entry.index());

            log.trace("[{}] Successfully applied entry at index {}, lastApplied={}",
                    nodeId, entry.index(), state.getLastApplied());

        } catch (Exception e) {
            // This is a critical error - we cannot skip entries
            log.error("[{}] FATAL: Failed to apply entry at index {}: {}. State machine may be inconsistent!",
                    nodeId, entry.index(), e.getMessage(), e);
            // In a production system, you might want to halt the node here
            throw new RuntimeException("Failed to apply committed entry", e);
        }
    }

    /**
     * Returns the current lastApplied index.
     */
    public long getLastApplied() {
        return state.getLastApplied();
    }

    /**
     * Returns the current commitIndex.
     */
    public long getCommitIndex() {
        return state.getCommitIndex();
    }

    /**
     * Returns the number of entries waiting to be applied.
     */
    public long getPendingEntries() {
        return Math.max(0, state.getCommitIndex() - state.getLastApplied());
    }
}

