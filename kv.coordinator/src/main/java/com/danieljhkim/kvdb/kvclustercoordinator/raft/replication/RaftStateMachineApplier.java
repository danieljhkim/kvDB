package com.danieljhkim.kvdb.kvclustercoordinator.raft.replication;

import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftCommand;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftLog;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftLogEntry;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.state.RaftNodeState;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.statemachine.RaftStateMachine;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

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

    @Getter
    private volatile boolean running = false;

    private volatile Throwable failure;

    private final Object applyLock = new Object();

    public RaftStateMachineApplier(String nodeId, RaftNodeState state, RaftStateMachine stateMachine) {
        this(nodeId, state, stateMachine, Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "raft-applier-" + nodeId);
            t.setDaemon(true);
            return t;
        }));
    }

    public RaftStateMachineApplier(
            String nodeId, RaftNodeState state, RaftStateMachine stateMachine, Executor applyExecutor) {
        this.nodeId = nodeId;
        this.state = state;
        this.stateMachine = stateMachine;
        this.applyExecutor = applyExecutor;
    }

    /**
     * Starts the applier. This should be called once during initialization.
     */
    public void start() {
        if (failure != null) {
            throw new IllegalStateException("Cannot restart a failed state machine applier", failure);
        }
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
        Throwable currentFailure = failure;
        if (currentFailure != null) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("State machine applier is unavailable", currentFailure));
        }
        if (!running) {
            return CompletableFuture.failedFuture(new IllegalStateException("State machine applier is not running"));
        }

        return CompletableFuture.runAsync(this::doApply, applyExecutor);
    }

    /**
     * Applies all committed but not yet applied entries.
     */
    private void doApply() {
        synchronized (applyLock) {
            if (failure != null) {
                throw new CompletionException("State machine applier is unavailable", failure);
            }

            long commitIndex = state.getCommitIndex();
            long lastApplied = state.getLastApplied();

            if (commitIndex <= lastApplied) {
                // Nothing to apply
                return;
            }

            RaftLog raftLog = state.getLog();
            try {
                for (long index = lastApplied + 1; index <= commitIndex; index++) {
                    RaftLogEntry entry = getRequiredEntry(raftLog, index, commitIndex);
                    applyEntry(entry);
                }
            } catch (Throwable error) {
                failure = error;
                running = false;
                log.error(
                        "[{}] FATAL: State machine applier failed at lastApplied={} with commitIndex={}; node is unavailable",
                        nodeId,
                        state.getLastApplied(),
                        commitIndex,
                        error);
                if (error instanceof CompletionException completionException) {
                    throw completionException;
                }
                throw new CompletionException("Failed to apply committed entries", error);
            }
        }
    }

    /**
     * Gets the required entry at an authoritative Raft log index.
     */
    private RaftLogEntry getRequiredEntry(RaftLog raftLog, long index, long commitIndex) throws IOException {
        RaftLogEntry entry = raftLog.getEntry(index)
                .orElseThrow(() -> new IllegalStateException(String.format(
                        "Missing committed log entry at index %d (lastApplied=%d, commitIndex=%d)",
                        index, state.getLastApplied(), commitIndex)));
        if (entry.index() != index) {
            throw new IllegalStateException(
                    "Raft log returned entry index " + entry.index() + " for requested index " + index);
        }
        return entry;
    }

    /**
     * Applies a single log entry to the state machine.
     */
    private void applyEntry(RaftLogEntry entry) {
        try {
            log.debug(
                    "[{}] Applying entry at index {} (term={}) to state machine", nodeId, entry.index(), entry.term());

            if (!(entry.command() instanceof RaftCommand.NoOp)) {
                // Completion is the state machine's acknowledgement that the operation succeeded.
                CompletableFuture<Void> application = stateMachine.apply(entry.command());
                if (application == null) {
                    throw new IllegalStateException("State machine returned a null application future");
                }
                application.join();
            }

            // Advance only after the corresponding operation completed successfully.
            state.advanceLastApplied(entry.index());

            log.trace(
                    "[{}] Successfully applied entry at index {}, lastApplied={}",
                    nodeId,
                    entry.index(),
                    state.getLastApplied());

        } catch (Exception e) {
            // This is a critical error - we cannot skip entries
            log.error(
                    "[{}] FATAL: Failed to apply entry at index {}: {}. State machine may be inconsistent!",
                    nodeId,
                    entry.index(),
                    e.getMessage(),
                    e);
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
