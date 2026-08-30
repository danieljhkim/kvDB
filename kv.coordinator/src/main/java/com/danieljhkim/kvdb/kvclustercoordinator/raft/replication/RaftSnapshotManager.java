package com.danieljhkim.kvdb.kvclustercoordinator.raft.replication;

import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftLog;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftSnapshotStore;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.state.RaftNodeState;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.statemachine.RaftStateMachine;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;

/** Coordinates snapshot creation/restoration with the log's durable compaction boundary. */
@Slf4j
public class RaftSnapshotManager {

    private final String nodeId;
    private final RaftNodeState state;
    private final RaftStateMachine stateMachine;
    private final RaftSnapshotStore snapshotStore;
    private final long threshold;

    public RaftSnapshotManager(
            String nodeId,
            RaftNodeState state,
            RaftStateMachine stateMachine,
            RaftSnapshotStore snapshotStore,
            long threshold) {
        this.nodeId = nodeId;
        this.state = state;
        this.stateMachine = stateMachine;
        this.snapshotStore = snapshotStore;
        this.threshold = threshold;
    }

    public synchronized void restoreOnStartup() throws IOException {
        var snapshot = snapshotStore.load();
        if (snapshot.isEmpty()) {
            if (state.getLog().compactedIndex() > 0) {
                throw new IOException("Raft log is compacted through index "
                        + state.getLog().compactedIndex() + " but no durable snapshot exists");
            }
            return;
        }
        RaftSnapshotStore.Snapshot durable = snapshot.get();
        if (state.getLog().compactedIndex() > durable.lastIncludedIndex()) {
            throw new IOException("Raft log compaction index is newer than the durable snapshot");
        }
        stateMachine.installSnapshot(durable.data());
        state.getLog().compactThrough(durable.lastIncludedIndex(), durable.lastIncludedTerm());
        state.advanceCommitIndex(durable.lastIncludedIndex());
        state.advanceLastApplied(durable.lastIncludedIndex());
        log.info("[{}] Restored durable snapshot through index {}", nodeId, durable.lastIncludedIndex());
    }

    public synchronized boolean createIfThresholdReached() throws IOException {
        if (threshold <= 0 || !state.isLeader()) {
            return false;
        }
        RaftLog logStore = state.getLog();
        long snapshotIndex = state.getLastApplied();
        if (snapshotIndex > state.getCommitIndex() || snapshotIndex - logStore.compactedIndex() < threshold) {
            return false;
        }
        long snapshotTerm = logStore.getTerm(snapshotIndex)
                .orElseThrow(() -> new IOException("Cannot snapshot missing applied index " + snapshotIndex));
        byte[] data = stateMachine.takeSnapshot();
        snapshotStore.save(snapshotIndex, snapshotTerm, data);
        // Compact only after the complete snapshot and its directory entry are durable.
        logStore.compactThrough(snapshotIndex, snapshotTerm);
        log.info("[{}] Created snapshot through index {} term {}", nodeId, snapshotIndex, snapshotTerm);
        return true;
    }
}
