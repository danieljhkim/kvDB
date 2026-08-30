package com.danieljhkim.kvdb.kvclustercoordinator.raft.replication;

import com.danieljhkim.kvdb.kvclustercoordinator.raft.election.RaftElectionTimer;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftPersistentStateStore;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftSnapshotStore;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.state.RaftNodeState;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.statemachine.RaftStateMachine;
import com.danieljhkim.kvdb.proto.raft.InstallSnapshotRequest;
import com.danieljhkim.kvdb.proto.raft.InstallSnapshotResponse;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;

/** Receives, durably installs, and applies chunked Raft snapshots. */
@Slf4j
public class RaftInstallSnapshotHandler {

    private final String nodeId;
    private final RaftNodeState state;
    private final RaftPersistentStateStore persistentStore;
    private final RaftSnapshotStore snapshotStore;
    private final RaftStateMachine stateMachine;
    private final RaftElectionTimer electionTimer;

    public RaftInstallSnapshotHandler(
            String nodeId,
            RaftNodeState state,
            RaftPersistentStateStore persistentStore,
            RaftSnapshotStore snapshotStore,
            RaftStateMachine stateMachine,
            RaftElectionTimer electionTimer) {
        this.nodeId = nodeId;
        this.state = state;
        this.persistentStore = persistentStore;
        this.snapshotStore = snapshotStore;
        this.stateMachine = stateMachine;
        this.electionTimer = electionTimer;
    }

    public synchronized InstallSnapshotResponse handleInstallSnapshot(InstallSnapshotRequest request)
            throws IOException {
        if (request.getTerm() < state.getCurrentTerm()) {
            return response(false, 0);
        }
        if (request.getTerm() > state.getCurrentTerm()) {
            persistentStore.save(request.getTerm(), null);
            state.updateTerm(request.getTerm());
        }
        state.transitionToFollower(request.getLeaderId());
        electionTimer.reset();

        if (request.getLastIncludedIndex() <= state.getLastApplied()) {
            // A retry or delayed older snapshot cannot replace newer applied state.
            return response(true, request.getTotalSize());
        }

        RaftSnapshotStore.ChunkResult result = snapshotStore.installChunk(
                request.getLastIncludedIndex(),
                request.getLastIncludedTerm(),
                request.getOffset(),
                request.getData().toByteArray(),
                request.getDone(),
                request.getTotalSize(),
                request.getChecksum());
        if (!result.accepted()) {
            return response(false, result.nextOffset());
        }
        if (result.complete()) {
            RaftSnapshotStore.Snapshot snapshot = result.snapshot();
            // The snapshot is durable before either state-machine replacement or log compaction.
            stateMachine.installSnapshot(snapshot.data());
            state.getLog().compactThrough(snapshot.lastIncludedIndex(), snapshot.lastIncludedTerm());
            state.advanceCommitIndex(snapshot.lastIncludedIndex());
            state.advanceLastApplied(snapshot.lastIncludedIndex());
            log.info(
                    "[{}] Installed snapshot through index {} term {}",
                    nodeId,
                    snapshot.lastIncludedIndex(),
                    snapshot.lastIncludedTerm());
        }
        return response(true, result.nextOffset());
    }

    private InstallSnapshotResponse response(boolean success, long nextOffset) {
        return InstallSnapshotResponse.newBuilder()
                .setTerm(state.getCurrentTerm())
                .setSuccess(success)
                .setFollowerId(nodeId)
                .setNextOffset(nextOffset)
                .build();
    }
}
