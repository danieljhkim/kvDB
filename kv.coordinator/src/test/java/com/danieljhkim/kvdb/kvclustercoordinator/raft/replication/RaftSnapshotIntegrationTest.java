package com.danieljhkim.kvdb.kvclustercoordinator.raft.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftCommand;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftConfiguration;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftNode;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.FileBasedRaftLog;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftLogEntry;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftPersistentStateStore;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftSnapshotStore;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.state.RaftNodeState;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.statemachine.RaftStateMachineImpl;
import com.danieljhkim.kvdb.proto.raft.AppendEntriesResponse;
import com.danieljhkim.kvdb.proto.raft.InstallSnapshotRequest;
import com.danieljhkim.kvdb.proto.raft.InstallSnapshotResponse;
import com.google.protobuf.ByteString;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RaftSnapshotIntegrationTest {

    @TempDir
    Path tempDir;

    @Test
    void leaderSnapshotsAppliedStateAndRestartRestoresCompactedLog() throws Exception {
        Path logPath = tempDir.resolve("leader.log");
        Path snapshotPath = tempDir.resolve("snapshots");
        RaftStateMachineImpl machine = new RaftStateMachineImpl();
        machine.apply(command("node-1")).join();
        machine.apply(command("node-2")).join();

        try (FileBasedRaftLog log = new FileBasedRaftLog(logPath)) {
            log.append(entry(1, command("node-1")));
            log.append(entry(2, command("node-2")));
            RaftNodeState state = new RaftNodeState("leader", log, 1, null);
            state.advanceCommitIndex(2);
            state.advanceLastApplied(2);
            state.becomeCandidate();
            state.becomeLeader(List.of());
            RaftSnapshotManager manager =
                    new RaftSnapshotManager("leader", state, machine, new RaftSnapshotStore(snapshotPath), 2);
            assertTrue(manager.createIfThresholdReached());
            assertEquals(2, log.compactedIndex());
            assertEquals(0, log.size());
        }

        RaftStateMachineImpl restartedMachine = new RaftStateMachineImpl();
        try (FileBasedRaftLog restartedLog = new FileBasedRaftLog(logPath)) {
            RaftNodeState restartedState = new RaftNodeState("leader", restartedLog, 2, null);
            new RaftSnapshotManager("leader", restartedState, restartedMachine, new RaftSnapshotStore(snapshotPath), 2)
                    .restoreOnStartup();
            assertEquals(2, restartedState.getLastApplied());
            assertEquals(2, restartedState.getCommitIndex());
            assertNotNull(restartedMachine.getSnapshot().getNode("node-1"));
            assertNotNull(restartedMachine.getSnapshot().getNode("node-2"));
        }
    }

    @Test
    void followerBehindCompactedPrefixCatchesUpViaSnapshotThenAppendEntries() throws Exception {
        Path leaderDir = tempDir.resolve("leader");
        FileBasedRaftLog leaderLog = new FileBasedRaftLog(leaderDir.resolve("raft.log"));
        leaderLog.append(entry(1, command("node-1")));
        leaderLog.append(entry(2, command("node-2")));
        byte[] data = "snapshot-payload".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        RaftSnapshotStore leaderSnapshots = new RaftSnapshotStore(leaderDir.resolve("snapshots"));
        leaderSnapshots.save(2, 1, data);
        leaderLog.compactThrough(2, 1);

        RaftNodeState leaderState = new RaftNodeState("leader", leaderLog, 2, null);
        leaderState.becomeCandidate();
        leaderState.becomeLeader(List.of("follower"));
        RaftConfiguration configuration = RaftConfiguration.builder()
                .nodeId("leader")
                .clusterMembers(Map.of("leader", "leader:1", "follower", "follower:2"))
                .dataDirectory(leaderDir.toString())
                .build();

        AtomicInteger appendCalls = new AtomicInteger();
        RaftSnapshotStore followerSnapshots = new RaftSnapshotStore(tempDir.resolve("follower-snapshots"));
        RaftReplicationManager manager = new RaftReplicationManager(
                "leader",
                configuration,
                leaderState,
                (peer, request) -> {
                    if (appendCalls.getAndIncrement() == 0) {
                        return CompletableFuture.completedFuture(AppendEntriesResponse.newBuilder()
                                .setTerm(leaderState.getCurrentTerm())
                                .setSuccess(false)
                                .setConflictIndex(1)
                                .build());
                    }
                    return CompletableFuture.completedFuture(AppendEntriesResponse.newBuilder()
                            .setTerm(leaderState.getCurrentTerm())
                            .setSuccess(true)
                            .setMatchIndex(request.getPrevLogIndex() + request.getEntriesCount())
                            .build());
                },
                (peer, request) -> {
                    try {
                        var result = followerSnapshots.installChunk(
                                request.getLastIncludedIndex(),
                                request.getLastIncludedTerm(),
                                request.getOffset(),
                                request.getData().toByteArray(),
                                request.getDone(),
                                request.getTotalSize(),
                                request.getChecksum());
                        return CompletableFuture.completedFuture(InstallSnapshotResponse.newBuilder()
                                .setTerm(leaderState.getCurrentTerm())
                                .setSuccess(result.accepted())
                                .setNextOffset(result.nextOffset())
                                .build());
                    } catch (Exception e) {
                        return CompletableFuture.failedFuture(e);
                    }
                },
                leaderSnapshots);

        manager.replicateToPeer("follower").join();
        assertEquals(2, followerSnapshots.load().orElseThrow().lastIncludedIndex());
        assertEquals(2, leaderState.getMatchIndex("follower"));
        assertEquals(2, appendCalls.get());
        leaderLog.close();
    }

    @Test
    void installSnapshotHandlerAtomicallyReplacesStateAndRestartRestoresIt() throws Exception {
        Path followerDir = tempDir.resolve("follower");
        RaftStateMachineImpl leaderMachine = new RaftStateMachineImpl();
        leaderMachine.apply(command("installed-node")).join();
        byte[] data = leaderMachine.takeSnapshot();
        RaftConfiguration configuration = RaftConfiguration.builder()
                .nodeId("follower")
                .clusterMembers(Map.of("follower", "follower:1"))
                .heartbeatInterval(Duration.ofSeconds(1))
                .electionTimeoutMin(Duration.ofHours(1))
                .electionTimeoutMax(Duration.ofHours(2))
                .dataDirectory(followerDir.toString())
                .build();

        RaftStateMachineImpl followerMachine = new RaftStateMachineImpl();
        FileBasedRaftLog followerLog = new FileBasedRaftLog(followerDir.resolve("raft.log"));
        RaftPersistentStateStore persistentState =
                new RaftPersistentStateStore(followerDir.resolve("state").toString());
        RaftSnapshotStore snapshots = new RaftSnapshotStore(followerDir.resolve("snapshots"));
        RaftNode follower = new RaftNode(
                "follower",
                configuration,
                followerLog,
                persistentState,
                followerMachine,
                (peer, request) -> CompletableFuture.failedFuture(new AssertionError("unexpected vote RPC")),
                (peer, request) -> CompletableFuture.failedFuture(new AssertionError("unexpected append RPC")),
                (peer, request) -> CompletableFuture.failedFuture(new AssertionError("unexpected snapshot RPC")),
                snapshots);
        follower.start();

        var response = follower.handleInstallSnapshot(InstallSnapshotRequest.newBuilder()
                .setTerm(1)
                .setLeaderId("leader")
                .setLastIncludedIndex(5)
                .setLastIncludedTerm(1)
                .setOffset(0)
                .setData(ByteString.copyFrom(data))
                .setDone(true)
                .setTotalSize(data.length)
                .setChecksum(RaftSnapshotStore.checksum(data))
                .build());
        assertTrue(response.getSuccess());
        assertNotNull(followerMachine.getSnapshot().getNode("installed-node"));
        assertEquals(5, followerLog.compactedIndex());
        assertEquals(5, follower.getState().getLastApplied());
        follower.stop();
        followerLog.close();

        RaftStateMachineImpl restartedMachine = new RaftStateMachineImpl();
        FileBasedRaftLog restartedLog = new FileBasedRaftLog(followerDir.resolve("raft.log"));
        RaftNode restarted = new RaftNode(
                "follower",
                configuration,
                restartedLog,
                persistentState,
                restartedMachine,
                (peer, request) -> CompletableFuture.failedFuture(new AssertionError("unexpected vote RPC")),
                (peer, request) -> CompletableFuture.failedFuture(new AssertionError("unexpected append RPC")),
                (peer, request) -> CompletableFuture.failedFuture(new AssertionError("unexpected snapshot RPC")),
                snapshots);
        restarted.start();
        assertNotNull(restartedMachine.getSnapshot().getNode("installed-node"));
        assertEquals(5, restarted.getState().getCommitIndex());
        restarted.stop();
        restartedLog.close();
    }

    private static RaftCommand command(String id) {
        return new RaftCommand.RegisterNode(id, "127.0.0.1:9000", "zone-a");
    }

    private static RaftLogEntry entry(long index, RaftCommand command) {
        return new RaftLogEntry(index, 1, index, command);
    }
}
