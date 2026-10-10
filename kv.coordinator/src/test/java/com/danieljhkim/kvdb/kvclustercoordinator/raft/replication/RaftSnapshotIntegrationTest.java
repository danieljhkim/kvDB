package com.danieljhkim.kvdb.kvclustercoordinator.raft.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftCommand;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftConfiguration;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftNode;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.election.RaftElectionTimer;
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
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
                new RaftPersistentStateStore(leaderDir.resolve("state").toString()),
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

    @Test
    void captureWindowCannotIncludeAnEntryBeyondTheSnapshotHeader() throws Exception {
        Path logPath = tempDir.resolve("capture.log");
        RaftSnapshotStore snapshots = new RaftSnapshotStore(tempDir.resolve("capture-snapshots"));
        AtomicReference<Runnable> captureWindow = new AtomicReference<>();
        RaftStateMachineImpl machine = new RaftStateMachineImpl() {
            @Override
            public byte[] takeSnapshot() {
                captureWindow.get().run();
                return super.takeSnapshot();
            }
        };
        try (FileBasedRaftLog log = new FileBasedRaftLog(logPath)) {
            appendShardHistory(log);
            RaftNodeState state = new RaftNodeState("leader", log, 8, null);
            RaftStateMachineApplier applier = directApplier(state, machine);
            state.advanceCommitIndex(3);
            applier.applyCommittedEntries().join();
            state.becomeCandidate();
            state.becomeLeader(List.of());
            state.advanceCommitIndex(4);

            // Launch the real applier precisely between header selection and payload capture. Wait until it
            // either contends on the application boundary or completes (the unfixed implementation).
            try (BoundaryAttempt application =
                    new BoundaryAttempt(() -> applier.applyCommittedEntries().join())) {
                captureWindow.set(application::startAndAwaitBoundary);
                assertTrue(new RaftSnapshotManager("leader", state, machine, snapshots, 3).createIfThresholdReached());
            }
            var durable = snapshots.load().orElseThrow();
            assertEquals(3, durable.lastIncludedIndex());
            assertEquals(7, durable.lastIncludedTerm());
            RaftStateMachineImpl captured = new RaftStateMachineImpl();
            captured.installSnapshot(durable.data());
            assertEquals(1, captured.getSnapshot().getShard("shard-0").epoch());
            assertEquals(3, log.compactedIndex());
            assertEquals(7, log.compactedTerm());
            assertEquals(4, log.firstIndex());
            assertEquals(8, log.getTerm(4).orElseThrow());
            assertEquals(4, state.getLastApplied());
            assertEquals(2, machine.getSnapshot().getShard("shard-0").epoch());
        }
        assertRestoredSuffix(logPath, snapshots);
    }

    @Test
    void snapshotWaitsForApplicationAcknowledgementAndIndexPublication() throws Exception {
        CompletableFuture<Void> acknowledgement = new CompletableFuture<>();
        CountDownLatch mutated = new CountDownLatch(1);
        RaftStateMachineImpl machine = new RaftStateMachineImpl() {
            @Override
            public CompletableFuture<Void> apply(RaftCommand command) {
                CompletableFuture<Void> applied = super.apply(command);
                if (command instanceof RaftCommand.SetShardReplicas) {
                    applied.join();
                    mutated.countDown();
                    return acknowledgement;
                }
                return applied;
            }
        };
        RaftSnapshotStore snapshots = new RaftSnapshotStore(tempDir.resolve("ack-snapshots"));
        try (FileBasedRaftLog log = new FileBasedRaftLog(tempDir.resolve("ack.log"))) {
            appendShardHistory(log);
            RaftNodeState state = new RaftNodeState("leader", log, 8, null);
            RaftStateMachineApplier applier = directApplier(state, machine);
            state.advanceCommitIndex(3);
            applier.applyCommittedEntries().join();
            state.becomeCandidate();
            state.becomeLeader(List.of());
            state.advanceCommitIndex(4);
            RaftSnapshotManager manager = new RaftSnapshotManager("leader", state, machine, snapshots, 3);
            try (BoundaryAttempt application =
                    new BoundaryAttempt(() -> applier.applyCommittedEntries().join())) {
                application.start();
                try {
                    assertTrue(mutated.await(5, TimeUnit.SECONDS));
                    assertEquals(3, state.getLastApplied());
                    assertEquals(2, machine.getSnapshot().getShard("shard-0").epoch());
                    try (BoundaryAttempt capture = new BoundaryAttempt(() -> {
                        try {
                            assertTrue(manager.createIfThresholdReached());
                        } catch (IOException e) {
                            throw new AssertionError(e);
                        }
                    })) {
                        capture.startAndAwaitBoundary();
                        acknowledgement.complete(null);
                    }
                } finally {
                    acknowledgement.complete(null);
                }
            }
            var durable = snapshots.load().orElseThrow();
            assertEquals(4, durable.lastIncludedIndex());
            assertEquals(8, durable.lastIncludedTerm());
            assertEquals(4, log.compactedIndex());
            assertEquals(8, log.compactedTerm());
            assertEquals(0, log.size());
            RaftStateMachineImpl captured = new RaftStateMachineImpl();
            captured.installSnapshot(durable.data());
            assertEquals(2, captured.getSnapshot().getShard("shard-0").epoch());
        }
    }

    @Test
    void installationWindowAppliesOnlyTheSuffixAfterPublishingSnapshotBoundary() throws Exception {
        Path logPath = tempDir.resolve("install.log");
        RaftSnapshotStore snapshots = new RaftSnapshotStore(tempDir.resolve("install-snapshots"));
        AtomicReference<Runnable> installWindow = new AtomicReference<>();
        RaftStateMachineImpl machine = new RaftStateMachineImpl() {
            @Override
            public void installSnapshot(byte[] data) throws IOException {
                installWindow.get().run();
                super.installSnapshot(data);
            }
        };
        RaftStateMachineImpl leader = new RaftStateMachineImpl();
        leader.apply(command("node-1")).join();
        leader.apply(command("node-2")).join();
        leader.apply(new RaftCommand.InitShards(1, 1)).join();
        byte[] data = leader.takeSnapshot();
        RaftConfiguration config = RaftConfiguration.builder()
                .nodeId("follower")
                .clusterMembers(Map.of("follower", "follower:1"))
                .electionTimeoutMin(Duration.ofHours(1))
                .electionTimeoutMax(Duration.ofHours(2))
                .build();
        try (FileBasedRaftLog log = new FileBasedRaftLog(logPath);
                var scheduler = Executors.newSingleThreadScheduledExecutor()) {
            appendShardHistory(log);
            RaftNodeState state = new RaftNodeState("follower", log, 8, null);
            RaftStateMachineApplier applier = directApplier(state, machine);
            state.advanceCommitIndex(2);
            applier.applyCommittedEntries().join();
            state.advanceCommitIndex(4);
            RaftElectionTimer timer = new RaftElectionTimer("follower", config, scheduler, () -> {});
            RaftInstallSnapshotHandler handler = new RaftInstallSnapshotHandler(
                    "follower",
                    state,
                    new RaftPersistentStateStore(
                            tempDir.resolve("install-state").toString()),
                    snapshots,
                    machine,
                    timer);
            InstallSnapshotRequest request = InstallSnapshotRequest.newBuilder()
                    .setTerm(9)
                    .setLeaderId("leader")
                    .setLastIncludedIndex(3)
                    .setLastIncludedTerm(7)
                    .setData(ByteString.copyFrom(data))
                    .setDone(true)
                    .setTotalSize(data.length)
                    .setChecksum(RaftSnapshotStore.checksum(data))
                    .build();
            try {
                try (BoundaryAttempt application = new BoundaryAttempt(
                        () -> applier.applyCommittedEntries().join())) {
                    installWindow.set(application::startAndAwaitBoundary);
                    assertTrue(handler.handleInstallSnapshot(request).getSuccess());
                }
                assertEquals(4, state.getLastApplied());
                assertEquals(4, state.getCommitIndex());
                assertEquals(2, machine.getSnapshot().getShard("shard-0").epoch());
                assertEquals(3, log.compactedIndex());
                assertEquals(7, log.compactedTerm());
                assertEquals(4, log.firstIndex());
                // A delayed retry cannot roll back the applied suffix or compact beyond the captured boundary.
                assertTrue(handler.handleInstallSnapshot(request).getSuccess());
                assertEquals(4, state.getLastApplied());
                assertEquals(2, machine.getSnapshot().getShard("shard-0").epoch());
                assertEquals(3, snapshots.load().orElseThrow().lastIncludedIndex());
            } finally {
                timer.stop();
            }
        }
        assertRestoredSuffix(logPath, snapshots);
    }

    private static void appendShardHistory(FileBasedRaftLog log) throws IOException {
        log.append(entry(1, command("node-1")));
        log.append(entry(2, command("node-2")));
        log.append(new RaftLogEntry(3, 7, 3, new RaftCommand.InitShards(1, 1)));
        log.append(new RaftLogEntry(4, 8, 4, new RaftCommand.SetShardReplicas("shard-0", List.of("node-2"))));
    }

    private static RaftStateMachineApplier directApplier(RaftNodeState state, RaftStateMachineImpl machine) {
        RaftStateMachineApplier applier = new RaftStateMachineApplier("node", state, machine, Runnable::run);
        applier.start();
        return applier;
    }

    private static void assertRestoredSuffix(Path logPath, RaftSnapshotStore snapshots) throws Exception {
        try (FileBasedRaftLog log = new FileBasedRaftLog(logPath)) {
            RaftNodeState state = new RaftNodeState("restarted", log, 9, null);
            RaftStateMachineImpl machine = new RaftStateMachineImpl();
            new RaftSnapshotManager("restarted", state, machine, snapshots, 3).restoreOnStartup();
            assertEquals(3, state.getLastApplied());
            assertEquals(7, log.compactedTerm());
            assertEquals(1, machine.getSnapshot().getShard("shard-0").epoch());
            state.advanceCommitIndex(4);
            directApplier(state, machine).applyCommittedEntries().join();
            assertEquals(4, state.getLastApplied());
            assertEquals(2, machine.getSnapshot().getShard("shard-0").epoch());
            assertEquals(
                    List.of("node-2"), machine.getSnapshot().getShard("shard-0").replicas());
        }
    }

    /** Observes contention rather than relying on sleeps to decide when a window has been exercised. */
    private static final class BoundaryAttempt implements AutoCloseable {
        private final CompletableFuture<Void> completion = new CompletableFuture<>();
        private final Thread thread;

        private BoundaryAttempt(Runnable action) {
            thread = new Thread(
                    () -> {
                        try {
                            action.run();
                            completion.complete(null);
                        } catch (Throwable error) {
                            completion.completeExceptionally(error);
                        }
                    },
                    "snapshot-boundary-regression");
        }

        private void start() {
            thread.start();
        }

        private void startAndAwaitBoundary() {
            start();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            try {
                while (thread.isAlive() && thread.getState() != Thread.State.BLOCKED) {
                    assertTrue(System.nanoTime() < deadline, "worker neither contended nor completed");
                    thread.join(1);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }

        @Override
        public void close() throws Exception {
            thread.join(5000);
            assertFalse(thread.isAlive(), "regression worker did not finish");
            completion.get(5, TimeUnit.SECONDS);
        }
    }

    private static RaftCommand command(String id) {
        return new RaftCommand.RegisterNode(id, "127.0.0.1:9000", "zone-a");
    }

    private static RaftLogEntry entry(long index, RaftCommand command) {
        return new RaftLogEntry(index, 1, index, command);
    }
}
