package com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftCommand;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftConfiguration;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.election.RaftElectionManager;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.election.RaftElectionTimer;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.election.RaftVoteHandler;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.replication.RaftAppendEntriesHandler;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.replication.RaftHeartbeatManager;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.replication.RaftInstallSnapshotHandler;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.replication.RaftReplicationManager;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.state.RaftNodeState;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.state.RaftRole;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.statemachine.StubRaftStateMachine;
import com.danieljhkim.kvdb.proto.raft.AppendEntriesRequest;
import com.danieljhkim.kvdb.proto.raft.AppendEntriesResponse;
import com.danieljhkim.kvdb.proto.raft.InstallSnapshotRequest;
import com.danieljhkim.kvdb.proto.raft.InstallSnapshotResponse;
import com.danieljhkim.kvdb.proto.raft.RequestVoteRequest;
import com.danieljhkim.kvdb.proto.raft.RequestVoteResponse;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class RaftPersistenceTest {

    @TempDir
    Path tempDir;

    @Test
    void compactionSurvivesRestartWithAbsoluteIndexes() throws Exception {
        Path path = tempDir.resolve("raft.log");
        try (FileBasedRaftLog log = new FileBasedRaftLog(path)) {
            log.append(entry(1, 1));
            log.append(entry(2, 1));
            log.append(entry(3, 2));
            log.compactThrough(2, 1);
            assertEquals(2, log.compactedIndex());
            assertEquals(3, log.lastIndex());
            assertEquals(1, log.size());
        }

        try (FileBasedRaftLog restarted = new FileBasedRaftLog(path)) {
            assertEquals(2, restarted.compactedIndex());
            assertEquals(1, restarted.compactedTerm());
            assertTrue(restarted.getEntry(2).isEmpty());
            assertEquals(2, restarted.getEntry(3).orElseThrow().term());
            restarted.append(entry(4, 2));
            assertEquals(4, restarted.lastIndex());
        }
    }

    @Test
    void readsLegacyLogThenUpgradesOnMutation() throws Exception {
        Path path = tempDir.resolve("legacy.log");
        try (DataOutputStream output = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(path)))) {
            byte[] first = entry(1, 1).toBytes();
            output.writeInt(first.length);
            output.write(first);
        }
        try (FileBasedRaftLog log = new FileBasedRaftLog(path)) {
            assertEquals(1, log.lastIndex());
            log.append(entry(2, 2));
        }
        assertEquals(
                FileBasedRaftLog.MAGIC,
                ByteBuffer.wrap(Files.readAllBytes(path)).getInt());
    }

    @Test
    void truncatedOversizedAndChecksumInvalidLogsFailDeterministically() throws Exception {
        Path truncated = tempDir.resolve("truncated.log");
        try (FileBasedRaftLog ignored = new FileBasedRaftLog(truncated)) {}
        byte[] bytes = Files.readAllBytes(truncated);
        Files.write(truncated, java.util.Arrays.copyOf(bytes, bytes.length - 1));
        assertTrue(assertThrows(IOException.class, () -> new FileBasedRaftLog(truncated))
                .getMessage()
                .contains("truncated"));

        Path oversized = tempDir.resolve("oversized.log");
        Files.write(
                oversized,
                ByteBuffer.allocate(4)
                        .putInt(FileBasedRaftLog.MAX_ENTRY_BYTES + 1)
                        .array());
        assertTrue(assertThrows(IOException.class, () -> new FileBasedRaftLog(oversized))
                .getMessage()
                .contains("outside"));

        Path checksum = tempDir.resolve("checksum.log");
        try (FileBasedRaftLog log = new FileBasedRaftLog(checksum)) {
            log.append(entry(1, 1));
        }
        byte[] corrupt = Files.readAllBytes(checksum);
        corrupt[corrupt.length - 1] ^= 1;
        Files.write(checksum, corrupt);
        assertTrue(assertThrows(IOException.class, () -> new FileBasedRaftLog(checksum))
                .getMessage()
                .contains("checksum"));
    }

    @Test
    void stateStoreReadsLegacyAndFailsClosedOnCorruption() throws Exception {
        Path stateDir = tempDir.resolve("state");
        Files.createDirectories(stateDir);
        Properties properties = new Properties();
        properties.setProperty("currentTerm", "7");
        properties.setProperty("votedFor", "node-2");
        try (var output = Files.newOutputStream(stateDir.resolve("raft_state.properties"))) {
            properties.store(output, "legacy");
        }
        RaftPersistentStateStore store = new RaftPersistentStateStore(stateDir.toString());
        assertEquals(7, store.load().getCurrentTerm());
        store.save(8, "node-3");
        assertEquals(
                RaftPersistentStateStore.MAGIC,
                ByteBuffer.wrap(Files.readAllBytes(stateDir.resolve("raft_state.properties")))
                        .getInt());

        byte[] corrupt = Files.readAllBytes(stateDir.resolve("raft_state.properties"));
        corrupt[corrupt.length - 1] ^= 1;
        Files.write(stateDir.resolve("raft_state.properties"), corrupt);
        assertTrue(assertThrows(IOException.class, store::load).getMessage().contains("checksum"));

        Files.write(
                stateDir.resolve("raft_state.properties"),
                ByteBuffer.allocate(12)
                        .putInt(RaftPersistentStateStore.MAGIC)
                        .putInt(RaftPersistentStateStore.FORMAT_VERSION)
                        .putInt(RaftPersistentStateStore.MAX_PAYLOAD_BYTES + 1)
                        .array());
        assertTrue(assertThrows(IOException.class, store::load).getMessage().contains("outside"));

        Files.write(stateDir.resolve("raft_state.properties"), new byte[] {1});
        assertTrue(assertThrows(IOException.class, store::load).getMessage().contains("truncated"));
    }

    @Test
    void fileAndDirectoryFsyncFailuresAreReported() throws Exception {
        Path stateDir = tempDir.resolve("fault-state");
        RaftPersistentStateStore initial = new RaftPersistentStateStore(stateDir.toString());
        initial.save(1, null);

        AtomicBoolean fileForceReached = new AtomicBoolean();
        DurableFileOps fileFailure = new DurableFileOps() {
            @Override
            public void forceFile(Path path) throws IOException {
                fileForceReached.set(true);
                throw new IOException("injected file fsync failure");
            }
        };
        RaftPersistentStateStore failingFileStore = new RaftPersistentStateStore(stateDir, fileFailure);
        assertThrows(IOException.class, () -> failingFileStore.save(2, null));
        assertTrue(fileForceReached.get());
        assertEquals(1, initial.load().getCurrentTerm());

        AtomicBoolean directoryForceReached = new AtomicBoolean();
        DurableFileOps directoryFailure = new DurableFileOps() {
            @Override
            public void forceDirectory(Path path) throws IOException {
                directoryForceReached.set(true);
                throw new IOException("injected directory fsync failure");
            }
        };
        RaftPersistentStateStore failingDirectoryStore = new RaftPersistentStateStore(stateDir, directoryFailure);
        assertThrows(IOException.class, () -> failingDirectoryStore.save(3, null));
        assertTrue(directoryForceReached.get());
    }

    @Test
    void higherTermFromAppendEntriesResponseIsDurableBeforeStepDown() throws Exception {
        Path stateDir = tempDir.resolve("append-state");
        try (LeaderFixture fixture = new LeaderFixture(stateDir, new DurableFileOps())) {
            RaftReplicationManager manager = fixture.replicationManager(
                    (peer, request) -> CompletableFuture.completedFuture(higherTermAppendResponse()), null);

            assertThrows(CompletionException.class, () -> manager.replicateToPeer("follower")
                    .join());

            assertSteppedDownAndRecoverable(fixture, stateDir);
        }
    }

    @Test
    void higherTermFromInstallSnapshotResponseIsDurableBeforeStepDown() throws Exception {
        Path stateDir = tempDir.resolve("snapshot-state");
        try (LeaderFixture fixture = new LeaderFixture(stateDir, new DurableFileOps())) {
            RaftSnapshotStore snapshots = new RaftSnapshotStore(tempDir.resolve("snapshots"));
            snapshots.save(1, 1, new byte[] {1, 2, 3});
            fixture.log.compactThrough(1, 1);
            fixture.state.setNextIndex("follower", 1);
            RaftReplicationManager manager = fixture.replicationManager(
                    (peer, request) -> CompletableFuture.failedFuture(new AssertionError("unexpected AppendEntries")),
                    snapshots);

            assertThrows(CompletionException.class, () -> manager.replicateToPeer("follower")
                    .join());

            assertSteppedDownAndRecoverable(fixture, stateDir);
        }
    }

    @Test
    void higherTermFromHeartbeatResponseIsDurableBeforeStepDown() throws Exception {
        Path stateDir = tempDir.resolve("heartbeat-state");
        try (LeaderFixture fixture = new LeaderFixture(stateDir, new DurableFileOps())) {
            runOneHeartbeat(fixture);

            assertSteppedDownAndRecoverable(fixture, stateDir);
        }
    }

    @Test
    void durabilityFailureOnResponseTermDoesNotExposeUnpersistedTerm() throws Exception {
        Path stateDir = tempDir.resolve("failing-response-state");
        DurableFileOps failing = new DurableFileOps() {
            @Override
            public void forceFile(Path path) throws IOException {
                throw new IOException("injected file fsync failure");
            }
        };
        try (LeaderFixture fixture = new LeaderFixture(stateDir, failing)) {
            RaftSnapshotStore snapshots = new RaftSnapshotStore(tempDir.resolve("failing-snapshots"));
            snapshots.save(1, 1, new byte[] {1, 2, 3});

            RaftReplicationManager appendManager = fixture.replicationManager(
                    (peer, request) -> CompletableFuture.completedFuture(higherTermAppendResponse()), null);
            assertThrows(
                    CompletionException.class,
                    () -> appendManager.replicateToPeer("follower").join());
            assertLeaderAtDurableTerm(fixture, stateDir);

            runOneHeartbeat(fixture);
            assertLeaderAtDurableTerm(fixture, stateDir);

            fixture.log.compactThrough(1, 1);
            fixture.state.setNextIndex("follower", 1);
            RaftReplicationManager snapshotManager = fixture.replicationManager(
                    (peer, request) -> CompletableFuture.failedFuture(new AssertionError("unexpected AppendEntries")),
                    snapshots,
                    (peer, request) -> CompletableFuture.completedFuture(
                            InstallSnapshotResponse.newBuilder().setTerm(5).build()));
            assertThrows(
                    CompletionException.class,
                    () -> snapshotManager.replicateToPeer("follower").join());
            assertLeaderAtDurableTerm(fixture, stateDir);
        }
    }

    @ParameterizedTest
    @EnumSource(IncomingRpc.class)
    void incomingTermSaveCannotEraseConcurrentVoteAfterRestart(IncomingRpc rpc) throws Exception {
        try (FollowerFixture fixture = new FollowerFixture(tempDir, null)) {
            fixture.store.pause(5, null, false);
            PendingCall<Boolean> incoming = fixture.call(() -> fixture.incoming(rpc, 5));
            fixture.store.awaitSave();
            PendingCall<RequestVoteResponse> vote = fixture.call(() -> fixture.vote(5, "candidate-a"));
            assertWaitingForState(vote, fixture.state);

            fixture.store.resume();
            assertTrue(incoming.get());
            assertTrue(vote.get().getVoteGranted());
            fixture.assertDurableVoteAndRestart(5, "candidate-a", "candidate-b");
        }
    }

    @ParameterizedTest
    @EnumSource(IncomingRpc.class)
    void incomingTermSaveCannotRegressConcurrentHigherTermVote(IncomingRpc rpc) throws Exception {
        try (FollowerFixture fixture = new FollowerFixture(tempDir, null)) {
            fixture.store.pause(5, null, false);
            PendingCall<Boolean> incoming = fixture.call(() -> fixture.incoming(rpc, 5));
            fixture.store.awaitSave();
            PendingCall<RequestVoteResponse> vote = fixture.call(() -> fixture.vote(6, "candidate-a"));
            assertWaitingForState(vote, fixture.state);

            fixture.store.resume();
            assertTrue(incoming.get());
            assertTrue(vote.get().getVoteGranted());
            fixture.assertDurableVoteAndRestart(6, "candidate-a", "candidate-b");
        }
    }

    @ParameterizedTest
    @EnumSource(IncomingRpc.class)
    void requestQueuedBehindVoteSavePreservesThatVote(IncomingRpc rpc) throws Exception {
        try (FollowerFixture fixture = new FollowerFixture(tempDir, null)) {
            fixture.store.pause(5, "candidate-a", false);
            PendingCall<RequestVoteResponse> vote = fixture.call(() -> fixture.vote(5, "candidate-a"));
            fixture.store.awaitSave();
            PendingCall<Boolean> incoming = fixture.call(() -> fixture.incoming(rpc, 5));
            assertWaitingForState(incoming, fixture.state);

            fixture.store.resume();
            assertTrue(vote.get().getVoteGranted());
            assertTrue(incoming.get());
            fixture.assertDurableVoteAndRestart(5, "candidate-a", "candidate-b");
        }
    }

    @ParameterizedTest
    @EnumSource(IncomingRpc.class)
    void failedIncomingTermFsyncPreservesPriorVoteAndRejectsConcurrentSecondVote(IncomingRpc rpc) throws Exception {
        try (FollowerFixture fixture = new FollowerFixture(tempDir, "candidate-a")) {
            fixture.store.pause(5, null, true);
            PendingCall<Boolean> incoming = fixture.call(() -> {
                if (rpc == IncomingRpc.SNAPSHOT) {
                    assertThrows(IOException.class, () -> fixture.incoming(rpc, 5));
                    return false;
                }
                return fixture.incoming(rpc, 5);
            });
            fixture.store.awaitSave();
            PendingCall<RequestVoteResponse> vote = fixture.call(() -> fixture.vote(4, "candidate-b"));
            assertWaitingForState(vote, fixture.state);

            fixture.store.resume();
            assertFalse(incoming.get());
            assertFalse(vote.get().getVoteGranted());
            fixture.assertDurableVoteAndRestart(4, "candidate-a", "candidate-b");
        }
    }

    @ParameterizedTest
    @EnumSource(IncomingRpc.class)
    void failedVoteFsyncNeverGrantsVoteWhileIncomingRequestWaits(IncomingRpc rpc) throws Exception {
        try (FollowerFixture fixture = new FollowerFixture(tempDir, null)) {
            fixture.store.pause(5, "candidate-a", true);
            PendingCall<RequestVoteResponse> vote = fixture.call(() -> fixture.vote(5, "candidate-a"));
            fixture.store.awaitSave();
            PendingCall<Boolean> incoming = fixture.call(() -> fixture.incoming(rpc, 5));
            assertWaitingForState(incoming, fixture.state);

            fixture.store.resume();
            assertFalse(vote.get().getVoteGranted());
            assertTrue(incoming.get());
            assertNull(fixture.state.getVotedFor());
            assertTrue(fixture.vote(5, "candidate-b").getVoteGranted());
            fixture.assertDurableVoteAndRestart(5, "candidate-b", "candidate-a");
        }
    }

    @ParameterizedTest
    @EnumSource(IncomingRpc.class)
    void concurrentIncomingHandlersSerializeTermChecks(IncomingRpc first) throws Exception {
        try (FollowerFixture fixture = new FollowerFixture(tempDir, null)) {
            fixture.store.pause(5, null, false);
            PendingCall<Boolean> incoming = fixture.call(() -> fixture.incoming(first, 5));
            fixture.store.awaitSave();
            IncomingRpc second = first == IncomingRpc.APPEND ? IncomingRpc.SNAPSHOT : IncomingRpc.APPEND;
            PendingCall<Boolean> higher = fixture.call(() -> fixture.incoming(second, 6));
            assertWaitingForState(higher, fixture.state);

            fixture.store.resume();
            assertTrue(incoming.get());
            assertTrue(higher.get());
            assertTrue(fixture.vote(6, "candidate-a").getVoteGranted());
            fixture.assertDurableVoteAndRestart(6, "candidate-a", "candidate-b");
            assertFalse(fixture.incoming(first, 5), "A delayed lower-term request must be rejected");
            fixture.assertDurableVoteAndRestart(6, "candidate-a", "candidate-b");
        }
    }

    @ParameterizedTest
    @EnumSource(IncomingRpc.class)
    void electionQueuedBehindIncomingTermSaveUsesTheNewTerm(IncomingRpc rpc) throws Exception {
        try (FollowerFixture fixture = new FollowerFixture(tempDir, null)) {
            fixture.store.pause(5, null, false);
            PendingCall<Boolean> incoming = fixture.call(() -> fixture.incoming(rpc, 5));
            fixture.store.awaitSave();
            PendingCall<Void> election = fixture.call(() -> {
                fixture.elections.startElection();
                return null;
            });
            assertWaitingForState(election, fixture.state);

            fixture.store.resume();
            assertTrue(incoming.get());
            election.get();
            assertEquals(RaftRole.CANDIDATE, fixture.state.getCurrentRole());
            fixture.assertDurableVoteAndRestart(6, "follower", "candidate-a");
        }
    }

    @ParameterizedTest
    @EnumSource(IncomingRpc.class)
    void incomingRequestQueuedBehindElectionCannotEraseSelfVote(IncomingRpc rpc) throws Exception {
        try (FollowerFixture fixture = new FollowerFixture(tempDir, null)) {
            fixture.store.pause(5, "follower", false);
            PendingCall<Void> election = fixture.call(() -> {
                fixture.elections.startElection();
                return null;
            });
            fixture.store.awaitSave();
            PendingCall<Boolean> incoming = fixture.call(() -> fixture.incoming(rpc, 5));
            assertWaitingForState(incoming, fixture.state);

            fixture.store.resume();
            election.get();
            assertTrue(incoming.get());
            fixture.assertDurableVoteAndRestart(5, "follower", "candidate-a");
        }
    }

    private enum IncomingRpc {
        APPEND,
        SNAPSHOT
    }

    /** Observe actual contention on the shared monitor, rather than assuming a sleeping thread ran. */
    private static void assertWaitingForState(PendingCall<?> call, RaftNodeState state) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!call.future.isDone() && System.nanoTime() < deadline) {
            ThreadInfo info = ManagementFactory.getThreadMXBean().getThreadInfo(call.thread.threadId());
            if (info != null
                    && info.getThreadState() == Thread.State.BLOCKED
                    && info.getLockInfo() != null
                    && info.getLockInfo().getIdentityHashCode() == System.identityHashCode(state)) {
                return;
            }
            Thread.sleep(10);
        }
        assertTrue(false, "Concurrent operation did not block on the shared Raft state monitor");
    }

    private record PendingCall<T>(Thread thread, FutureTask<T> future) {
        T get() throws Exception {
            return future.get(10, TimeUnit.SECONDS);
        }
    }

    /** Pauses before the real store replacement and optionally fails the real file-fsync boundary once. */
    private static final class PausingStateStore extends RaftPersistentStateStore {
        private final AtomicBoolean failFsync;
        private final AtomicBoolean pauseNext = new AtomicBoolean();
        private final CountDownLatch saveEntered = new CountDownLatch(1);
        private final CountDownLatch saveReleased = new CountDownLatch(1);
        private long pausedTerm;
        private String pausedVote;
        private boolean failPausedSave;

        PausingStateStore(Path directory, AtomicBoolean failFsync) throws IOException {
            super(directory, new DurableFileOps() {
                @Override
                public void forceFile(Path path) throws IOException {
                    if (failFsync.getAndSet(false)) {
                        throw new IOException("injected incoming term/vote file fsync failure");
                    }
                    super.forceFile(path);
                }
            });
            this.failFsync = failFsync;
        }

        void pause(long term, String vote, boolean fail) {
            pausedTerm = term;
            pausedVote = vote;
            failPausedSave = fail;
            pauseNext.set(true);
        }

        @Override
        public void save(long term, String vote) throws IOException {
            if (term == pausedTerm && Objects.equals(vote, pausedVote) && pauseNext.compareAndSet(true, false)) {
                saveEntered.countDown();
                try {
                    if (!saveReleased.await(10, TimeUnit.SECONDS)) {
                        throw new IOException("Timed out waiting to release controlled save");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted at controlled save", e);
                }
                failFsync.set(failPausedSave);
            }
            super.save(term, vote);
        }

        void awaitSave() throws InterruptedException {
            assertTrue(saveEntered.await(10, TimeUnit.SECONDS), "Save boundary was not reached");
        }

        void resume() {
            saveReleased.countDown();
        }
    }

    private static final class FollowerFixture implements AutoCloseable {
        private final Path directory;
        private final FileBasedRaftLog log;
        private final PausingStateStore store;
        private final RaftNodeState state;
        private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        private final List<Thread> calls = new ArrayList<>();
        private final RaftElectionTimer timer;
        private final RaftVoteHandler votes;
        private final RaftAppendEntriesHandler appends;
        private final RaftInstallSnapshotHandler snapshots;
        private final RaftElectionManager elections;

        FollowerFixture(Path directory, String initialVote) throws IOException {
            this.directory = directory;
            this.store = new PausingStateStore(directory, new AtomicBoolean());
            store.save(4, initialVote);
            this.log = new FileBasedRaftLog(directory.resolve("raft.log"));
            this.state = new RaftNodeState("follower", log, 4, initialVote);
            RaftConfiguration config = RaftConfiguration.builder()
                    .nodeId("follower")
                    .clusterMembers(Map.of("follower", "follower:1", "peer", "peer:2"))
                    .dataDirectory(directory.toString())
                    .electionTimeoutMin(Duration.ofHours(1))
                    .electionTimeoutMax(Duration.ofHours(2))
                    .build();
            this.timer = new RaftElectionTimer("follower", config, scheduler, () -> {
                throw new AssertionError("unexpected election timeout");
            });
            this.votes = new RaftVoteHandler("follower", state, store, timer);
            this.appends = new RaftAppendEntriesHandler("follower", state, store, timer);
            this.snapshots = new RaftInstallSnapshotHandler(
                    "follower",
                    state,
                    store,
                    new RaftSnapshotStore(directory.resolve("snapshots")),
                    new StubRaftStateMachine(),
                    timer);
            this.elections = new RaftElectionManager(
                    "follower", config, state, store, timer, (peer, request) -> new CompletableFuture<>());
        }

        boolean incoming(IncomingRpc rpc, long term) throws IOException {
            if (rpc == IncomingRpc.APPEND) {
                return appends.handleAppendEntries(AppendEntriesRequest.newBuilder()
                                .setTerm(term)
                                .setLeaderId("leader")
                                .build())
                        .getSuccess();
            }
            // An already-applied snapshot still exercises term handling, without unrelated chunk/application work.
            return snapshots
                    .handleInstallSnapshot(InstallSnapshotRequest.newBuilder()
                            .setTerm(term)
                            .setLeaderId("leader")
                            .setLastIncludedIndex(0)
                            .build())
                    .getSuccess();
        }

        RequestVoteResponse vote(long term, String candidate) {
            return votes.handleRequestVote(voteRequest(term, candidate));
        }

        <T> PendingCall<T> call(Callable<T> operation) {
            FutureTask<T> future = new FutureTask<>(operation);
            Thread thread = new Thread(future, "controlled-raft-handler");
            calls.add(thread);
            thread.start();
            return new PendingCall<>(thread, future);
        }

        void assertDurableVoteAndRestart(long term, String candidate, String rejectedCandidate) throws Exception {
            assertEquals(term, state.getCurrentTerm());
            assertEquals(candidate, state.getVotedFor());
            // Open new store, log, state and handler instances as a restarted follower would.
            RaftPersistentStateStore restartedStore = new RaftPersistentStateStore(directory.toString());
            var durable = restartedStore.load();
            assertEquals(term, durable.getCurrentTerm());
            assertEquals(candidate, durable.getVotedFor());
            try (FileBasedRaftLog restartedLog = new FileBasedRaftLog(directory.resolve("raft.log"))) {
                RaftNodeState restartedState =
                        new RaftNodeState("follower", restartedLog, durable.getCurrentTerm(), durable.getVotedFor());
                RaftVoteHandler restartedVotes = new RaftVoteHandler("follower", restartedState, restartedStore, timer);
                assertFalse(restartedVotes
                        .handleRequestVote(voteRequest(term, rejectedCandidate))
                        .getVoteGranted());
                assertTrue(restartedVotes
                        .handleRequestVote(voteRequest(term, candidate))
                        .getVoteGranted());
            }
        }

        private static RequestVoteRequest voteRequest(long term, String candidate) {
            return RequestVoteRequest.newBuilder()
                    .setTerm(term)
                    .setCandidateId(candidate)
                    .build();
        }

        @Override
        public void close() throws Exception {
            store.resume();
            for (Thread call : calls) {
                call.join(TimeUnit.SECONDS.toMillis(10));
                assertFalse(call.isAlive(), "Controlled handler thread did not finish");
            }
            timer.stop();
            scheduler.shutdownNow();
            assertTrue(scheduler.awaitTermination(10, TimeUnit.SECONDS));
            log.close();
        }
    }

    private static AppendEntriesResponse higherTermAppendResponse() {
        return AppendEntriesResponse.newBuilder().setTerm(5).setSuccess(false).build();
    }

    private static void runOneHeartbeat(LeaderFixture fixture) throws Exception {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        CountDownLatch rpcSent = new CountDownLatch(1);
        RaftHeartbeatManager heartbeats = new RaftHeartbeatManager(
                "leader", fixture.configuration, fixture.state, fixture.store, scheduler, (peer, request) -> {
                    rpcSent.countDown();
                    return CompletableFuture.completedFuture(higherTermAppendResponse());
                });
        heartbeats.start();
        assertTrue(rpcSent.await(10, TimeUnit.SECONDS));
        // Shutdown lets the in-flight heartbeat task, including its response handling, run to completion.
        scheduler.shutdown();
        assertTrue(scheduler.awaitTermination(10, TimeUnit.SECONDS));
    }

    private static void assertSteppedDownAndRecoverable(LeaderFixture fixture, Path stateDir) throws Exception {
        assertEquals(5, fixture.state.getCurrentTerm());
        assertEquals(RaftRole.FOLLOWER, fixture.state.getCurrentRole());
        assertNull(fixture.state.getVotedFor());

        RaftPersistentStateStore.PersistentState recovered = new RaftPersistentStateStore(stateDir.toString()).load();
        assertEquals(5, recovered.getCurrentTerm());
        assertNull(recovered.getVotedFor());
    }

    private static void assertLeaderAtDurableTerm(LeaderFixture fixture, Path stateDir) throws Exception {
        assertEquals(2, fixture.state.getCurrentTerm());
        assertEquals(RaftRole.LEADER, fixture.state.getCurrentRole());
        assertEquals("leader", fixture.state.getVotedFor());

        RaftPersistentStateStore.PersistentState recovered = new RaftPersistentStateStore(stateDir.toString()).load();
        assertEquals(2, recovered.getCurrentTerm());
        assertEquals("leader", recovered.getVotedFor());
    }

    /** A term-2 leader whose term and self-vote are already durable in {@code stateDir}. */
    private static final class LeaderFixture implements AutoCloseable {
        final FileBasedRaftLog log;
        final RaftNodeState state;
        final RaftPersistentStateStore store;
        final RaftConfiguration configuration;

        LeaderFixture(Path stateDir, DurableFileOps failingOps) throws IOException {
            new RaftPersistentStateStore(stateDir.toString()).save(2, "leader");
            this.store = new RaftPersistentStateStore(stateDir, failingOps);
            this.log = new FileBasedRaftLog(stateDir.resolve("raft.log"));
            log.append(entry(1, 1));
            this.state = new RaftNodeState("leader", log, 1, null);
            state.becomeCandidate(); // term 2, voted for self
            state.becomeLeader(List.of("follower"));
            this.configuration = RaftConfiguration.builder()
                    .nodeId("leader")
                    .clusterMembers(Map.of("leader", "leader:1", "follower", "follower:2"))
                    .dataDirectory(stateDir.toString())
                    .build();
        }

        RaftReplicationManager replicationManager(
                java.util.function.BiFunction<
                                String,
                                com.danieljhkim.kvdb.proto.raft.AppendEntriesRequest,
                                CompletableFuture<AppendEntriesResponse>>
                        appendClient,
                RaftSnapshotStore snapshots) {
            return replicationManager(
                    appendClient,
                    snapshots,
                    (peer, request) -> CompletableFuture.completedFuture(
                            InstallSnapshotResponse.newBuilder().setTerm(5).build()));
        }

        RaftReplicationManager replicationManager(
                java.util.function.BiFunction<
                                String,
                                com.danieljhkim.kvdb.proto.raft.AppendEntriesRequest,
                                CompletableFuture<AppendEntriesResponse>>
                        appendClient,
                RaftSnapshotStore snapshots,
                java.util.function.BiFunction<
                                String,
                                com.danieljhkim.kvdb.proto.raft.InstallSnapshotRequest,
                                CompletableFuture<InstallSnapshotResponse>>
                        snapshotClient) {
            return new RaftReplicationManager(
                    "leader", configuration, state, store, appendClient, snapshotClient, snapshots);
        }

        @Override
        public void close() throws IOException {
            log.close();
        }
    }

    private static RaftLogEntry entry(long index, long term) {
        return new RaftLogEntry(
                index,
                term,
                index,
                new RaftCommand.RegisterNode("node-" + index, "127.0.0.1:" + (9000 + index), "zone-a"));
    }
}
