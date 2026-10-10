package com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftCommand;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftConfiguration;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.replication.RaftHeartbeatManager;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.replication.RaftReplicationManager;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.state.RaftNodeState;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.state.RaftRole;
import com.danieljhkim.kvdb.proto.raft.AppendEntriesResponse;
import com.danieljhkim.kvdb.proto.raft.InstallSnapshotResponse;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
