package com.danieljhkim.kvdb.kvclustercoordinator.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.danieljhkim.kvdb.kvclustercoordinator.converter.RaftCommandConverter;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.FileBasedRaftLog;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftLog;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftLogEntry;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftPersistentStateStore;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftSnapshotStore;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.replication.RaftReplicationManager;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.state.RaftNodeState;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.statemachine.StubRaftStateMachine;
import com.danieljhkim.kvdb.proto.raft.AppendEntriesRequest;
import com.danieljhkim.kvdb.proto.raft.AppendEntriesResponse;
import com.danieljhkim.kvdb.proto.raft.InstallSnapshotRequest;
import com.danieljhkim.kvdb.proto.raft.InstallSnapshotResponse;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RaftNodeCommitIntegrationTest {

    @TempDir
    Path tempDir;

    @Test
    void majorityCommitMinorityStallAndRecoveryConvergence() throws Exception {
        ControlledNetwork network = new ControlledNetwork();
        Map<String, String> members = Map.of("n1", "n1:9000", "n2", "n2:9000", "n3", "n3:9000");
        Map<String, StubRaftStateMachine> stateMachines = new ConcurrentHashMap<>();

        for (String nodeId : members.keySet()) {
            StubRaftStateMachine stateMachine = new StubRaftStateMachine();
            stateMachines.put(nodeId, stateMachine);
            RaftConfiguration config = configuration(nodeId, members, tempDir.resolve(nodeId));
            RaftNode node = new RaftNode(
                    nodeId,
                    config,
                    new InMemoryRaftLog(),
                    new RaftPersistentStateStore(tempDir.resolve(nodeId).toString()),
                    stateMachine,
                    (peer, request) -> CompletableFuture.failedFuture(new AssertionError("unexpected vote RPC")),
                    (peer, request) -> network.append(nodeId, peer, request));
            network.nodes.put(nodeId, node);
        }

        RaftNode leader = network.nodes.get("n1");
        network.nodes.values().forEach(RaftNode::start);
        network.nodes.get("n2").getState().updateTerm(1);
        network.nodes.get("n3").getState().updateTerm(1);
        leader.getState().becomeCandidate();
        leader.getState()
                .becomeLeader(members.keySet().stream()
                        .filter(peer -> !peer.equals("n1"))
                        .toList());
        await(() -> network.appendCallCount.get() >= 4);

        try {
            network.block("n1", "n3");
            // Replica changes are validated against membership, so the storage nodes are registered first.
            leader.submitCommand(new RaftCommand.RegisterNode("storage-1", "storage-1:9000", "zone-a"))
                    .get(5, TimeUnit.SECONDS);
            leader.submitCommand(new RaftCommand.RegisterNode("storage-2", "storage-2:9000", "zone-b"))
                    .get(5, TimeUnit.SECONDS);
            leader.submitCommand(new RaftCommand.InitShards(1, 1)).get(5, TimeUnit.SECONDS);

            assertEquals(3, leader.getState().getCommitIndex());
            assertEquals(3, leader.getState().getLastApplied());
            // Two registrations and shard initialization each publish one map version.
            assertEquals(3, stateMachines.get("n1").getMapVersion());
            assertEquals(3, network.nodes.get("n2").getState().getLog().size());
            assertEquals(0, network.nodes.get("n3").getState().getLog().size());

            sendCommitHeartbeat(leader, network.nodes.get("n2"));
            await(() -> stateMachines.get("n2").getMapVersion() == 3);

            network.block("n1", "n2");
            CompletableFuture<Void> minoritySubmission = leader.submitCommand(
                    new RaftCommand.SetShardReplicas("shard-0", List.of("storage-1", "storage-2")));

            Thread.sleep(100);
            assertFalse(minoritySubmission.isDone(), "A leader isolated from a majority must not acknowledge");
            assertEquals(3, leader.getState().getCommitIndex());
            assertEquals(3, stateMachines.get("n1").getMapVersion());

            network.unblock("n1", "n3");
            minoritySubmission.get(5, TimeUnit.SECONDS);
            assertEquals(4, leader.getState().getCommitIndex());
            assertEquals(4, leader.getState().getLastApplied());

            network.unblock("n1", "n2");
            sendCommitHeartbeat(leader, network.nodes.get("n2"));
            sendCommitHeartbeat(leader, network.nodes.get("n3"));

            await(() -> network.nodes.values().stream()
                    .allMatch(node -> node.getState().getLastApplied() == 4));
            assertTrue(stateMachines.values().stream().allMatch(stateMachine -> stateMachine.getMapVersion() == 4));
            assertTrue(stateMachines.values().stream().allMatch(stateMachine -> stateMachine
                    .getSnapshot()
                    .getShard("shard-0")
                    .replicas()
                    .equals(List.of("storage-1", "storage-2"))));
        } finally {
            network.nodes.values().forEach(RaftNode::stop);
        }
    }

    @Test
    void committedMetadataRecoversAfterEveryCoordinatorRestarts() throws Exception {
        Map<String, String> members = members();
        PersistentCluster original = createPersistentCluster(members);
        startAndElect(original, "n1");

        try {
            RaftNode leader = original.network.nodes.get("n1");
            leader.submitCommand(new RaftCommand.RegisterNode("storage-1", "storage-1:9000", "zone-a"))
                    .get(5, TimeUnit.SECONDS);
            leader.submitCommand(new RaftCommand.RegisterNode("storage-2", "storage-2:9000", "zone-b"))
                    .get(5, TimeUnit.SECONDS);
            leader.submitCommand(new RaftCommand.InitShards(8, 2)).get(5, TimeUnit.SECONDS);
            sendCommitHeartbeat(leader, original.network.nodes.get("n2"));
            sendCommitHeartbeat(leader, original.network.nodes.get("n3"));
            await(() -> original.stateMachines.values().stream().allMatch(stateMachine -> {
                var snapshot = stateMachine.getSnapshot();
                return snapshot.getMapVersion() == 3
                        && snapshot.getNodes().keySet().equals(Set.of("storage-1", "storage-2"))
                        && snapshot.getShards().size() == 8;
            }));
        } finally {
            original.stop();
        }

        PersistentCluster restarted = createPersistentCluster(members);
        restarted.network.nodes.values().forEach(RaftNode::start);
        assertTrue(
                restarted.stateMachines.values().stream().allMatch(stateMachine -> stateMachine.getMapVersion() == 0));

        try {
            elect(restarted, "n2");
            await(() -> restarted.stateMachines.values().stream().allMatch(stateMachine -> {
                var snapshot = stateMachine.getSnapshot();
                return snapshot.getMapVersion() == 3
                        && snapshot.getNodes().keySet().equals(Set.of("storage-1", "storage-2"))
                        && snapshot.getShards().size() == 8;
            }));
        } finally {
            restarted.stop();
        }
    }

    @Test
    void recoveryDoesNotApplyUncommittedSuffix() throws Exception {
        Map<String, String> members = members();
        RaftCommand committed = new RaftCommand.RegisterNode("committed", "committed:9000", "zone-a");
        RaftCommand uncommitted = new RaftCommand.RegisterNode("uncommitted", "uncommitted:9000", "zone-b");

        for (String nodeId : members.keySet()) {
            Path dataDirectory = tempDir.resolve(nodeId);
            try (FileBasedRaftLog log = new FileBasedRaftLog(dataDirectory.resolve("log"))) {
                log.append(new RaftLogEntry(1, 1, 1, committed));
                if (nodeId.equals("n3")) {
                    log.append(new RaftLogEntry(2, 2, 2, uncommitted));
                }
            }
            new RaftPersistentStateStore(dataDirectory.toString()).save(2, null);
        }

        PersistentCluster restarted = createPersistentCluster(members);
        restarted.network.nodes.values().forEach(RaftNode::start);
        assertTrue(
                restarted.stateMachines.values().stream().allMatch(stateMachine -> stateMachine.getMapVersion() == 0));

        try {
            elect(restarted, "n1");
            await(() -> restarted.stateMachines.values().stream().allMatch(stateMachine -> {
                var snapshot = stateMachine.getSnapshot();
                return snapshot.getMapVersion() == 1
                        && snapshot.getNode("committed") != null
                        && snapshot.getNode("uncommitted") == null;
            }));
            assertTrue(
                    restarted
                                    .network
                                    .nodes
                                    .get("n3")
                                    .getState()
                                    .getLog()
                                    .getEntry(2)
                                    .orElseThrow()
                                    .command()
                            instanceof RaftCommand.NoOp);
        } finally {
            restarted.stop();
        }
    }

    @Test
    void emptyAppendEntriesDoesNotCommitUnverifiedDivergentSuffix() throws Exception {
        RaftCommand shared = new RaftCommand.RegisterNode("shared", "shared:9000", "zone-a");
        RaftCommand divergent = new RaftCommand.RegisterNode("divergent", "divergent:9000", "zone-b");
        StubRaftStateMachine stateMachine = new StubRaftStateMachine();
        InMemoryRaftLog followerLog = new InMemoryRaftLog();
        followerLog.append(new RaftLogEntry(1, 1, 1, shared));
        followerLog.append(new RaftLogEntry(2, 2, 2, divergent));
        RaftNode follower = divergentFollower(followerLog, stateMachine);
        follower.start();

        try {
            // The heartbeat verifies only index 1, so the divergent index 2 must stay uncommitted.
            AppendEntriesResponse response = follower.handleAppendEntries(AppendEntriesRequest.newBuilder()
                    .setTerm(3)
                    .setLeaderId("n1")
                    .setPrevLogIndex(1)
                    .setPrevLogTerm(1)
                    .setLeaderCommit(2)
                    .build());

            assertTrue(response.getSuccess());
            assertEquals(1, response.getMatchIndex());
            assertEquals(1, follower.getState().getCommitIndex());
            assertEquals(1, follower.getState().getLastApplied());
            assertEquals(1, stateMachine.getMapVersion());
            assertNotNull(stateMachine.getSnapshot().getNode("shared"));
            assertNull(stateMachine.getSnapshot().getNode("divergent"));
        } finally {
            follower.stop();
        }
    }

    @Test
    void partialAppendEntriesReplacesDivergentSuffixAndCommitsOnlyVerifiedEntries() throws Exception {
        RaftCommand shared = new RaftCommand.RegisterNode("shared", "shared:9000", "zone-a");
        RaftCommand divergent = new RaftCommand.RegisterNode("divergent", "divergent:9000", "zone-b");
        RaftCommand leaderOnly = new RaftCommand.RegisterNode("leader-only", "leader-only:9000", "zone-b");
        StubRaftStateMachine stateMachine = new StubRaftStateMachine();
        InMemoryRaftLog followerLog = new InMemoryRaftLog();
        followerLog.append(new RaftLogEntry(1, 1, 1, shared));
        followerLog.append(new RaftLogEntry(2, 2, 2, divergent));
        RaftNode follower = divergentFollower(followerLog, stateMachine);
        follower.start();

        try {
            // The request verifies indexes 1 and 2, so the leader's index 2 replaces the divergent entry
            // and leaderCommit=3 is capped at the verified index rather than the follower's old last index.
            AppendEntriesResponse response = follower.handleAppendEntries(AppendEntriesRequest.newBuilder()
                    .setTerm(3)
                    .setLeaderId("n1")
                    .setPrevLogIndex(1)
                    .setPrevLogTerm(1)
                    .addEntries(com.danieljhkim.kvdb.proto.raft.RaftLogEntry.newBuilder()
                            .setIndex(2)
                            .setTerm(3)
                            .setCommand(RaftCommandConverter.toProto(leaderOnly))
                            .build())
                    .setLeaderCommit(3)
                    .build());

            assertTrue(response.getSuccess());
            assertEquals(2, response.getMatchIndex());
            assertEquals(2, follower.getState().getCommitIndex());
            assertEquals(2, follower.getState().getLastApplied());
            assertEquals(2, stateMachine.getMapVersion());
            assertNotNull(stateMachine.getSnapshot().getNode("shared"));
            assertNotNull(stateMachine.getSnapshot().getNode("leader-only"));
            assertNull(stateMachine.getSnapshot().getNode("divergent"));
        } finally {
            follower.stop();
        }
    }

    @Test
    void delayedAppendSuccessCannotAcknowledgeReplacementAcrossElections() throws Exception {
        verifySupersededAppend(true, false);
    }

    @Test
    void delayedAppendFailureCannotBacktrackOrRetryInNewTerm() throws Exception {
        verifySupersededAppend(false, false);
    }

    @Test
    void clearingReplicationInvalidatesCallbacksEvenInSameTerm() throws Exception {
        verifySupersededAppend(true, true);
    }

    private void verifySupersededAppend(boolean success, boolean clearOnly) throws Exception {
        InMemoryRaftLog log = new InMemoryRaftLog();
        RaftNodeState state = new RaftNodeState("n1", log);
        state.becomeCandidate();
        state.becomeLeader(List.of("n2", "n3"));
        RaftCommand oldCommand = new RaftCommand.RegisterNode("old", "old:9000", "zone-a");
        RaftCommand replacement = new RaftCommand.RegisterNode("replacement", "replacement:9000", "zone-b");
        log.append(new RaftLogEntry(1, 1, 1, oldCommand));
        List<PendingAppend> requests = new ArrayList<>();
        RaftReplicationManager manager = new RaftReplicationManager(
                "n1",
                configuration("n1", members(), tempDir.resolve("n1")),
                state,
                new RaftPersistentStateStore(tempDir.resolve("n1").toString()),
                (peer, request) -> {
                    CompletableFuture<AppendEntriesResponse> response = new CompletableFuture<>();
                    requests.add(new PendingAppend(peer, request, response));
                    return response;
                });
        CompletableFuture<Void> oldQuorum = manager.replicateToAll();
        PendingAppend delayed = requests.stream()
                .filter(request -> request.destination.equals("n2"))
                .findFirst()
                .orElseThrow();
        assertEquals(1, delayed.request.getTerm());
        assertEquals(
                RaftCommandConverter.toProto(oldCommand),
                delayed.request.getEntries(0).getCommand());

        if (!clearOnly) {
            state.becomeFollower(2, "n3");
        }
        manager.clear();
        if (!clearOnly) {
            log.truncateAfter(0);
            log.append(new RaftLogEntry(1, 3, 2, replacement));
            state.becomeCandidate();
            state.becomeLeader(List.of("n2", "n3"));
            state.setNextIndex("n2", 1);
            state.setNextIndex("n3", 1);
        }
        CompletableFuture<Void> currentQuorum = manager.replicateToAll();
        CompletableFuture<Void> currentPeer = manager.replicateToPeer("n2");
        PendingAppend current = requests.stream()
                .filter(request -> request.destination.equals("n2") && request != delayed)
                .findFirst()
                .orElseThrow();
        assertEquals(clearOnly ? 1 : 3, current.request.getTerm());
        assertEquals(
                RaftCommandConverter.toProto(clearOnly ? oldCommand : replacement),
                current.request.getEntries(0).getCommand());
        delayed.future.complete(AppendEntriesResponse.newBuilder()
                .setTerm(1)
                .setSuccess(success)
                .setMatchIndex(1)
                .setConflictIndex(7)
                .build());

        assertEquals(0L, state.getMatchIndex("n2"));
        assertEquals(1L, state.getNextIndex("n2"));
        assertEquals(0, state.getCommitIndex());
        assertTrue(oldQuorum.isCompletedExceptionally(), "Superseded requests cannot satisfy their quorum");
        assertFalse(currentQuorum.isDone());
        assertSame(currentPeer, manager.replicateToPeer("n2"), "Old cleanup must preserve the new in-flight RPC");
        assertEquals(4, requests.size(), "A stale failure must not issue a retry");

        // Deliver the actual current-term entry to a real follower before releasing its acknowledgement.
        InMemoryRaftLog followerLog = new InMemoryRaftLog();
        RaftNode follower = divergentFollower(followerLog, new StubRaftStateMachine());
        follower.start();
        try {
            current.future.complete(follower.handleAppendEntries(current.request));
            currentQuorum.get(5, TimeUnit.SECONDS);
            assertEquals(1, state.getCommitIndex());
            assertEquals(1L, state.getMatchIndex("n2"));
            assertEquals(
                    clearOnly ? oldCommand : replacement,
                    followerLog.getEntry(1).orElseThrow().command());
        } finally {
            follower.stop();
        }
    }

    @Test
    void delayedCompletedSnapshotCannotAcknowledgeNewLeadership() throws Exception {
        verifySupersededSnapshot(true, 2);
    }

    @Test
    void delayedPartialSnapshotCannotContinueInNewLeadership() throws Exception {
        verifySupersededSnapshot(true, 1);
    }

    @Test
    void delayedRejectedSnapshotCannotRetryInNewLeadership() throws Exception {
        verifySupersededSnapshot(false, 0);
    }

    private void verifySupersededSnapshot(boolean success, long nextOffset) throws Exception {
        Path directory = tempDir.resolve("n1");
        try (FileBasedRaftLog log = new FileBasedRaftLog(directory.resolve("log"))) {
            log.append(new RaftLogEntry(1, 1, 1, new RaftCommand.NoOp()));
            RaftSnapshotStore snapshots = new RaftSnapshotStore(directory);
            snapshots.save(1, 1, new byte[] {1, 2});
            log.compactThrough(1, 1);
            RaftNodeState state = new RaftNodeState("n1", log);
            state.becomeCandidate();
            state.becomeLeader(List.of("n2", "n3"));
            state.setNextIndex("n2", 1);
            List<PendingSnapshot> requests = new ArrayList<>();
            AtomicInteger appends = new AtomicInteger();
            RaftReplicationManager manager = new RaftReplicationManager(
                    "n1",
                    configuration("n1", members(), directory),
                    state,
                    new RaftPersistentStateStore(directory.toString()),
                    (peer, request) -> {
                        appends.incrementAndGet();
                        return CompletableFuture.completedFuture(AppendEntriesResponse.newBuilder()
                                .setTerm(request.getTerm())
                                .setSuccess(true)
                                .setMatchIndex(1)
                                .build());
                    },
                    (peer, request) -> {
                        CompletableFuture<InstallSnapshotResponse> response = new CompletableFuture<>();
                        requests.add(new PendingSnapshot(request, response));
                        return response;
                    },
                    snapshots);
            CompletableFuture<Void> oldPeer = manager.replicateToPeer("n2");
            state.becomeFollower(2, "n3");
            manager.clear();
            state.becomeCandidate();
            state.becomeLeader(List.of("n2", "n3"));
            state.setNextIndex("n2", 1);
            CompletableFuture<Void> currentPeer = manager.replicateToPeer("n2");
            assertEquals(1, requests.get(0).request.getTerm());
            assertEquals(3, requests.get(1).request.getTerm());
            requests.get(0)
                    .future
                    .complete(InstallSnapshotResponse.newBuilder()
                            .setTerm(1)
                            .setSuccess(success)
                            .setNextOffset(nextOffset)
                            .build());

            assertTrue(oldPeer.isCompletedExceptionally());
            assertEquals(0L, state.getMatchIndex("n2"));
            assertEquals(1L, state.getNextIndex("n2"));
            assertEquals(0, state.getCommitIndex());
            assertEquals(2, requests.size(), "Stale chunks must not start a continuation or retry");
            assertEquals(0, appends.get(), "Stale snapshot completion must not start AppendEntries");
            assertSame(currentPeer, manager.replicateToPeer("n2"));
            requests.get(1)
                    .future
                    .complete(InstallSnapshotResponse.newBuilder()
                            .setTerm(3)
                            .setSuccess(true)
                            .setNextOffset(2)
                            .build());
            currentPeer.get(5, TimeUnit.SECONDS);
            assertEquals(1L, state.getMatchIndex("n2"));
            assertEquals(1, appends.get());
        }
    }

    private record PendingSnapshot(InstallSnapshotRequest request, CompletableFuture<InstallSnapshotResponse> future) {}

    private RaftNode divergentFollower(InMemoryRaftLog followerLog, StubRaftStateMachine stateMachine)
            throws IOException {
        return new RaftNode(
                "n2",
                configuration("n2", members(), tempDir.resolve("n2")),
                followerLog,
                new RaftPersistentStateStore(tempDir.resolve("n2").toString()),
                stateMachine,
                (peer, request) -> CompletableFuture.failedFuture(new AssertionError("unexpected vote RPC")),
                (peer, request) -> CompletableFuture.failedFuture(new AssertionError("unexpected append RPC")));
    }

    private PersistentCluster createPersistentCluster(Map<String, String> members) throws IOException {
        ControlledNetwork network = new ControlledNetwork();
        Map<String, StubRaftStateMachine> stateMachines = new ConcurrentHashMap<>();
        for (String nodeId : members.keySet()) {
            Path dataDirectory = tempDir.resolve(nodeId);
            StubRaftStateMachine stateMachine = new StubRaftStateMachine();
            stateMachines.put(nodeId, stateMachine);
            RaftNode node = new RaftNode(
                    nodeId,
                    configuration(nodeId, members, dataDirectory),
                    new FileBasedRaftLog(dataDirectory.resolve("log")),
                    new RaftPersistentStateStore(dataDirectory.toString()),
                    stateMachine,
                    (peer, request) -> CompletableFuture.failedFuture(new AssertionError("unexpected vote RPC")),
                    (peer, request) -> network.append(nodeId, peer, request));
            network.nodes.put(nodeId, node);
        }
        return new PersistentCluster(network, stateMachines);
    }

    private static Map<String, String> members() {
        return Map.of("n1", "n1:9000", "n2", "n2:9000", "n3", "n3:9000");
    }

    private static void startAndElect(PersistentCluster cluster, String leaderId) {
        cluster.network.nodes.values().forEach(RaftNode::start);
        elect(cluster, leaderId);
    }

    private static void elect(PersistentCluster cluster, String leaderId) {
        RaftNode leader = cluster.network.nodes.get(leaderId);
        leader.getState().becomeCandidate();
        leader.getState()
                .becomeLeader(cluster.network.nodes.keySet().stream()
                        .filter(nodeId -> !nodeId.equals(leaderId))
                        .toList());
    }

    private static RaftConfiguration configuration(String nodeId, Map<String, String> members, Path dataDirectory) {
        return RaftConfiguration.builder()
                .nodeId(nodeId)
                .clusterMembers(members)
                .heartbeatInterval(Duration.ofHours(1))
                .electionTimeoutMin(Duration.ofHours(2))
                .electionTimeoutMax(Duration.ofHours(3))
                .dataDirectory(dataDirectory.toString())
                .build();
    }

    private static void sendCommitHeartbeat(RaftNode leader, RaftNode follower) throws IOException {
        long lastIndex = leader.getState().getLog().size();
        long lastTerm =
                leader.getState().getLog().getEntry(lastIndex).orElseThrow().term();
        AppendEntriesResponse response = follower.handleAppendEntries(AppendEntriesRequest.newBuilder()
                .setTerm(leader.getCurrentTerm())
                .setLeaderId(leader.getLeaderId())
                .setPrevLogIndex(lastIndex)
                .setPrevLogTerm(lastTerm)
                .setLeaderCommit(leader.getState().getCommitIndex())
                .build());
        assertTrue(response.getSuccess());
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertTrue(condition.getAsBoolean(), "Condition was not satisfied before timeout");
    }

    private record Link(String from, String to) {}

    private record PendingAppend(
            String destination, AppendEntriesRequest request, CompletableFuture<AppendEntriesResponse> future) {}

    private record PersistentCluster(ControlledNetwork network, Map<String, StubRaftStateMachine> stateMachines) {

        void stop() {
            network.nodes.values().forEach(RaftNode::stop);
        }
    }

    private static final class ControlledNetwork {

        private final Map<String, RaftNode> nodes = new ConcurrentHashMap<>();
        private final Set<Link> blocked = ConcurrentHashMap.newKeySet();
        private final Map<Link, List<PendingAppend>> pending = new ConcurrentHashMap<>();
        private final AtomicInteger appendCallCount = new AtomicInteger();

        CompletableFuture<AppendEntriesResponse> append(
                String source, String destination, AppendEntriesRequest request) {
            appendCallCount.incrementAndGet();
            Link link = new Link(source, destination);
            if (!blocked.contains(link)) {
                return CompletableFuture.completedFuture(nodes.get(destination).handleAppendEntries(request));
            }

            CompletableFuture<AppendEntriesResponse> future = new CompletableFuture<>();
            pending.computeIfAbsent(link, ignored -> Collections.synchronizedList(new ArrayList<>()))
                    .add(new PendingAppend(destination, request, future));
            return future;
        }

        void block(String source, String destination) {
            blocked.add(new Link(source, destination));
        }

        void unblock(String source, String destination) {
            Link link = new Link(source, destination);
            blocked.remove(link);
            List<PendingAppend> pendingAppends = pending.remove(link);
            if (pendingAppends == null) {
                return;
            }

            List.copyOf(pendingAppends)
                    .forEach(append ->
                            append.future.complete(nodes.get(append.destination).handleAppendEntries(append.request)));
        }
    }

    private static final class InMemoryRaftLog implements RaftLog {

        private final List<RaftLogEntry> entries = new ArrayList<>();

        @Override
        public synchronized void append(RaftLogEntry entry) {
            entries.add(entry);
        }

        @Override
        public synchronized List<RaftLogEntry> getEntriesSince(long fromIndex) {
            int start = (int) Math.max(0, fromIndex - 1);
            return List.copyOf(entries.subList(Math.min(start, entries.size()), entries.size()));
        }

        @Override
        public synchronized Optional<RaftLogEntry> getEntry(long index) {
            if (index < 1 || index > entries.size()) {
                return Optional.empty();
            }
            return Optional.of(entries.get((int) index - 1));
        }

        @Override
        public synchronized Optional<RaftLogEntry> getLastEntry() {
            return entries.isEmpty() ? Optional.empty() : Optional.of(entries.getLast());
        }

        @Override
        public synchronized long size() {
            return entries.size();
        }

        @Override
        public synchronized void truncateAfter(long index) {
            int newSize = (int) Math.max(0, Math.min(index, entries.size()));
            entries.subList(newSize, entries.size()).clear();
        }

        @Override
        public void close() {}
    }
}
