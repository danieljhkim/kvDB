package com.danieljhkim.kvdb.kvclustercoordinator.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftLog;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftLogEntry;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftPersistentStateStore;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.statemachine.StubRaftStateMachine;
import com.danieljhkim.kvdb.proto.raft.AppendEntriesRequest;
import com.danieljhkim.kvdb.proto.raft.AppendEntriesResponse;
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
            leader.submitCommand(new RaftCommand.InitShards(1, 1)).get(5, TimeUnit.SECONDS);

            assertEquals(1, leader.getState().getCommitIndex());
            assertEquals(1, leader.getState().getLastApplied());
            assertEquals(1, stateMachines.get("n1").getMapVersion());
            assertEquals(1, network.nodes.get("n2").getState().getLog().size());
            assertEquals(0, network.nodes.get("n3").getState().getLog().size());

            sendCommitHeartbeat(leader, network.nodes.get("n2"));
            await(() -> stateMachines.get("n2").getMapVersion() == 1);

            network.block("n1", "n2");
            CompletableFuture<Void> minoritySubmission = leader.submitCommand(
                    new RaftCommand.SetShardReplicas("shard-0", List.of("storage-1", "storage-2")));

            Thread.sleep(100);
            assertFalse(minoritySubmission.isDone(), "A leader isolated from a majority must not acknowledge");
            assertEquals(1, leader.getState().getCommitIndex());
            assertEquals(1, stateMachines.get("n1").getMapVersion());

            network.unblock("n1", "n3");
            minoritySubmission.get(5, TimeUnit.SECONDS);
            assertEquals(2, leader.getState().getCommitIndex());
            assertEquals(2, leader.getState().getLastApplied());

            network.unblock("n1", "n2");
            sendCommitHeartbeat(leader, network.nodes.get("n2"));
            sendCommitHeartbeat(leader, network.nodes.get("n3"));

            await(() -> network.nodes.values().stream()
                    .allMatch(node -> node.getState().getLastApplied() == 2));
            assertTrue(stateMachines.values().stream().allMatch(stateMachine -> stateMachine.getMapVersion() == 2));
            assertTrue(stateMachines.values().stream().allMatch(stateMachine -> stateMachine
                    .getSnapshot()
                    .getShard("shard-0")
                    .replicas()
                    .equals(List.of("storage-1", "storage-2"))));
        } finally {
            network.nodes.values().forEach(RaftNode::stop);
        }
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
