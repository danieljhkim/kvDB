package com.danieljhkim.kvdb.kvclustercoordinator.raft.election;

import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftConfiguration;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.FileBasedRaftLog;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftLog;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftPersistentStateStore;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.state.RaftNodeState;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.state.RaftRole;
import com.danieljhkim.kvdb.proto.raft.RequestVoteResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.*;

class RaftElectionManagerTest {

    @TempDir
    Path tempDir;

    private RaftConfiguration config;
    private RaftNodeState state;
    private RaftPersistentStateStore persistentStore;
    private RaftElectionTimer electionTimer;
    private RaftElectionManager electionManager;
    private RaftLog log;
    private ScheduledExecutorService scheduler;

    // Mock RPC client that auto-grants votes
    private final Map<String, RequestVoteResponse> mockResponses = new java.util.concurrent.ConcurrentHashMap<>();
    private final Map<String, CompletableFuture<RequestVoteResponse>> pendingResponses = new java.util.concurrent.ConcurrentHashMap<>();

    @BeforeEach
    void setUp() throws Exception {
        scheduler = Executors.newScheduledThreadPool(1);
        log = new FileBasedRaftLog(tempDir.resolve("raftlog"));
        state = new RaftNodeState("node-1", log);
        persistentStore = new RaftPersistentStateStore(tempDir.toString());

        config = RaftConfiguration.builder()
                .nodeId("node-1")
                .clusterMembers(Map.of(
                        "node-1", "localhost:5001",
                        "node-2", "localhost:5002",
                        "node-3", "localhost:5003"
                ))
                .electionTimeoutMin(Duration.ofMillis(150))
                .electionTimeoutMax(Duration.ofMillis(300))
                .heartbeatInterval(Duration.ofMillis(50))
                .build();

        electionTimer = new RaftElectionTimer("node-1", config, scheduler, () -> {});

        // Mock RPC client that can control response timing
        electionManager = new RaftElectionManager(
                "node-1",
                config,
                state,
                persistentStore,
                electionTimer,
                (peerId, request) -> {
                    // Check if there's a pending future to complete later
                    CompletableFuture<RequestVoteResponse> pending = pendingResponses.get(peerId);
                    if (pending != null) {
                        return pending;
                    }

                    // Otherwise return immediate response
                    RequestVoteResponse response = mockResponses.getOrDefault(
                            peerId,
                            RequestVoteResponse.newBuilder()
                                    .setTerm(request.getTerm())
                                    .setVoteGranted(true)
                                    .setVoterId(peerId)
                                    .build()
                    );
                    return CompletableFuture.completedFuture(response);
                }
        );
    }

    @AfterEach
    void tearDown() throws Exception {
        log.close();
        scheduler.shutdown();
    }

    @Test
    void testStartElection() {
        // Configure mock to delay responses so we can check intermediate state
        pendingResponses.put("node-2", new CompletableFuture<>());
        pendingResponses.put("node-3", new CompletableFuture<>());

        assertEquals(RaftRole.FOLLOWER, state.getCurrentRole());
        assertEquals(0, state.getCurrentTerm());

        electionManager.startElection();

        assertEquals(RaftRole.CANDIDATE, state.getCurrentRole(), "Should transition to CANDIDATE");
        assertEquals(1, state.getCurrentTerm(), "Should increment term");
        assertEquals("node-1", state.getVotedFor(), "Should vote for self");
    }

    @Test
    void testWinElectionWithMajority() throws InterruptedException {
        // node-2 and node-3 will grant votes
        mockResponses.put("node-2", RequestVoteResponse.newBuilder()
                .setTerm(1)
                .setVoteGranted(true)
                .setVoterId("node-2")
                .build());
        mockResponses.put("node-3", RequestVoteResponse.newBuilder()
                .setTerm(1)
                .setVoteGranted(true)
                .setVoterId("node-3")
                .build());

        electionManager.startElection();

        // Wait a bit for async responses
        Thread.sleep(100);

        assertEquals(RaftRole.LEADER, state.getCurrentRole(), "Should become LEADER with majority");
        assertTrue(electionManager.isElectionWon());
    }

    @Test
    void testLoseElectionWithoutMajority() throws InterruptedException {
        // Only one peer grants vote (not enough for majority of 3)
        mockResponses.put("node-2", RequestVoteResponse.newBuilder()
                .setTerm(1)
                .setVoteGranted(true)
                .setVoterId("node-2")
                .build());
        mockResponses.put("node-3", RequestVoteResponse.newBuilder()
                .setTerm(1)
                .setVoteGranted(false)
                .setVoterId("node-3")
                .build());

        electionManager.startElection();

        // Wait for async responses
        Thread.sleep(100);

        assertEquals(RaftRole.CANDIDATE, state.getCurrentRole(), "Should remain CANDIDATE without majority");
        assertFalse(electionManager.isElectionWon());
        assertEquals(2, electionManager.getVotesReceived(), "Should have 2 votes (self + node-2)");
    }

    @Test
    void testStepDownOnHigherTerm() throws InterruptedException {
        // node-2 responds with higher term
        mockResponses.put("node-2", RequestVoteResponse.newBuilder()
                .setTerm(5)
                .setVoteGranted(false)
                .setVoterId("node-2")
                .build());
        mockResponses.put("node-3", RequestVoteResponse.newBuilder()
                .setTerm(1)
                .setVoteGranted(true)
                .setVoterId("node-3")
                .build());

        electionManager.startElection();

        // Wait for async responses
        Thread.sleep(100);

        assertEquals(RaftRole.FOLLOWER, state.getCurrentRole(), "Should step down on discovering higher term");
        assertEquals(5, state.getCurrentTerm(), "Should update to higher term");
        assertNull(state.getVotedFor(), "Should clear vote after stepping down");
    }

    @Test
    void testSingleNodeClusterAutoWin() {
        // Create single-node cluster
        RaftConfiguration singleNodeConfig = RaftConfiguration.builder()
                .nodeId("node-1")
                .clusterMembers(Map.of("node-1", "localhost:5001"))
                .electionTimeoutMin(Duration.ofMillis(150))
                .electionTimeoutMax(Duration.ofMillis(300))
                .heartbeatInterval(Duration.ofMillis(50))
                .build();

        RaftElectionManager singleNodeManager = new RaftElectionManager(
                "node-1",
                singleNodeConfig,
                state,
                persistentStore,
                electionTimer,
                (peerId, request) -> CompletableFuture.completedFuture(null)
        );

        singleNodeManager.startElection();

        assertEquals(RaftRole.LEADER, state.getCurrentRole(), "Single node should automatically become LEADER");
        assertTrue(singleNodeManager.isElectionWon());
    }

    @Test
    void testIgnoreElectionWhenAlreadyLeader() {
        // Manually set as leader
        state.becomeLeader(config.getPeers().keySet());

        long termBefore = state.getCurrentTerm();

        electionManager.startElection();

        assertEquals(termBefore, state.getCurrentTerm(), "Should not change term when already leader");
        assertEquals(RaftRole.LEADER, state.getCurrentRole(), "Should remain LEADER");
    }
}

