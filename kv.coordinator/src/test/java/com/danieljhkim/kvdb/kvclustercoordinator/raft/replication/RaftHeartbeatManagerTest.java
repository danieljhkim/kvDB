package com.danieljhkim.kvdb.kvclustercoordinator.raft.replication;

import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftConfiguration;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.FileBasedRaftLog;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftLog;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.state.RaftNodeState;
import com.danieljhkim.kvdb.proto.raft.AppendEntriesResponse;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class RaftHeartbeatManagerTest {

    @TempDir
    Path tempDir;

    private RaftConfiguration config;
    private RaftNodeState state;
    private RaftLog log;
    private ScheduledExecutorService scheduler;
    private RaftHeartbeatManager heartbeatManager;

    private final AtomicInteger heartbeatCount = new AtomicInteger(0);

    @BeforeEach
    void setUp() throws Exception {
        scheduler = Executors.newScheduledThreadPool(2);
        log = new FileBasedRaftLog(tempDir.resolve("raftlog"));
        state = new RaftNodeState("node-1", log);

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

        // Mock RPC client that counts heartbeats
        heartbeatManager = new RaftHeartbeatManager(
                "node-1",
                config,
                state,
                scheduler,
                (peerId, request) -> {
                    heartbeatCount.incrementAndGet();
                    return CompletableFuture.completedFuture(
                            AppendEntriesResponse.newBuilder()
                                    .setTerm(request.getTerm())
                                    .setSuccess(true)
                                    .setMatchIndex(request.getPrevLogIndex())
                                    .setFollowerId(peerId)
                                    .build()
                    );
                }
        );
    }

    @AfterEach
    void tearDown() throws Exception {
        heartbeatManager.stop();
        log.close();
        scheduler.shutdown();
    }

    @Test
    void testStartSendsHeartbeats() throws Exception {
        // Given: node is a leader
        state.becomeLeader(config.getPeers().keySet());

        // When: heartbeat manager starts
        heartbeatManager.start();

        // Wait for a few heartbeat intervals
        Thread.sleep(150); // 3 intervals at 50ms each

        // Then: heartbeats should be sent
        assertTrue(heartbeatCount.get() >= 4, // At least 2 rounds to 2 peers
                "Expected at least 4 heartbeats, got " + heartbeatCount.get());
        assertTrue(heartbeatManager.isActive());
    }

    @Test
    void testStopStopsHeartbeats() throws Exception {
        // Given: heartbeats are active
        state.becomeLeader(config.getPeers().keySet());
        heartbeatManager.start();
        Thread.sleep(60); // Wait for at least one heartbeat

        int countBeforeStop = heartbeatCount.get();
        assertTrue(countBeforeStop > 0, "Should have sent some heartbeats");

        // When: heartbeat manager stops
        heartbeatManager.stop();
        Thread.sleep(100); // Wait to ensure no more heartbeats

        // Then: no more heartbeats should be sent
        int countAfterStop = heartbeatCount.get();
        assertEquals(countBeforeStop, countAfterStop, "No new heartbeats should be sent after stop");
        assertFalse(heartbeatManager.isActive());
    }

    @Test
    void testHeartbeatsStopWhenNotLeader() throws Exception {
        // Given: node is leader and heartbeats are active
        state.becomeLeader(config.getPeers().keySet());
        heartbeatManager.start();
        Thread.sleep(60);

        int countBeforeStepDown = heartbeatCount.get();
        assertTrue(countBeforeStepDown > 0);

        // When: node steps down from leader
        state.becomeFollower(2, "node-2");
        Thread.sleep(100); // Wait for next heartbeat cycle

        // Then: heartbeats should stop
        assertFalse(heartbeatManager.isActive());
    }

    @Test
    void testHeartbeatIncludesCommitIndex() throws Exception {
        // Given: leader with commit index = 5
        state.becomeLeader(config.getPeers().keySet());
        state.updateCommitIndex(5);

        final AtomicInteger receivedCommitIndex = new AtomicInteger(-1);

        heartbeatManager = new RaftHeartbeatManager(
                "node-1",
                config,
                state,
                scheduler,
                (peerId, request) -> {
                    receivedCommitIndex.set((int) request.getLeaderCommit());
                    return CompletableFuture.completedFuture(
                            AppendEntriesResponse.newBuilder()
                                    .setTerm(request.getTerm())
                                    .setSuccess(true)
                                    .setFollowerId(peerId)
                                    .build()
                    );
                }
        );

        // When: heartbeat is sent
        heartbeatManager.start();
        Thread.sleep(60);

        // Then: heartbeat should include commit index
        assertEquals(5, receivedCommitIndex.get());
    }

    @Test
    void testRestartHeartbeats() throws Exception {
        // Given: heartbeats are running
        state.becomeLeader(config.getPeers().keySet());
        heartbeatManager.start();
        Thread.sleep(60);
        heartbeatManager.stop();

        int countAfterFirstRun = heartbeatCount.get();

        // When: heartbeats restart
        heartbeatManager.start();
        Thread.sleep(60);

        // Then: new heartbeats should be sent
        int countAfterRestart = heartbeatCount.get();
        assertTrue(countAfterRestart > countAfterFirstRun,
                "Should send new heartbeats after restart");
    }

    @Test
    void testHeartbeatFailureDoesNotCrash() throws Exception {
        // Given: RPC client that fails
        heartbeatManager = new RaftHeartbeatManager(
                "node-1",
                config,
                state,
                scheduler,
                (peerId, request) -> CompletableFuture.failedFuture(new RuntimeException("Network error"))
        );

        state.becomeLeader(config.getPeers().keySet());

        // When: heartbeats are sent
        heartbeatManager.start();
        Thread.sleep(100);

        // Then: heartbeat manager should still be active (doesn't crash)
        assertTrue(heartbeatManager.isActive());
    }

    @Test
    void testHigherTermInResponseStepsDown() throws Exception {
        // Given: leader at term 1
        state.becomeLeader(config.getPeers().keySet());
        assertEquals(0, state.getCurrentTerm());

        heartbeatManager = new RaftHeartbeatManager(
                "node-1",
                config,
                state,
                scheduler,
                (peerId, request) -> CompletableFuture.completedFuture(
                        AppendEntriesResponse.newBuilder()
                                .setTerm(5) // Higher term
                                .setSuccess(false)
                                .setFollowerId(peerId)
                                .build()
                )
        );

        // When: heartbeat discovers higher term
        heartbeatManager.start();
        Thread.sleep(100);

        // Then: node should step down
        assertTrue(state.isFollower());
        assertEquals(5, state.getCurrentTerm());
        assertFalse(heartbeatManager.isActive());
    }
}

