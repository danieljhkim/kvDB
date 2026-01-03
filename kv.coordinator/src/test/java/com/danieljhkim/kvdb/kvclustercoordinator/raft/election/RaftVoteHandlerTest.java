package com.danieljhkim.kvdb.kvclustercoordinator.raft.election;

import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftCommand;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftConfiguration;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.FileBasedRaftLog;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftLog;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftLogEntry;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftPersistentStateStore;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.state.RaftNodeState;
import com.danieljhkim.kvdb.proto.raft.RequestVoteRequest;
import com.danieljhkim.kvdb.proto.raft.RequestVoteResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.*;

class RaftVoteHandlerTest {

    @TempDir
    Path tempDir;

    private RaftNodeState state;
    private RaftPersistentStateStore persistentStore;
    private RaftElectionTimer electionTimer;
    private RaftVoteHandler voteHandler;
    private RaftLog log;
    private ScheduledExecutorService scheduler;

    @BeforeEach
    void setUp() throws IOException {
        scheduler = Executors.newScheduledThreadPool(1);
        log = new FileBasedRaftLog(tempDir.resolve("raftlog"));
        state = new RaftNodeState("node-1", log);
        persistentStore = new RaftPersistentStateStore(tempDir.toString());

        RaftConfiguration config = RaftConfiguration.builder()
                .nodeId("node-1")
                .clusterMembers(Map.of("node-1", "localhost:5001", "node-2", "localhost:5002"))
                .electionTimeoutMin(Duration.ofMillis(150))
                .electionTimeoutMax(Duration.ofMillis(300))
                .heartbeatInterval(Duration.ofMillis(50))
                .build();

        electionTimer = new RaftElectionTimer("node-1", config, scheduler, () -> {});
        voteHandler = new RaftVoteHandler("node-1", state, persistentStore, electionTimer);
    }

    @AfterEach
    void tearDown() throws Exception {
        log.close();
        scheduler.shutdown();
    }

    @Test
    void testGrantVoteWhenHaventVoted() {
        RequestVoteRequest request = RequestVoteRequest.newBuilder()
                .setTerm(1)
                .setCandidateId("node-2")
                .setLastLogIndex(0)
                .setLastLogTerm(0)
                .build();

        RequestVoteResponse response = voteHandler.handleRequestVote(request);

        assertTrue(response.getVoteGranted(), "Should grant vote when haven't voted yet");
        assertEquals(1, response.getTerm());
        assertEquals("node-1", response.getVoterId());
        assertEquals("node-2", state.getVotedFor(), "Should record vote for candidate");
    }

    @Test
    void testRejectVoteWhenAlreadyVoted() {
        // First vote for node-2
        state.updateTerm(1);
        state.setVotedFor("node-2");

        // node-3 requests vote
        RequestVoteRequest request = RequestVoteRequest.newBuilder()
                .setTerm(1)
                .setCandidateId("node-3")
                .setLastLogIndex(0)
                .setLastLogTerm(0)
                .build();

        RequestVoteResponse response = voteHandler.handleRequestVote(request);

        assertFalse(response.getVoteGranted(), "Should reject vote when already voted for someone else");
        assertEquals("node-2", state.getVotedFor(), "Vote should remain for original candidate");
    }

    @Test
    void testRejectVoteForStaleTerm() {
        state.updateTerm(2);

        RequestVoteRequest request = RequestVoteRequest.newBuilder()
                .setTerm(1)
                .setCandidateId("node-2")
                .setLastLogIndex(0)
                .setLastLogTerm(0)
                .build();

        RequestVoteResponse response = voteHandler.handleRequestVote(request);

        assertFalse(response.getVoteGranted(), "Should reject vote for stale term");
        assertEquals(2, response.getTerm(), "Should return current term");
    }

    @Test
    void testUpdateTermWhenDiscoveringHigherTerm() {
        state.updateTerm(1);

        RequestVoteRequest request = RequestVoteRequest.newBuilder()
                .setTerm(2)
                .setCandidateId("node-2")
                .setLastLogIndex(0)
                .setLastLogTerm(0)
                .build();

        RequestVoteResponse response = voteHandler.handleRequestVote(request);

        assertTrue(response.getVoteGranted(), "Should grant vote after updating term");
        assertEquals(2, state.getCurrentTerm(), "Should update to higher term");
        assertEquals("node-2", state.getVotedFor());
    }

    @Test
    void testRejectVoteWhenCandidateLogNotUpToDate() throws IOException {
        // Our log has entries
        RaftCommand testCommand = new RaftCommand.InitShards(8, 3);
        log.append(new RaftLogEntry(1, 1, System.currentTimeMillis(), testCommand));

        // Candidate has empty log
        RequestVoteRequest request = RequestVoteRequest.newBuilder()
                .setTerm(1)
                .setCandidateId("node-2")
                .setLastLogIndex(0)
                .setLastLogTerm(0)
                .build();

        RequestVoteResponse response = voteHandler.handleRequestVote(request);

        assertFalse(response.getVoteGranted(), "Should reject vote when candidate log is not up-to-date");
    }

    @Test
    void testGrantVoteWhenCandidateLogIsUpToDate() throws IOException {
        // Our log
        RaftCommand testCommand = new RaftCommand.InitShards(8, 3);
        log.append(new RaftLogEntry(1, 1, System.currentTimeMillis(), testCommand));

        // Candidate has same or better log
        RequestVoteRequest request = RequestVoteRequest.newBuilder()
                .setTerm(2)
                .setCandidateId("node-2")
                .setLastLogIndex(1)
                .setLastLogTerm(1)
                .build();

        RequestVoteResponse response = voteHandler.handleRequestVote(request);

        assertTrue(response.getVoteGranted(), "Should grant vote when candidate log is up-to-date");
    }

    @Test
    void testGrantVoteWhenCandidateLogHasHigherTerm() throws IOException {
        // Our log
        RaftCommand testCommand = new RaftCommand.InitShards(8, 3);
        log.append(new RaftLogEntry(1, 1, System.currentTimeMillis(), testCommand));

        // Candidate has log with higher term
        RequestVoteRequest request = RequestVoteRequest.newBuilder()
                .setTerm(2)
                .setCandidateId("node-2")
                .setLastLogIndex(1)
                .setLastLogTerm(2)
                .build();

        RequestVoteResponse response = voteHandler.handleRequestVote(request);

        assertTrue(response.getVoteGranted(), "Should grant vote when candidate has higher term in log");
    }
}

