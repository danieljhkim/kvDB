package com.danieljhkim.kvdb.kvclustercoordinator.raft.replication;

import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftCommand;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.election.RaftElectionTimer;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.FileBasedRaftLog;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftLog;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftLogEntry;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftPersistentStateStore;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.state.RaftNodeState;
import com.danieljhkim.kvdb.proto.raft.AppendEntriesRequest;
import com.danieljhkim.kvdb.proto.raft.AppendEntriesResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class RaftAppendEntriesHandlerTest {

    @TempDir
    Path tempDir;

    private RaftNodeState state;
    private RaftLog log;
    private RaftPersistentStateStore persistentStore;
    private RaftElectionTimer electionTimer;
    private RaftAppendEntriesHandler handler;

    private final AtomicBoolean electionTimerReset = new AtomicBoolean(false);

    @BeforeEach
    void setUp() throws Exception {
        log = new FileBasedRaftLog(tempDir.resolve("raftlog"));
        state = new RaftNodeState("node-1", log);
        persistentStore = new RaftPersistentStateStore(tempDir.resolve("state").toString());

        // Mock election timer
        electionTimer = new RaftElectionTimer(
                "node-1",
                null,
                null,
                () -> {}
        ) {
            @Override
            public void reset() {
                electionTimerReset.set(true);
            }
        };

        handler = new RaftAppendEntriesHandler("node-1", state, persistentStore, electionTimer);
    }

    @AfterEach
    void tearDown() throws Exception {
        log.close();
    }

    @Test
    void testRejectStaleTerm() {
        // Given: node at term 5
        state.updateTerm(5);

        // When: receive AppendEntries with term 3
        AppendEntriesRequest request = AppendEntriesRequest.newBuilder()
                .setTerm(3)
                .setLeaderId("node-2")
                .setPrevLogIndex(0)
                .setPrevLogTerm(0)
                .setLeaderCommit(0)
                .build();

        AppendEntriesResponse response = handler.handleAppendEntries(request);

        // Then: reject with current term
        assertFalse(response.getSuccess());
        assertEquals(5, response.getTerm());
        assertEquals(5, state.getCurrentTerm());
    }

    @Test
    void testUpdateTermOnHigherTerm() {
        // Given: node at term 2
        state.updateTerm(2);

        // When: receive AppendEntries with term 5
        AppendEntriesRequest request = AppendEntriesRequest.newBuilder()
                .setTerm(5)
                .setLeaderId("node-2")
                .setPrevLogIndex(0)
                .setPrevLogTerm(0)
                .setLeaderCommit(0)
                .build();

        AppendEntriesResponse response = handler.handleAppendEntries(request);

        // Then: update term and accept
        assertTrue(response.getSuccess());
        assertEquals(5, state.getCurrentTerm());
        assertTrue(state.isFollower());
        assertEquals("node-2", state.getCurrentLeader());
    }

    @Test
    void testResetElectionTimer() {
        // Given: valid AppendEntries
        electionTimerReset.set(false);

        // When: handle AppendEntries
        AppendEntriesRequest request = AppendEntriesRequest.newBuilder()
                .setTerm(1)
                .setLeaderId("node-2")
                .setPrevLogIndex(0)
                .setPrevLogTerm(0)
                .setLeaderCommit(0)
                .build();

        handler.handleAppendEntries(request);

        // Then: election timer should be reset
        assertTrue(electionTimerReset.get());
    }

    @Test
    void testRejectMissingPrevLogEntry() {
        // Given: log is empty
        assertEquals(0, log.size());

        // When: receive AppendEntries with prevLogIndex=5
        AppendEntriesRequest request = AppendEntriesRequest.newBuilder()
                .setTerm(1)
                .setLeaderId("node-2")
                .setPrevLogIndex(5)
                .setPrevLogTerm(1)
                .setLeaderCommit(0)
                .build();

        AppendEntriesResponse response = handler.handleAppendEntries(request);

        // Then: reject due to log inconsistency
        assertFalse(response.getSuccess());
        assertTrue(response.getConflictIndex() > 0);
    }

    @Test
    void testRejectMismatchedPrevLogTerm() throws Exception {
        // Given: log has entry at index 1 with term 1
        RaftLogEntry entry = RaftLogEntry.create(1, 1, new RaftCommand("test", new byte[0]));
        log.append(entry);

        // When: receive AppendEntries with prevLogTerm=2 (mismatch)
        AppendEntriesRequest request = AppendEntriesRequest.newBuilder()
                .setTerm(2)
                .setLeaderId("node-2")
                .setPrevLogIndex(1)
                .setPrevLogTerm(2) // Mismatch!
                .setLeaderCommit(0)
                .build();

        AppendEntriesResponse response = handler.handleAppendEntries(request);

        // Then: reject
        assertFalse(response.getSuccess());
        assertEquals(1, response.getConflictIndex());
    }

    @Test
    void testAppendNewEntries() throws Exception {
        // Given: log is empty
        assertEquals(0, log.size());

        // When: receive AppendEntries with 2 new entries
        List<com.danieljhkim.kvdb.proto.raft.RaftLogEntry> protoEntries = List.of(
                createProtoEntry(1, 1),
                createProtoEntry(2, 1)
        );

        AppendEntriesRequest request = AppendEntriesRequest.newBuilder()
                .setTerm(1)
                .setLeaderId("node-2")
                .setPrevLogIndex(0)
                .setPrevLogTerm(0)
                .addAllEntries(protoEntries)
                .setLeaderCommit(0)
                .build();

        AppendEntriesResponse response = handler.handleAppendEntries(request);

        // Then: accept and append entries
        assertTrue(response.getSuccess());
        assertEquals(2, log.size());
        assertEquals(2, response.getMatchIndex());
    }

    @Test
    void testTruncateConflictingEntries() throws Exception {
        // Given: log has entries [1,1] [2,1] [3,1]
        log.append(RaftLogEntry.create(1, 1, new RaftCommand("cmd1", new byte[0])));
        log.append(RaftLogEntry.create(2, 1, new RaftCommand("cmd2", new byte[0])));
        log.append(RaftLogEntry.create(3, 1, new RaftCommand("cmd3", new byte[0])));
        assertEquals(3, log.size());

        // When: receive AppendEntries with conflicting entry at index 2 (term 2)
        List<com.danieljhkim.kvdb.proto.raft.RaftLogEntry> protoEntries = List.of(
                createProtoEntry(2, 2), // Conflict!
                createProtoEntry(3, 2)
        );

        AppendEntriesRequest request = AppendEntriesRequest.newBuilder()
                .setTerm(2)
                .setLeaderId("node-2")
                .setPrevLogIndex(1)
                .setPrevLogTerm(1)
                .addAllEntries(protoEntries)
                .setLeaderCommit(0)
                .build();

        AppendEntriesResponse response = handler.handleAppendEntries(request);

        // Then: truncate and append new entries
        assertTrue(response.getSuccess());
        assertEquals(3, log.size());
        assertEquals(2, log.getEntry(2).get().term()); // New term
        assertEquals(2, log.getEntry(3).get().term()); // New term
    }

    @Test
    void testUpdateCommitIndex() {
        // Given: commit index is 0
        assertEquals(0, state.getCommitIndex());

        // When: receive AppendEntries with leaderCommit=5
        AppendEntriesRequest request = AppendEntriesRequest.newBuilder()
                .setTerm(1)
                .setLeaderId("node-2")
                .setPrevLogIndex(0)
                .setPrevLogTerm(0)
                .setLeaderCommit(5)
                .build();

        handler.handleAppendEntries(request);

        // Then: commit index should advance (limited by log size)
        assertEquals(Math.min(5, log.size()), state.getCommitIndex());
    }

    @Test
    void testHeartbeat() {
        // Given: log and commitIndex at 0
        assertEquals(0, log.size());

        // When: receive heartbeat (empty entries)
        AppendEntriesRequest request = AppendEntriesRequest.newBuilder()
                .setTerm(1)
                .setLeaderId("node-2")
                .setPrevLogIndex(0)
                .setPrevLogTerm(0)
                .setLeaderCommit(0)
                .build();

        AppendEntriesResponse response = handler.handleAppendEntries(request);

        // Then: accept heartbeat
        assertTrue(response.getSuccess());
        assertEquals(0, log.size()); // No entries added
        assertTrue(electionTimerReset.get()); // Timer should be reset
    }

    private com.danieljhkim.kvdb.proto.raft.RaftLogEntry createProtoEntry(long index, long term) throws Exception {
        RaftLogEntry entry = RaftLogEntry.create(index, term, new RaftCommand("cmd" + index, new byte[0]));
        return com.danieljhkim.kvdb.proto.raft.RaftLogEntry.parseFrom(entry.toBytes());
    }
}

