package com.danieljhkim.kvdb.kvclustercoordinator.raft;

import static org.junit.jupiter.api.Assertions.*;

import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.FileBasedRaftLog;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftLog;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftPersistentStateStore;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.state.RaftNodeState;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.state.RaftRole;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for Phase 1 Raft components: Configuration, NodeState, and PersistentStateStore.
 */
class RaftPhase1Test {

    @TempDir
    Path tempDir;

    @Test
    void testRaftConfiguration_basicSetup() {
        // Given
        Map<String, String> members =
                Map.of("node1", "localhost:5001", "node2", "localhost:5002", "node3", "localhost:5003");

        // When
        RaftConfiguration config = RaftConfiguration.builder()
                .nodeId("node1")
                .clusterMembers(members)
                .electionTimeoutMin(Duration.ofMillis(150))
                .electionTimeoutMax(Duration.ofMillis(300))
                .heartbeatInterval(Duration.ofMillis(50))
                .maxEntriesPerAppendRequest(100)
                .snapshotThreshold(10000)
                .dataDirectory(tempDir.toString())
                .build();

        // Then
        assertEquals("node1", config.getNodeId());
        assertEquals(3, config.getClusterSize());
        assertEquals(2, config.getQuorumSize());
        assertEquals(2, config.getPeers().size());
        assertTrue(config.getPeers().containsKey("node2"));
        assertTrue(config.getPeers().containsKey("node3"));
        assertFalse(config.getPeers().containsKey("node1"));
    }

    @Test
    void testRaftConfiguration_validation() {
        // Election timeout max must be >= min
        assertThrows(IllegalArgumentException.class, () -> {
            RaftConfiguration.builder()
                    .nodeId("node1")
                    .clusterMembers(Map.of("node1", "localhost:5001"))
                    .electionTimeoutMin(Duration.ofMillis(300))
                    .electionTimeoutMax(Duration.ofMillis(150))
                    .build();
        });

        // Heartbeat must be less than election timeout min
        assertThrows(IllegalArgumentException.class, () -> {
            RaftConfiguration.builder()
                    .nodeId("node1")
                    .clusterMembers(Map.of("node1", "localhost:5001"))
                    .electionTimeoutMin(Duration.ofMillis(150))
                    .heartbeatInterval(Duration.ofMillis(200))
                    .build();
        });

        // NodeId must be in cluster members
        assertThrows(IllegalArgumentException.class, () -> {
            RaftConfiguration.builder()
                    .nodeId("node1")
                    .clusterMembers(Map.of("node2", "localhost:5002", "node3", "localhost:5003"))
                    .build();
        });
    }

    @Test
    void testRaftNodeState_initialState() throws IOException {
        // Given
        RaftLog log = new FileBasedRaftLog(tempDir.resolve("log"));
        RaftNodeState state = new RaftNodeState("node1", log);

        // Then
        assertEquals(0, state.getCurrentTerm());
        assertNull(state.getVotedFor());
        assertEquals(RaftRole.FOLLOWER, state.getCurrentRole());
        assertNull(state.getCurrentLeader());
        assertEquals(0, state.getCommitIndex());
        assertEquals(0, state.getLastApplied());
        assertFalse(state.isLeader());
        assertFalse(state.isCandidate());
        assertTrue(state.isFollower());
    }

    @Test
    void testRaftNodeState_termManagement() throws IOException {
        // Given
        RaftLog log = new FileBasedRaftLog(tempDir.resolve("log"));
        RaftNodeState state = new RaftNodeState("node1", log);

        // When: increment term
        long newTerm = state.incrementTerm();

        // Then
        assertEquals(1, newTerm);
        assertEquals(1, state.getCurrentTerm());
        assertNull(state.getVotedFor()); // votedFor should be cleared

        // When: update term to higher value
        state.setVotedFor("node2");
        boolean updated = state.updateTerm(5);

        // Then
        assertTrue(updated);
        assertEquals(5, state.getCurrentTerm());
        assertNull(state.getVotedFor()); // votedFor should be cleared again

        // When: try to update to lower term
        updated = state.updateTerm(3);

        // Then
        assertFalse(updated);
        assertEquals(5, state.getCurrentTerm()); // term unchanged
    }

    @Test
    void testRaftNodeState_roleTransitions() throws IOException {
        // Given
        RaftLog log = new FileBasedRaftLog(tempDir.resolve("log"));
        RaftNodeState state = new RaftNodeState("node1", log);

        // When: become candidate
        state.becomeCandidate();

        // Then
        assertEquals(RaftRole.CANDIDATE, state.getCurrentRole());
        assertEquals(1, state.getCurrentTerm()); // term incremented
        assertEquals("node1", state.getVotedFor()); // voted for self
        assertTrue(state.isCandidate());

        // When: become leader
        state.becomeLeader(java.util.List.of("node2", "node3"));

        // Then
        assertEquals(RaftRole.LEADER, state.getCurrentRole());
        assertEquals("node1", state.getCurrentLeader());
        assertTrue(state.isLeader());
        assertEquals(1L, state.getNextIndex("node2")); // initialized to lastLogIndex + 1
        assertEquals(0L, state.getMatchIndex("node2")); // initialized to 0

        // When: become follower
        state.becomeFollower(2, "node2");

        // Then
        assertEquals(RaftRole.FOLLOWER, state.getCurrentRole());
        assertEquals(2, state.getCurrentTerm());
        assertEquals("node2", state.getCurrentLeader());
        assertTrue(state.isFollower());
        assertTrue(state.getNextIndexMap().isEmpty()); // leader state cleared
    }

    @Test
    void testRaftNodeState_leaderStateManagement() throws IOException {
        // Given
        RaftLog log = new FileBasedRaftLog(tempDir.resolve("log"));
        RaftNodeState state = new RaftNodeState("node1", log);
        state.becomeCandidate();
        state.becomeLeader(java.util.List.of("node2", "node3"));

        // When: update match index
        state.setMatchIndex("node2", 5);

        // Then
        assertEquals(5L, state.getMatchIndex("node2"));
        assertEquals(6L, state.getNextIndex("node2")); // nextIndex = matchIndex + 1

        // When: set next index directly (e.g., after conflict)
        state.setNextIndex("node3", 3);

        // Then
        assertEquals(3L, state.getNextIndex("node3"));
    }

    @Test
    void testRaftPersistentStateStore_saveAndLoad() throws IOException {
        // Given
        String dataDir = tempDir.resolve("raft").toString();
        RaftPersistentStateStore store = new RaftPersistentStateStore(dataDir);

        // When: save state
        store.save(42, "node2");

        // Then: load state
        RaftPersistentStateStore.PersistentState loaded = store.load();
        assertEquals(42, loaded.getCurrentTerm());
        assertEquals("node2", loaded.getVotedFor());
    }

    @Test
    void testRaftPersistentStateStore_saveWithNullVotedFor() throws IOException {
        // Given
        String dataDir = tempDir.resolve("raft").toString();
        RaftPersistentStateStore store = new RaftPersistentStateStore(dataDir);

        // When: save with null votedFor
        store.save(10, null);

        // Then: load state
        RaftPersistentStateStore.PersistentState loaded = store.load();
        assertEquals(10, loaded.getCurrentTerm());
        assertNull(loaded.getVotedFor());
    }

    @Test
    void testRaftPersistentStateStore_loadWhenNoFileExists() throws IOException {
        // Given
        String dataDir = tempDir.resolve("raft").toString();
        RaftPersistentStateStore store = new RaftPersistentStateStore(dataDir);

        // When: load without saving first
        RaftPersistentStateStore.PersistentState loaded = store.load();

        // Then: should return default state
        assertEquals(0, loaded.getCurrentTerm());
        assertNull(loaded.getVotedFor());
    }

    @Test
    void testRaftPersistentStateStore_clear() throws IOException {
        // Given
        String dataDir = tempDir.resolve("raft").toString();
        RaftPersistentStateStore store = new RaftPersistentStateStore(dataDir);
        store.save(100, "node5");

        // When: clear
        store.clear();

        // Then: load should return default state
        RaftPersistentStateStore.PersistentState loaded = store.load();
        assertEquals(0, loaded.getCurrentTerm());
        assertNull(loaded.getVotedFor());
    }

    @Test
    void testRaftNodeState_commitIndexAdvancement() throws IOException {
        // Given
        RaftLog log = new FileBasedRaftLog(tempDir.resolve("log"));
        RaftNodeState state = new RaftNodeState("node1", log);

        // When: update commit index
        state.updateCommitIndex(5);

        // Then
        assertEquals(5, state.getCommitIndex());

        // When: update to lower value (should not regress)
        state.updateCommitIndex(3);

        // Then
        assertEquals(5, state.getCommitIndex()); // unchanged

        // When: update to higher value
        state.updateCommitIndex(10);

        // Then
        assertEquals(10, state.getCommitIndex());
    }

    @Test
    void testRaftNodeState_lastAppliedManagement() throws IOException {
        // Given
        RaftLog log = new FileBasedRaftLog(tempDir.resolve("log"));
        RaftNodeState state = new RaftNodeState("node1", log);

        // When: update last applied
        state.updateLastApplied(7);

        // Then
        assertEquals(7, state.getLastApplied());
    }
}
