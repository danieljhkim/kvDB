package com.danieljhkim.kvdb.kvclustercoordinator.raft.replication;

import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftCommand;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftConfiguration;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.FileBasedRaftLog;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftLog;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftLogEntry;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.state.RaftNodeState;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.statemachine.RaftStateMachine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

class RaftStateMachineApplierTest {

    @TempDir
    Path tempDir;

    private RaftNodeState state;
    private RaftLog log;
    private TestStateMachine stateMachine;
    private RaftStateMachineApplier applier;

    @BeforeEach
    void setUp() throws Exception {
        log = new FileBasedRaftLog(tempDir.resolve("raftlog"));
        state = new RaftNodeState("node-1", log);
        stateMachine = new TestStateMachine();
        applier = new RaftStateMachineApplier("node-1", state, stateMachine);
    }

    @AfterEach
    void tearDown() throws Exception {
        applier.stop();
        log.close();
    }

    @Test
    void testApplyCommittedEntries() throws Exception {
        // Given: log has 5 entries, commitIndex=3, lastApplied=0
        for (int i = 1; i <= 5; i++) {
            log.append(RaftLogEntry.create(i, 1, new RaftCommand("cmd" + i, new byte[0])));
        }
        state.updateCommitIndex(3);
        assertEquals(0, state.getLastApplied());

        applier.start();

        // When: apply committed entries
        CompletableFuture<Void> future = applier.applyCommittedEntries();
        future.get();

        // Then: entries 1-3 should be applied
        assertEquals(3, state.getLastApplied());
        assertEquals(3, stateMachine.appliedCommands.size());
        assertEquals("cmd1", stateMachine.appliedCommands.get(0));
        assertEquals("cmd2", stateMachine.appliedCommands.get(1));
        assertEquals("cmd3", stateMachine.appliedCommands.get(2));
    }

    @Test
    void testNoApplyWhenCommitIndexEqualslastApplied() throws Exception {
        // Given: log has 3 entries, commitIndex=lastApplied=3
        for (int i = 1; i <= 3; i++) {
            log.append(RaftLogEntry.create(i, 1, new RaftCommand("cmd" + i, new byte[0])));
        }
        state.updateCommitIndex(3);
        state.updateLastApplied(3);

        applier.start();

        // When: try to apply
        CompletableFuture<Void> future = applier.applyCommittedEntries();
        future.get();

        // Then: nothing should be applied
        assertEquals(3, state.getLastApplied());
        assertEquals(0, stateMachine.appliedCommands.size());
    }

    @Test
    void testApplyOnlyNewCommittedEntries() throws Exception {
        // Given: log has 5 entries, commitIndex=5, lastApplied=2
        for (int i = 1; i <= 5; i++) {
            log.append(RaftLogEntry.create(i, 1, new RaftCommand("cmd" + i, new byte[0])));
        }
        state.updateCommitIndex(5);
        state.updateLastApplied(2);

        applier.start();

        // When: apply
        CompletableFuture<Void> future = applier.applyCommittedEntries();
        future.get();

        // Then: only entries 3-5 should be applied
        assertEquals(5, state.getLastApplied());
        assertEquals(3, stateMachine.appliedCommands.size());
        assertEquals("cmd3", stateMachine.appliedCommands.get(0));
        assertEquals("cmd4", stateMachine.appliedCommands.get(1));
        assertEquals("cmd5", stateMachine.appliedCommands.get(2));
    }

    @Test
    void testIdempotentApply() throws Exception {
        // Given: log has 3 entries, commitIndex=3
        for (int i = 1; i <= 3; i++) {
            log.append(RaftLogEntry.create(i, 1, new RaftCommand("cmd" + i, new byte[0])));
        }
        state.updateCommitIndex(3);

        applier.start();

        // When: apply multiple times
        applier.applyCommittedEntries().get();
        applier.applyCommittedEntries().get();
        applier.applyCommittedEntries().get();

        // Then: entries should only be applied once
        assertEquals(3, state.getLastApplied());
        assertEquals(3, stateMachine.appliedCommands.size());
    }

    @Test
    void testIncrementalApply() throws Exception {
        // Given: log has 5 entries
        for (int i = 1; i <= 5; i++) {
            log.append(RaftLogEntry.create(i, 1, new RaftCommand("cmd" + i, new byte[0])));
        }

        applier.start();

        // When: commit index advances incrementally
        state.updateCommitIndex(2);
        applier.applyCommittedEntries().get();
        assertEquals(2, state.getLastApplied());
        assertEquals(2, stateMachine.appliedCommands.size());

        state.updateCommitIndex(4);
        applier.applyCommittedEntries().get();
        assertEquals(4, state.getLastApplied());
        assertEquals(4, stateMachine.appliedCommands.size());

        state.updateCommitIndex(5);
        applier.applyCommittedEntries().get();
        assertEquals(5, state.getLastApplied());
        assertEquals(5, stateMachine.appliedCommands.size());

        // Then: all entries should be applied in order
        for (int i = 1; i <= 5; i++) {
            assertEquals("cmd" + i, stateMachine.appliedCommands.get(i - 1));
        }
    }

    @Test
    void testDoNotApplyWhenStopped() throws Exception {
        // Given: applier is stopped
        applier.stop();
        assertFalse(applier.isRunning());

        for (int i = 1; i <= 3; i++) {
            log.append(RaftLogEntry.create(i, 1, new RaftCommand("cmd" + i, new byte[0])));
        }
        state.updateCommitIndex(3);

        // When: try to apply
        CompletableFuture<Void> future = applier.applyCommittedEntries();
        future.get();

        // Then: nothing should be applied
        assertEquals(0, state.getLastApplied());
        assertEquals(0, stateMachine.appliedCommands.size());
    }

    @Test
    void testGetPendingEntries() throws Exception {
        // Given: log has 5 entries, commitIndex=5, lastApplied=2
        for (int i = 1; i <= 5; i++) {
            log.append(RaftLogEntry.create(i, 1, new RaftCommand("cmd" + i, new byte[0])));
        }
        state.updateCommitIndex(5);
        state.updateLastApplied(2);

        // Then: pending entries should be 3
        assertEquals(3, applier.getPendingEntries());

        applier.start();
        applier.applyCommittedEntries().get();

        // After apply, pending should be 0
        assertEquals(0, applier.getPendingEntries());
    }

    /**
     * Test state machine that tracks applied commands
     */
    private static class TestStateMachine implements RaftStateMachine {
        final List<String> appliedCommands = new ArrayList<>();

        @Override
        public void apply(RaftCommand command) {
            appliedCommands.add(command.type());
        }
    }
}

