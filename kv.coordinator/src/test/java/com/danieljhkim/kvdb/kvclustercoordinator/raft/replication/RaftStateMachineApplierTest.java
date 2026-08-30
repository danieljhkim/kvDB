package com.danieljhkim.kvdb.kvclustercoordinator.raft.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftCommand;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.FileBasedRaftLog;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftLog;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftLogEntry;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.state.RaftNodeState;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.statemachine.RaftStateMachine;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.statemachine.RaftStateMachineImpl;
import com.danieljhkim.kvdb.kvclustercoordinator.state.ShardMapDelta;
import com.danieljhkim.kvdb.kvclustercoordinator.state.ShardMapSnapshot;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RaftStateMachineApplierTest {

    @TempDir
    Path tempDir;

    @Test
    void restartReplaysCommittedEntriesInOrderExactlyOnceAndHidesUncommittedTail() throws Exception {
        Path logPath = tempDir.resolve("raft-log");
        try (RaftLog log = new FileBasedRaftLog(logPath)) {
            log.append(entry(1, command("node-1")));
            log.append(entry(2, command("node-2")));
            log.append(entry(3, command("uncommitted")));
        }

        Recovery first = recover(logPath, 2);
        Recovery restarted = recover(logPath, 2);

        assertEquals(List.of("node-1", "node-2"), first.changedNodes());
        assertEquals(first.changedNodes(), restarted.changedNodes());
        assertEquals(2, first.lastApplied());
        assertEquals(2, restarted.lastApplied());
        assertTrue(restarted.snapshot().getNodes().containsKey("node-1"));
        assertTrue(restarted.snapshot().getNodes().containsKey("node-2"));
        assertNull(restarted.snapshot().getNode("uncommitted"));
    }

    @Test
    void applyFailureDoesNotAdvancePastFailedEntryAndMakesApplierUnavailable() {
        RaftCommand first = command("node-1");
        RaftCommand failed = command("node-2");
        RaftCommand third = command("node-3");
        InMemoryRaftLog log = new InMemoryRaftLog(List.of(entry(1, first), entry(2, failed), entry(3, third)));
        RaftNodeState state = committedState(log, 3);
        List<RaftCommand> attempts = new ArrayList<>();
        RaftStateMachine stateMachine = recordingStateMachine(
                attempts,
                command -> command.equals(failed)
                        ? CompletableFuture.failedFuture(new IOException("injected apply failure"))
                        : CompletableFuture.completedFuture(null));
        RaftStateMachineApplier applier = directApplier(state, stateMachine);

        assertThrows(
                CompletionException.class, () -> applier.applyCommittedEntries().join());

        assertEquals(1, state.getLastApplied());
        assertEquals(List.of(first, failed), attempts);
        assertFalse(applier.isRunning());
        assertThrows(
                CompletionException.class, () -> applier.applyCommittedEntries().join());
        assertEquals(List.of(first, failed), attempts);
    }

    @Test
    void raftLogReadFailureDoesNotAdvancePastLastSuccessfulEntry() {
        RaftCommand first = command("node-1");
        InMemoryRaftLog log = new InMemoryRaftLog(List.of(entry(1, first), entry(2, command("node-2"))));
        log.failReadAt(2);
        RaftNodeState state = committedState(log, 2);
        List<RaftCommand> applied = new ArrayList<>();
        RaftStateMachineApplier applier = directApplier(
                state, recordingStateMachine(applied, ignored -> CompletableFuture.completedFuture(null)));

        assertThrows(
                CompletionException.class, () -> applier.applyCommittedEntries().join());

        assertEquals(1, state.getLastApplied());
        assertEquals(List.of(first), applied);
        assertFalse(applier.isRunning());
    }

    @Test
    void concurrentTriggersCannotReorderOrDoubleApplyEntries() throws Exception {
        List<RaftCommand> commands = List.of(command("node-1"), command("node-2"), command("node-3"));
        InMemoryRaftLog log = new InMemoryRaftLog(
                List.of(entry(1, commands.get(0)), entry(2, commands.get(1)), entry(3, commands.get(2))));
        RaftNodeState state = committedState(log, 3);
        List<RaftCommand> applied = new ArrayList<>();
        CountDownLatch firstApplyEntered = new CountDownLatch(1);
        CountDownLatch releaseFirstApply = new CountDownLatch(1);
        AtomicBoolean first = new AtomicBoolean(true);
        RaftStateMachine stateMachine = recordingStateMachine(applied, ignored -> {
            if (first.compareAndSet(true, false)) {
                firstApplyEntered.countDown();
                try {
                    if (!releaseFirstApply.await(2, TimeUnit.SECONDS)) {
                        return CompletableFuture.failedFuture(new IllegalStateException("test timed out"));
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return CompletableFuture.failedFuture(e);
                }
            }
            return CompletableFuture.completedFuture(null);
        });

        ExecutorService executor = Executors.newFixedThreadPool(3);
        try {
            RaftStateMachineApplier applier = new RaftStateMachineApplier("node", state, stateMachine, executor);
            applier.start();
            CompletableFuture<Void> firstTrigger = applier.applyCommittedEntries();
            assertTrue(firstApplyEntered.await(2, TimeUnit.SECONDS));
            CompletableFuture<Void> secondTrigger = applier.applyCommittedEntries();
            CompletableFuture<Void> thirdTrigger = applier.applyCommittedEntries();

            releaseFirstApply.countDown();
            CompletableFuture.allOf(firstTrigger, secondTrigger, thirdTrigger).get(2, TimeUnit.SECONDS);

            assertEquals(commands, applied);
            assertEquals(3, state.getLastApplied());
        } finally {
            releaseFirstApply.countDown();
            executor.shutdownNow();
        }
    }

    private Recovery recover(Path logPath, long commitIndex) throws Exception {
        try (RaftLog log = new FileBasedRaftLog(logPath)) {
            RaftNodeState state = committedState(log, commitIndex);
            RaftStateMachineImpl stateMachine = new RaftStateMachineImpl();
            List<String> changedNodes = new ArrayList<>();
            stateMachine.addWatcher(delta -> changedNodes.addAll(delta.changedNodes()));
            RaftStateMachineApplier applier = directApplier(state, stateMachine);

            applier.applyCommittedEntries().join();

            return new Recovery(stateMachine.getSnapshot(), List.copyOf(changedNodes), state.getLastApplied());
        }
    }

    private static RaftNodeState committedState(RaftLog log, long commitIndex) {
        RaftNodeState state = new RaftNodeState("node", log);
        state.advanceCommitIndex(commitIndex);
        return state;
    }

    private static RaftStateMachineApplier directApplier(RaftNodeState state, RaftStateMachine stateMachine) {
        RaftStateMachineApplier applier = new RaftStateMachineApplier("node", state, stateMachine, Runnable::run);
        applier.start();
        return applier;
    }

    private static RaftStateMachine recordingStateMachine(
            List<RaftCommand> attempts, Function<RaftCommand, CompletableFuture<Void>> result) {
        return new RaftStateMachine() {
            @Override
            public CompletableFuture<Void> apply(RaftCommand command) {
                attempts.add(command);
                return result.apply(command);
            }

            @Override
            public ShardMapSnapshot getSnapshot() {
                return ShardMapSnapshot.empty();
            }

            @Override
            public void addWatcher(Consumer<ShardMapDelta> watcher) {}

            @Override
            public boolean removeWatcher(Consumer<ShardMapDelta> watcher) {
                return false;
            }

            @Override
            public boolean isLeader() {
                return true;
            }
        };
    }

    private static RaftCommand command(String nodeId) {
        return new RaftCommand.RegisterNode(nodeId, nodeId + ":9000", "zone-a");
    }

    private static RaftLogEntry entry(long index, RaftCommand command) {
        return new RaftLogEntry(index, 7, index, command);
    }

    private record Recovery(ShardMapSnapshot snapshot, List<String> changedNodes, long lastApplied) {}

    private static final class InMemoryRaftLog implements RaftLog {
        private final List<RaftLogEntry> entries;
        private long failingReadIndex = -1;

        private InMemoryRaftLog(List<RaftLogEntry> entries) {
            this.entries = new ArrayList<>(entries);
        }

        private void failReadAt(long index) {
            failingReadIndex = index;
        }

        @Override
        public void append(RaftLogEntry entry) {
            entries.add(entry);
        }

        @Override
        public List<RaftLogEntry> getEntriesSince(long fromIndex) throws IOException {
            List<RaftLogEntry> result = new ArrayList<>();
            for (long index = fromIndex; index <= entries.size(); index++) {
                getEntry(index).ifPresent(result::add);
            }
            return result;
        }

        @Override
        public Optional<RaftLogEntry> getEntry(long index) throws IOException {
            if (index == failingReadIndex) {
                throw new IOException("injected disk read failure");
            }
            if (index < 1 || index > entries.size()) {
                return Optional.empty();
            }
            return Optional.of(entries.get((int) index - 1));
        }

        @Override
        public Optional<RaftLogEntry> getLastEntry() {
            return entries.isEmpty() ? Optional.empty() : Optional.of(entries.getLast());
        }

        @Override
        public long size() {
            return entries.size();
        }

        @Override
        public void truncateAfter(long index) {
            if (index < entries.size()) {
                entries.subList(Math.max(0, (int) index), entries.size()).clear();
            }
        }

        @Override
        public void close() {}
    }
}
