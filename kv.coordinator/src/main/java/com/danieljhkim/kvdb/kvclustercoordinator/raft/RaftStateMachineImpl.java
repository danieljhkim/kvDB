package com.danieljhkim.kvdb.kvclustercoordinator.raft;

import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.FileBasedRaftLog;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftLog;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftLogEntry;
import com.danieljhkim.kvdb.kvclustercoordinator.state.ClusterState;
import com.danieljhkim.kvdb.kvclustercoordinator.state.ShardMapDelta;
import com.danieljhkim.kvdb.kvclustercoordinator.state.ShardMapSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class RaftStateMachineImpl implements RaftStateMachine {

    private static final Logger logger = LoggerFactory.getLogger(RaftStateMachineImpl.class);

    private final ClusterState state;
    private final AtomicReference<ShardMapSnapshot> snapshotRef;
    private final List<Consumer<ShardMapDelta>> watchers;
    private final Object writeLock = new Object();
    private final RaftLog raftLog;
    private final AtomicLong currentIndex = new AtomicLong(0);
    private final AtomicLong currentTerm = new AtomicLong(0);

    public RaftStateMachineImpl(Path logPath) throws IOException {
        this.state = new ClusterState();
        this.snapshotRef = new AtomicReference<>(ShardMapSnapshot.empty());
        this.watchers = new CopyOnWriteArrayList<>();
        this.raftLog = new FileBasedRaftLog(logPath);

        replayLog();
    }

    @Override
    public CompletableFuture<Void> apply(RaftCommand command) {
        return CompletableFuture.supplyAsync(() -> {
            synchronized (writeLock) {
                try {
                    // Persist to log first
                    RaftLogEntry entry =
                            RaftLogEntry.create(currentIndex.incrementAndGet(), currentTerm.get(), command);
                    raftLog.append(entry);

                    // Then apply to state machine
                    ShardMapDelta delta = applyCommand(command);
                    notifyWatchers(delta);
                    logger.info("Applied command: {}", command.describe());
                    return null;
                } catch (Exception e) {
                    logger.error("Failed to apply command: {}", command.describe(), e);
                    throw new RuntimeException("Command application failed", e);
                }
            }
        });
    }
    /**
     * Applies a command to the state and returns the resulting delta. Must be called while holding writeLock.
     */
    private ShardMapDelta applyCommand(RaftCommand command) {
        long oldVersion = state.getMapVersion();
        ShardMapDelta delta;

        switch (command) {
            case RaftCommand.InitShards cmd -> {
                List<String> createdShards = state.initializeShards(cmd.numShards(), cmd.replicationFactor());
                ShardMapSnapshot snapshot = updateSnapshot();
                delta = ShardMapDelta.forShardChanges(snapshot.getMapVersion(), createdShards, snapshot);
            }
            case RaftCommand.RegisterNode cmd -> {
                state.registerNode(cmd.nodeId(), cmd.address(), cmd.zone());
                ShardMapSnapshot snapshot = updateSnapshot();
                delta = ShardMapDelta.forNodeChange(snapshot.getMapVersion(), cmd.nodeId(), snapshot);
            }
            case RaftCommand.SetNodeStatus cmd -> {
                state.setNodeStatus(cmd.nodeId(), cmd.status());
                ShardMapSnapshot snapshot = updateSnapshot();
                delta = ShardMapDelta.forNodeChange(snapshot.getMapVersion(), cmd.nodeId(), snapshot);
            }
            case RaftCommand.SetShardReplicas cmd -> {
                state.setShardReplicas(cmd.shardId(), cmd.replicas());
                ShardMapSnapshot snapshot = updateSnapshot();
                delta = ShardMapDelta.forShardChange(snapshot.getMapVersion(), cmd.shardId(), snapshot);
            }
            case RaftCommand.SetShardLeader cmd -> {
                state.setShardLeader(cmd.shardId(), cmd.epoch(), cmd.leaderNodeId());
                ShardMapSnapshot snapshot = updateSnapshot();
                delta = ShardMapDelta.forShardChange(snapshot.getMapVersion(), cmd.shardId(), snapshot);
            }
        }

        return delta;
    }

    /**
     * Creates a new snapshot and updates the atomic reference.
     */
    private ShardMapSnapshot updateSnapshot() {
        ShardMapSnapshot snapshot = state.createSnapshot();
        snapshotRef.set(snapshot);
        return snapshot;
    }

    /**
     * Notifies all watchers of a state change.
     */
    private void notifyWatchers(ShardMapDelta delta) {
        for (Consumer<ShardMapDelta> watcher : watchers) {
            try {
                watcher.accept(delta);
            } catch (Exception e) {
                logger.warn("Watcher threw exception", e);
            }
        }
    }

    private void replayLog() throws IOException {
        List<RaftLogEntry> entries = raftLog.getEntriesSince(0);
        for (RaftLogEntry entry : entries) {
            applyCommand(entry.command());
            currentIndex.set(entry.index());
            currentTerm.set(entry.term());
        }
        logger.info("Replayed {} log entries", entries.size());
    }

    @Override
    public ShardMapSnapshot getSnapshot() {
        return snapshotRef.get();
    }

    @Override
    public void addWatcher(Consumer<ShardMapDelta> watcher) {
        if (watcher != null) {
            watchers.add(watcher);
            logger.debug("Added watcher, total watchers: {}", watchers.size());
        }
    }

    @Override
    public boolean removeWatcher(Consumer<ShardMapDelta> watcher) {
        boolean removed = watchers.remove(watcher);
        if (removed) {
            logger.debug("Removed watcher, remaining watchers: {}", watchers.size());
        }
        return removed;
    }

    @Override
    public boolean isLeader() {
        // Stub is always the leader (single-node mode)
        return true;
    }

    /**
     * Returns the number of registered watchers. Useful for testing and debugging.
     */
    public int getWatcherCount() {
        return watchers.size();
    }

    /**
     * Applies a command synchronously and blocks until completion. Convenience method for testing.
     */
    public void applySync(RaftCommand command) {
        apply(command).join();
    }
}
