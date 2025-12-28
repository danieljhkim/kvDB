package com.danieljhkim.kvdb.kvclustercoordinator.raft;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.danieljhkim.kvdb.kvclustercoordinator.state.ClusterState;
import com.danieljhkim.kvdb.kvclustercoordinator.state.ShardMapSnapshot;

/**
 * Stub implementation of {@link RaftStateMachine} for single-node operation.
 *
 * <p>This implementation:
 * <ul>
 *   <li>Applies commands synchronously without Raft replication</li>
 *   <li>Always considers itself the leader</li>
 *   <li>Provides thread-safe reads via immutable snapshots</li>
 *   <li>Notifies watchers of state changes</li>
 * </ul>
 *
 * <p>This is suitable for development, testing, and single-coordinator deployments.
 * For production HA, replace with a real Raft implementation (e.g., Apache Ratis, MicroRaft).
 */
public class StubRaftStateMachine implements RaftStateMachine {

    private static final Logger LOGGER = Logger.getLogger(StubRaftStateMachine.class.getName());

    private final ClusterState state;
    private final AtomicReference<ShardMapSnapshot> snapshotRef;
    private final List<Consumer<ShardMapDelta>> watchers;
    private final Object writeLock = new Object();

    public StubRaftStateMachine() {
        this.state = new ClusterState();
        this.snapshotRef = new AtomicReference<>(ShardMapSnapshot.empty());
        this.watchers = new CopyOnWriteArrayList<>();
    }

    @Override
    public CompletableFuture<Void> apply(RaftCommand command) {
        return CompletableFuture.supplyAsync(() -> {
            synchronized (writeLock) {
                try {
                    ShardMapDelta delta = applyCommand(command);
                    notifyWatchers(delta);
                    LOGGER.info("Applied command: " + command.describe());
                    return null;
                } catch (Exception e) {
                    LOGGER.log(Level.SEVERE, "Failed to apply command: " + command.describe(), e);
                    throw new RuntimeException("Command application failed: " + command.describe(), e);
                }
            }
        });
    }

    /**
     * Applies a command to the state and returns the resulting delta.
     * Must be called while holding writeLock.
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
                LOGGER.log(Level.WARNING, "Watcher threw exception", e);
            }
        }
    }

    @Override
    public ShardMapSnapshot getSnapshot() {
        return snapshotRef.get();
    }

    @Override
    public void addWatcher(Consumer<ShardMapDelta> watcher) {
        if (watcher != null) {
            watchers.add(watcher);
            LOGGER.fine("Added watcher, total watchers: " + watchers.size());
        }
    }

    @Override
    public boolean removeWatcher(Consumer<ShardMapDelta> watcher) {
        boolean removed = watchers.remove(watcher);
        if (removed) {
            LOGGER.fine("Removed watcher, remaining watchers: " + watchers.size());
        }
        return removed;
    }

    @Override
    public boolean isLeader() {
        // Stub is always the leader (single-node mode)
        return true;
    }

    /**
     * Returns the number of registered watchers.
     * Useful for testing and debugging.
     */
    public int getWatcherCount() {
        return watchers.size();
    }

    /**
     * Applies a command synchronously and blocks until completion.
     * Convenience method for testing.
     */
    public void applySync(RaftCommand command) {
        apply(command).join();
    }
}

