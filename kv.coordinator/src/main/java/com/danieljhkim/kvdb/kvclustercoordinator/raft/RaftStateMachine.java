package com.danieljhkim.kvdb.kvclustercoordinator.raft;

import com.danieljhkim.kvdb.kvclustercoordinator.state.ShardMapSnapshot;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Interface for the Raft-replicated state machine that manages cluster
 * metadata.
 *
 * <p>
 * In a production system, implementations would:
 * <ul>
 * <li>Replicate commands via Raft consensus before applying</li>
 * <li>Support leader election and log compaction</li>
 * <li>Persist state to durable storage</li>
 * </ul>
 *
 * <p>
 * For the initial implementation, {@link StubRaftStateMachine} provides a
 * single-node stub that applies commands synchronously without replication.
 */
public interface RaftStateMachine {

	/**
	 * Applies a command to the state machine.
	 *
	 * <p>
	 * In a Raft implementation, this would:
	 * <ol>
	 * <li>Append the command to the Raft log</li>
	 * <li>Replicate to a majority of nodes</li>
	 * <li>Apply to the state machine once committed</li>
	 * <li>Notify watchers of the change</li>
	 * </ol>
	 *
	 * @param command
	 *            the command to apply
	 * @return a future that completes when the command is committed and applied
	 */
	CompletableFuture<Void> apply(RaftCommand command);

	/**
	 * Returns the current snapshot of the shard map.
	 * This is safe to call from any thread.
	 *
	 * @return the current immutable snapshot
	 */
	ShardMapSnapshot getSnapshot();

	/**
	 * Returns the current map version.
	 * Convenience method equivalent to {@code getSnapshot().getMapVersion()}.
	 *
	 * @return the current map version
	 */
	default long getMapVersion() {
		return getSnapshot().getMapVersion();
	}

	/**
	 * Adds a watcher that will be notified of shard map changes.
	 * Watchers are notified asynchronously after each command is applied.
	 *
	 * @param watcher
	 *            a consumer that receives delta updates
	 */
	void addWatcher(Consumer<ShardMapDelta> watcher);

	/**
	 * Removes a previously registered watcher.
	 *
	 * @param watcher
	 *            the watcher to remove
	 * @return true if the watcher was found and removed
	 */
	boolean removeWatcher(Consumer<ShardMapDelta> watcher);

	/**
	 * Returns true if this node is the current Raft leader.
	 * Only the leader can accept write commands.
	 *
	 * @return true if this is the leader
	 */
	boolean isLeader();
}
