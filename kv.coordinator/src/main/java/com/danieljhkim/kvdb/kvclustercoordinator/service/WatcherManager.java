package com.danieljhkim.kvdb.kvclustercoordinator.service;

import com.danieljhkim.kvdb.kvclustercoordinator.converter.ProtoConverter;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.ShardMapDelta;
import com.danieljhkim.kvdb.kvclustercoordinator.state.ShardMapSnapshot;

import io.grpc.stub.StreamObserver;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages active WatchShardMap streaming connections.
 * Converts internal delta events to proto and broadcasts to all watchers.
 */
public class WatcherManager implements Consumer<ShardMapDelta> {

	private static final Logger LOGGER = Logger.getLogger(WatcherManager.class.getName());
	private static final long HEARTBEAT_INTERVAL_MS = 30_000; // 30 seconds

	/**
	 * Context for each watcher connection.
	 */
	private record WatcherContext(
			StreamObserver<com.danieljhkim.kvdb.proto.coordinator.ShardMapDelta> observer,
			long fromVersion,
			long registeredAt) {
	}

	private final Map<StreamObserver<com.danieljhkim.kvdb.proto.coordinator.ShardMapDelta>, WatcherContext> watchers;
	private final ScheduledExecutorService heartbeatExecutor;
	private volatile boolean running;

	public WatcherManager() {
		this.watchers = new ConcurrentHashMap<>();
		this.heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
			Thread t = new Thread(r, "watcher-heartbeat");
			t.setDaemon(true);
			return t;
		});
		this.running = false;
	}

	/**
	 * Starts the heartbeat scheduler to keep streams alive.
	 */
	public void start() {
		if (running) {
			return;
		}
		running = true;
		heartbeatExecutor.scheduleAtFixedRate(
				this::sendHeartbeats, HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
		LOGGER.info("WatcherManager started with heartbeat interval " + HEARTBEAT_INTERVAL_MS + "ms");
	}

	/**
	 * Stops the heartbeat scheduler and closes all watcher connections.
	 */
	public void stop() {
		running = false;
		heartbeatExecutor.shutdown();
		try {
			if (!heartbeatExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
				heartbeatExecutor.shutdownNow();
			}
		} catch (InterruptedException e) {
			heartbeatExecutor.shutdownNow();
			Thread.currentThread().interrupt();
		}

		// Complete all watchers
		for (var entry : watchers.entrySet()) {
			try {
				entry.getKey().onCompleted();
			} catch (Exception e) {
				LOGGER.log(Level.WARNING, "Error completing watcher on shutdown", e);
			}
		}
		watchers.clear();
		LOGGER.info("WatcherManager stopped");
	}

	/**
	 * Registers a new watcher and sends the initial state if newer than
	 * fromVersion.
	 *
	 * @param observer
	 *            the gRPC stream observer
	 * @param fromVersion
	 *            the client's current version (0 for full state)
	 * @param snapshot
	 *            the current snapshot to send initially
	 */
	public void registerWatcher(
			StreamObserver<com.danieljhkim.kvdb.proto.coordinator.ShardMapDelta> observer,
			long fromVersion,
			ShardMapSnapshot snapshot) {

		WatcherContext context = new WatcherContext(observer, fromVersion, System.currentTimeMillis());
		watchers.put(observer, context);
		LOGGER.info("Registered watcher (fromVersion=" + fromVersion + "), total watchers: " + watchers.size());

		// Send initial full state if snapshot is newer than fromVersion
		if (snapshot.getMapVersion() > fromVersion) {
			try {
				var protoState = ProtoConverter.toProto(snapshot);
				var delta = com.danieljhkim.kvdb.proto.coordinator.ShardMapDelta.newBuilder()
						.setNewMapVersion(snapshot.getMapVersion())
						.setFullState(protoState)
						.build();
				observer.onNext(delta);
				LOGGER.fine("Sent initial state to watcher (version=" + snapshot.getMapVersion() + ")");
			} catch (Exception e) {
				LOGGER.log(Level.WARNING, "Failed to send initial state to watcher", e);
				unregisterWatcher(observer);
			}
		}
	}

	/**
	 * Unregisters a watcher (called on client disconnect or error).
	 */
	public void unregisterWatcher(
			StreamObserver<com.danieljhkim.kvdb.proto.coordinator.ShardMapDelta> observer) {
		WatcherContext removed = watchers.remove(observer);
		if (removed != null) {
			LOGGER.info("Unregistered watcher, remaining watchers: " + watchers.size());
		}
	}

	/**
	 * Receives delta events from the Raft state machine and broadcasts to all
	 * watchers.
	 * This is the Consumer<ShardMapDelta> implementation.
	 */
	@Override
	public void accept(ShardMapDelta delta) {
		if (watchers.isEmpty()) {
			return;
		}

		var protoDelta = ProtoConverter.toProto(delta);
		broadcastDelta(protoDelta);
	}

	/**
	 * Broadcasts a proto delta to all registered watchers.
	 */
	private void broadcastDelta(com.danieljhkim.kvdb.proto.coordinator.ShardMapDelta delta) {
		for (var entry : watchers.entrySet()) {
			try {
				entry.getKey().onNext(delta);
			} catch (Exception e) {
				LOGGER.log(Level.WARNING, "Failed to send delta to watcher, removing", e);
				watchers.remove(entry.getKey());
			}
		}
		LOGGER.fine("Broadcast delta (version=" + delta.getNewMapVersion() + ") to " + watchers.size() + " watchers");
	}

	/**
	 * Sends heartbeat pings to all watchers to keep streams alive.
	 */
	private void sendHeartbeats() {
		if (watchers.isEmpty()) {
			return;
		}

		// Send an empty delta as a heartbeat (version 0 indicates heartbeat)
		var heartbeat = com.danieljhkim.kvdb.proto.coordinator.ShardMapDelta.newBuilder()
				.setNewMapVersion(0) // 0 indicates heartbeat, not a real update
				.build();

		for (var entry : watchers.entrySet()) {
			try {
				entry.getKey().onNext(heartbeat);
			} catch (Exception e) {
				LOGGER.log(Level.WARNING, "Heartbeat failed for watcher, removing", e);
				watchers.remove(entry.getKey());
			}
		}
		LOGGER.fine("Sent heartbeat to " + watchers.size() + " watchers");
	}

	/**
	 * Returns the number of active watchers.
	 */
	public int getWatcherCount() {
		return watchers.size();
	}
}
