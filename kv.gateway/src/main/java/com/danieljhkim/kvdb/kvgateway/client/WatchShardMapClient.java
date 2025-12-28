package com.danieljhkim.kvdb.kvgateway.client;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.danieljhkim.kvdb.proto.coordinator.CoordinatorGrpc;
import com.danieljhkim.kvdb.proto.coordinator.ShardMapDelta;
import com.danieljhkim.kvdb.proto.coordinator.WatchShardMapRequest;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

/**
 * Async streaming client for WatchShardMap RPC.
 * Subscribes to coordinator shard map deltas and notifies consumers of updates.
 * Includes automatic reconnection with exponential backoff on stream failures.
 */
public class WatchShardMapClient {

	private static final Logger LOGGER = Logger.getLogger(WatchShardMapClient.class.getName());

	// Backoff configuration
	private static final long INITIAL_BACKOFF_MS = 1000;
	private static final long MAX_BACKOFF_MS = 30000;
	private static final double BACKOFF_MULTIPLIER = 2.0;

	private final ManagedChannel channel;
	private final CoordinatorGrpc.CoordinatorStub asyncStub;
	private final Consumer<ShardMapDelta> deltaConsumer;
	private final ScheduledExecutorService scheduler;

	private final AtomicBoolean running = new AtomicBoolean(false);
	private final AtomicBoolean connected = new AtomicBoolean(false);
	private final AtomicLong currentVersion = new AtomicLong(0);
	private final AtomicInteger consecutiveFailures = new AtomicInteger(0);

	/**
	 * Creates a new WatchShardMapClient.
	 *
	 * @param host
	 *            coordinator host
	 * @param port
	 *            coordinator port
	 * @param deltaConsumer
	 *            consumer to receive delta updates
	 */
	public WatchShardMapClient(String host, int port, Consumer<ShardMapDelta> deltaConsumer) {
		// Register DNS resolver
		io.grpc.internal.DnsNameResolverProvider provider = new io.grpc.internal.DnsNameResolverProvider();
		io.grpc.NameResolverRegistry.getDefaultRegistry().register(provider);

		this.channel = ManagedChannelBuilder.forAddress(host, port)
				.usePlaintext()
				.build();
		this.asyncStub = CoordinatorGrpc.newStub(channel);
		this.deltaConsumer = deltaConsumer;
		this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
			Thread t = new Thread(r, "watch-shard-map-reconnect");
			t.setDaemon(true);
			return t;
		});

		LOGGER.info("WatchShardMapClient created for " + host + ":" + port);
	}

	/**
	 * Starts the streaming subscription.
	 * Will automatically reconnect on failures with exponential backoff.
	 */
	public void start() {
		if (running.compareAndSet(false, true)) {
			LOGGER.info("Starting WatchShardMapClient");
			connect();
		}
	}

	/**
	 * Starts the streaming subscription with an initial version.
	 * Use this to resume from a known version after gateway restart.
	 *
	 * @param fromVersion
	 *            the version to start streaming from
	 */
	public void start(long fromVersion) {
		currentVersion.set(fromVersion);
		start();
	}

	/**
	 * Stops the streaming subscription.
	 */
	public void stop() {
		if (running.compareAndSet(true, false)) {
			LOGGER.info("Stopping WatchShardMapClient");
			connected.set(false);
			scheduler.shutdown();
			try {
				if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
					scheduler.shutdownNow();
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				scheduler.shutdownNow();
			}
		}
	}

	/**
	 * Shuts down the client and releases resources.
	 */
	public void shutdown() {
		stop();
		LOGGER.info("Shutting down WatchShardMapClient channel");
		try {
			channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			LOGGER.warning("Interrupted while shutting down channel");
			channel.shutdownNow();
			Thread.currentThread().interrupt();
		}
	}

	/**
	 * Checks if the client is currently connected to the coordinator.
	 */
	public boolean isConnected() {
		return connected.get();
	}

	/**
	 * Gets the current shard map version known to this client.
	 */
	public long getCurrentVersion() {
		return currentVersion.get();
	}

	/**
	 * Establishes the streaming connection to the coordinator.
	 */
	private void connect() {
		if (!running.get()) {
			return;
		}

		long fromVersion = currentVersion.get();
		LOGGER.info("Connecting to WatchShardMap stream (fromVersion=" + fromVersion + ")");

		WatchShardMapRequest request = WatchShardMapRequest.newBuilder()
				.setFromVersion(fromVersion)
				.build();

		asyncStub.watchShardMap(request, new StreamObserver<ShardMapDelta>() {
			@Override
			public void onNext(ShardMapDelta delta) {
				handleDelta(delta);
			}

			@Override
			public void onError(Throwable t) {
				handleError(t);
			}

			@Override
			public void onCompleted() {
				handleCompleted();
			}
		});
	}

	/**
	 * Handles incoming delta updates from the stream.
	 */
	private void handleDelta(ShardMapDelta delta) {
		long newVersion = delta.getNewMapVersion();

		// Version 0 is a heartbeat, not a real update
		if (newVersion == 0) {
			LOGGER.fine("Received heartbeat from coordinator");
			return;
		}

		// Mark as connected on first real message
		if (!connected.get()) {
			connected.set(true);
			consecutiveFailures.set(0);
			LOGGER.info("WatchShardMap stream connected");
		}

		// Update our tracked version
		if (newVersion > currentVersion.get()) {
			currentVersion.set(newVersion);
			LOGGER.fine("Received delta: version=" + newVersion
					+ ", changedShards=" + delta.getChangedShardsCount()
					+ ", changedNodes=" + delta.getChangedNodesCount()
					+ ", hasFullState=" + delta.hasFullState());

			// Notify consumer
			try {
				deltaConsumer.accept(delta);
			} catch (Exception e) {
				LOGGER.log(Level.WARNING, "Error processing delta in consumer", e);
			}
		} else {
			LOGGER.fine("Ignoring stale delta (version=" + newVersion
					+ ", current=" + currentVersion.get() + ")");
		}
	}

	/**
	 * Handles stream errors with reconnection.
	 */
	private void handleError(Throwable t) {
		connected.set(false);
		int failures = consecutiveFailures.incrementAndGet();
		LOGGER.log(Level.WARNING, "WatchShardMap stream error (failures=" + failures + ")", t);

		scheduleReconnect(failures);
	}

	/**
	 * Handles stream completion (server closed stream).
	 */
	private void handleCompleted() {
		connected.set(false);
		LOGGER.info("WatchShardMap stream completed by server");

		// Schedule immediate reconnect (server completed normally)
		scheduleReconnect(0);
	}

	/**
	 * Schedules a reconnection attempt with exponential backoff.
	 */
	private void scheduleReconnect(int failureCount) {
		if (!running.get()) {
			LOGGER.fine("Not scheduling reconnect - client stopped");
			return;
		}

		long backoffMs = calculateBackoff(failureCount);
		LOGGER.info("Scheduling reconnect in " + backoffMs + "ms");

		scheduler.schedule(this::connect, backoffMs, TimeUnit.MILLISECONDS);
	}

	/**
	 * Calculates backoff delay with exponential increase and jitter.
	 */
	private long calculateBackoff(int failureCount) {
		if (failureCount <= 0) {
			return 0; // Immediate reconnect for normal completion
		}

		double backoff = INITIAL_BACKOFF_MS * Math.pow(BACKOFF_MULTIPLIER, failureCount - 1);
		long cappedBackoff = (long) Math.min(backoff, MAX_BACKOFF_MS);

		// Add jitter (0-25% of backoff)
		long jitter = (long) (cappedBackoff * Math.random() * 0.25);
		return cappedBackoff + jitter;
	}
}
