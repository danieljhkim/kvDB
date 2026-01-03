package com.danieljhkim.kvdb.kvgateway.client;

import com.danieljhkim.kvdb.proto.coordinator.CoordinatorGrpc;
import com.danieljhkim.kvdb.proto.coordinator.ShardMapDelta;
import com.danieljhkim.kvdb.proto.coordinator.WatchShardMapRequest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Async streaming client for WatchShardMap RPC. Subscribes to coordinator shard map deltas and notifies consumers of
 * updates. Includes automatic reconnection with exponential backoff on stream failures.
 */
public class WatchShardMapClient {

    private static final Logger logger = LoggerFactory.getLogger(WatchShardMapClient.class);

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

    private final ExecutorService consumerExecutor;
    private final AtomicReference<ClientCallStreamObserver<WatchShardMapRequest>> activeCall =
            new AtomicReference<>(null);

    /**
     * Creates a new WatchShardMapClient.
     *
     * @param host coordinator host
     * @param port coordinator port
     * @param deltaConsumer consumer to receive delta updates
     */
    public WatchShardMapClient(String host, int port, Consumer<ShardMapDelta> deltaConsumer) {
        this.channel =
                ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        this.asyncStub = CoordinatorGrpc.newStub(channel);
        this.deltaConsumer = deltaConsumer;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "watch-shard-map-reconnect");
            t.setDaemon(true);
            return t;
        });

        this.consumerExecutor = new ThreadPoolExecutor(
                1,
                1,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(1024),
                r -> {
                    Thread t = new Thread(r, "watch-shard-map-consumer");
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.CallerRunsPolicy());

        logger.info("WatchShardMapClient created for {}:{}", host, port);
    }

    /**
     * Starts the streaming subscription. Will automatically reconnect on failures with exponential backoff.
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            logger.info("Starting WatchShardMapClient");
            connect();
        }
    }

    /**
     * Starts the streaming subscription with an initial version. Use this to resume from a known version after gateway
     * restart.
     *
     * @param fromVersion the version to start streaming from
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
            logger.info("Stopping WatchShardMapClient");
            connected.set(false);
            ClientCallStreamObserver<WatchShardMapRequest> call = activeCall.getAndSet(null);
            if (call != null) {
                try {
                    call.cancel("WatchShardMapClient stopped", null);
                } catch (Exception e) {
                    logger.debug("Error cancelling active WatchShardMap call", e);
                }
            }
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                scheduler.shutdownNow();
            }

            consumerExecutor.shutdown();
            try {
                if (!consumerExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    consumerExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                consumerExecutor.shutdownNow();
            }
        }
    }

    /**
     * Shuts down the client and releases resources.
     */
    public void shutdown() {
        stop();
        logger.info("Shutting down WatchShardMapClient channel");
        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.warn("Interrupted while shutting down channel");
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
        if (channel.isShutdown() || channel.isTerminated()) {
            logger.debug("Not connecting - channel is shut down");
            return;
        }

        long fromVersion = currentVersion.get();
        logger.info("Connecting to WatchShardMap stream (fromVersion={})", fromVersion);

        WatchShardMapRequest request =
                WatchShardMapRequest.newBuilder().setFromVersion(fromVersion).build();

        asyncStub.watchShardMap(request, new ClientResponseObserver<WatchShardMapRequest, ShardMapDelta>() {
            @Override
            public void beforeStart(ClientCallStreamObserver<WatchShardMapRequest> requestStream) {
                // Capture the call so we can cancel it on stop().
                ClientCallStreamObserver<WatchShardMapRequest> prev = activeCall.getAndSet(requestStream);
                if (prev != null) {
                    try {
                        prev.cancel("Replaced by new WatchShardMap call", null);
                    } catch (Exception e) {
                        logger.debug("Error cancelling previous WatchShardMap call", e);
                    }
                }
            }

            @Override
            public void onNext(ShardMapDelta delta) {
                handleDelta(delta);
            }

            @Override
            public void onError(Throwable t) {
                activeCall.set(null);
                handleError(t);
            }

            @Override
            public void onCompleted() {
                activeCall.set(null);
                handleCompleted();
            }
        });
    }

    /**
     * Handles incoming delta updates from the stream.
     */
    private void handleDelta(ShardMapDelta delta) {
        long newVersion = delta.getNewMapVersion();

        // Version 0 is a heartbeat (liveness), not a real update
        if (!connected.get()) {
            connected.set(true);
            consecutiveFailures.set(0);
            logger.info("WatchShardMap stream connected");
        }

        if (newVersion == 0) {
            logger.debug("Received heartbeat from coordinator");
            return;
        }

        // Update our tracked version
        if (newVersion > currentVersion.get()) {
            currentVersion.set(newVersion);
            logger.debug(
                    "Received delta: version={}, changedShards={}, changedNodes={}, hasFullState={}",
                    newVersion,
                    delta.getChangedShardsCount(),
                    delta.getChangedNodesCount(),
                    delta.hasFullState());

            // Notify consumer off the gRPC callback thread
            try {
                consumerExecutor.execute(() -> {
                    try {
                        deltaConsumer.accept(delta);
                    } catch (Exception e) {
                        logger.warn("Error processing delta in consumer", e);
                    }
                });
            } catch (Exception e) {
                logger.warn("Failed to dispatch delta to consumer executor", e);
            }
        } else {
            logger.debug("Ignoring stale delta (version={}, current={})", newVersion, currentVersion.get());
        }
    }

    /**
     * Handles stream errors with reconnection.
     */
    private void handleError(Throwable t) {
        connected.set(false);
        int failures = consecutiveFailures.incrementAndGet();
        logger.warn("WatchShardMap stream error (failures={})", failures, t);

        scheduleReconnect(failures);
    }

    /**
     * Handles stream completion (server closed stream).
     */
    private void handleCompleted() {
        connected.set(false);
        logger.info("WatchShardMap stream completed by server");

        // Schedule immediate reconnect (server completed normally)
        scheduleReconnect(0);
    }

    /**
     * Schedules a reconnection attempt with exponential backoff.
     */
    private void scheduleReconnect(int failureCount) {
        if (!running.get() || scheduler.isShutdown()) {
            logger.debug("Not scheduling reconnect - client stopped or scheduler shut down");
            return;
        }

        long backoffMs = calculateBackoff(failureCount);
        logger.info("Scheduling reconnect in {}ms", backoffMs);

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
