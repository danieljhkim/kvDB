package com.danieljhkim.kvdb.kvcommon.grpc;

import com.danieljhkim.kvdb.kvcommon.cache.ShardMapCache;
import com.danieljhkim.kvdb.proto.coordinator.ShardMapDelta;
import com.danieljhkim.kvdb.proto.coordinator.WatchShardMapRequest;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Node-side async streaming client for WatchShardMap. Keeps a local cache up to date for per-shard leader/replica
 * checks.
 */
public class WatchShardMapClient {

    private static final Logger logger = LoggerFactory.getLogger(WatchShardMapClient.class);

    private static final long INITIAL_BACKOFF_MS = 500;
    private static final long MAX_BACKOFF_MS = 3000;
    private static final double BACKOFF_MULTIPLIER = 2.0;

    private final CoordinatorClientManager clientManager;
    private final ShardMapCache shardMapCache;
    private final Consumer<ShardMapDelta> deltaConsumer;
    private final ScheduledExecutorService scheduler;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final AtomicLong currentVersion = new AtomicLong(0);
    private final AtomicInteger consecutiveFailures = new AtomicInteger(0);

    public WatchShardMapClient(ShardMapCache shardMapCache, CoordinatorClientManager clientManager) {

        io.grpc.internal.DnsNameResolverProvider provider = new io.grpc.internal.DnsNameResolverProvider();
        io.grpc.NameResolverRegistry.getDefaultRegistry().register(provider);
        this.clientManager = clientManager;
        this.shardMapCache = shardMapCache;
        this.deltaConsumer = shardMapCache;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "node-watch-shard-map-reconnect");
            t.setDaemon(true);
            return t;
        });
    }

    public void start(long fromVersion) {
        currentVersion.set(fromVersion);
        if (running.compareAndSet(false, true)) {
            connect();
        }
    }

    public void shutdown() {
        if (running.compareAndSet(true, false)) {
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

    public long getCurrentVersion() {
        return currentVersion.get();
    }

    public boolean isConnected() {
        return connected.get();
    }

    private void connect() {
        if (!running.get()) {
            return;
        }
        CoordinatorClient coordinatorClient = clientManager.getLeaderClient();
        if (coordinatorClient == null || coordinatorClient.getAsyncStub() == null) {
            logger.warn("No known coordinator leader to watch shard map from");
            scheduleReconnect(consecutiveFailures.incrementAndGet());
            return;
        }

        long fromVersion = currentVersion.get();
        logger.info(
                "Connecting to WatchShardMap stream on {} (fromVersion={})",
                coordinatorClient.getAddress(),
                fromVersion);

        WatchShardMapRequest request =
                WatchShardMapRequest.newBuilder().setFromVersion(fromVersion).build();

        coordinatorClient.getAsyncStub().watchShardMap(request, new StreamObserver<ShardMapDelta>() {
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

    private void refreshShardMapIfPossible() {
        try {
            long current = shardMapCache.getMapVersion();
            var state = clientManager.fetchShardMap(current);
            if (state != null) {
                shardMapCache.refreshFromFullState(state);
            }
        } catch (Exception e) {
            logger.debug("Initial shard map fetch failed (continuing)", e);
        }
    }

    @SuppressWarnings("all")
    private void handleDelta(ShardMapDelta delta) {
        long newVersion = delta.getNewMapVersion();

        // Version 0 without fullState is a heartbeat - just acknowledge connection
        if (newVersion == 0 && !delta.hasFullState()) {
            if (!connected.get()) {
                connected.set(true);
                consecutiveFailures.set(0);
                logger.debug("WatchShardMap stream connected (received heartbeat)");
            }
            logger.trace("Received heartbeat from coordinator");
            return;
        }

        logger.debug("Received shard map delta: newVersion={}, hasFullState={}", newVersion, delta.hasFullState());

        // Mark as connected
        if (!connected.get()) {
            connected.set(true);
            consecutiveFailures.set(0);
            logger.info("WatchShardMap stream connected, received version {}", newVersion);
        }

        // Apply the update if it's newer
        if (newVersion > currentVersion.get()) {
            logger.info(
                    "Applying shard map update: {} -> {} (shards={}, nodes={})",
                    currentVersion.get(),
                    newVersion,
                    delta.hasFullState() ? delta.getFullState().getShardsCount() : "N/A",
                    delta.hasFullState() ? delta.getFullState().getNodesCount() : "N/A");
            currentVersion.set(newVersion);
            try {
                deltaConsumer.accept(delta);
            } catch (Exception e) {
                logger.warn("Error applying shard map delta", e);
            }

            // If this was an incremental update without full state, trigger a full refresh
            if (!delta.hasFullState()
                    && (!delta.getChangedShardsList().isEmpty()
                            || !delta.getChangedNodesList().isEmpty())) {
                try {
                    refreshShardMapIfPossible();
                } catch (Exception e) {
                    logger.warn("Error handling incremental delta", e);
                }
            }
        }
    }

    private void handleError(Throwable t) {
        connected.set(false);
        int failures = consecutiveFailures.incrementAndGet();

        // Check if this is a "not leader" error - clear cached leader and retry quickly
        if (t instanceof io.grpc.StatusRuntimeException sre) {
            io.grpc.Status.Code code = sre.getStatus().getCode();
            if (code == io.grpc.Status.Code.FAILED_PRECONDITION || code == io.grpc.Status.Code.UNAVAILABLE) {
                logger.info("WatchShardMap stream got {}, will rediscover leader", code);
                clientManager.clearCachedLeader();
                scheduleReconnect(0); // Retry immediately
                return;
            }
        }

        logger.warn("WatchShardMap stream error (failures={})", failures, t);
        scheduleReconnect(failures);
    }

    private void handleCompleted() {
        connected.set(false);
        scheduleReconnect(0);
    }

    private void scheduleReconnect(int failureCount) {
        if (!running.get()) {
            return;
        }
        long backoffMs = calculateBackoff(failureCount);
        scheduler.schedule(this::connect, backoffMs, TimeUnit.MILLISECONDS);
    }

    private long calculateBackoff(int failureCount) {
        if (failureCount <= 0) {
            return 0;
        }
        double backoff = INITIAL_BACKOFF_MS * Math.pow(BACKOFF_MULTIPLIER, failureCount - 1);
        long cappedBackoff = (long) Math.min(backoff, MAX_BACKOFF_MS);
        long jitter = (long) (cappedBackoff * Math.random() * 0.25);
        return cappedBackoff + jitter;
    }
}
