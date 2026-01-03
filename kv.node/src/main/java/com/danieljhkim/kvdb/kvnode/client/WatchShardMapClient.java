package com.danieljhkim.kvdb.kvnode.client;

import com.danieljhkim.kvdb.proto.coordinator.CoordinatorGrpc;
import com.danieljhkim.kvdb.proto.coordinator.ShardMapDelta;
import com.danieljhkim.kvdb.proto.coordinator.WatchShardMapRequest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
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

    private static final long INITIAL_BACKOFF_MS = 1000;
    private static final long MAX_BACKOFF_MS = 30000;
    private static final double BACKOFF_MULTIPLIER = 2.0;

    private final ManagedChannel channel;
    private final CoordinatorGrpc.CoordinatorStub asyncStub;
    private final Consumer<ShardMapDelta> deltaConsumer;
    private final Runnable onIncrementalDelta;
    private final ScheduledExecutorService scheduler;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final AtomicLong currentVersion = new AtomicLong(0);
    private final AtomicInteger consecutiveFailures = new AtomicInteger(0);

    public WatchShardMapClient(
            String host, int port, Consumer<ShardMapDelta> deltaConsumer, Runnable onIncrementalDelta) {
        io.grpc.internal.DnsNameResolverProvider provider = new io.grpc.internal.DnsNameResolverProvider();
        io.grpc.NameResolverRegistry.getDefaultRegistry().register(provider);

        this.channel =
                ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        this.asyncStub = CoordinatorGrpc.newStub(channel);
        this.deltaConsumer = deltaConsumer;
        this.onIncrementalDelta = onIncrementalDelta;
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
        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            channel.shutdownNow();
        }
    }

    public long getCurrentVersion() {
        return currentVersion.get();
    }

    private void connect() {
        if (!running.get()) {
            return;
        }

        long fromVersion = currentVersion.get();
        WatchShardMapRequest request =
                WatchShardMapRequest.newBuilder().setFromVersion(fromVersion).build();

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

    private void handleDelta(ShardMapDelta delta) {
        long newVersion = delta.getNewMapVersion();
        if (newVersion == 0) {
            return;
        }

        if (!connected.get()) {
            connected.set(true);
            consecutiveFailures.set(0);
        }

        if (newVersion > currentVersion.get()) {
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
                    onIncrementalDelta.run();
                } catch (Exception e) {
                    logger.warn("Error handling incremental delta", e);
                }
            }
        }
    }

    private void handleError(Throwable t) {
        connected.set(false);
        int failures = consecutiveFailures.incrementAndGet();
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
