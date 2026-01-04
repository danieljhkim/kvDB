package com.danieljhkim.kvdb.kvclustercoordinator.service;

import com.danieljhkim.kvdb.kvclustercoordinator.converter.ProtoConverter;
import com.danieljhkim.kvdb.kvclustercoordinator.state.ShardMapDelta;
import com.danieljhkim.kvdb.kvclustercoordinator.state.ShardMapSnapshot;
import io.grpc.stub.StreamObserver;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages active WatchShardMap streaming connections. Converts internal delta events to proto and broadcasts to all
 * watchers.
 */
public class WatcherManager implements Consumer<ShardMapDelta> {

    private static final Logger logger = LoggerFactory.getLogger(WatcherManager.class);
    private static final long HEARTBEAT_INTERVAL_MS = 30_000; // 30 seconds

    /**
     * Context for each watcher connection.
     */
    private record WatcherContext(
            StreamObserver<com.danieljhkim.kvdb.proto.coordinator.ShardMapDelta> observer,
            long fromVersion,
            long registeredAt) {}

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
        logger.info("WatcherManager started with heartbeat interval {}ms", HEARTBEAT_INTERVAL_MS);
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

        closeAllWatchers();
        logger.info("WatcherManager stopped");
    }

    /**
     * Closes all watcher connections with a FAILED_PRECONDITION error to force clients to reconnect.
     * Called when this coordinator steps down from leader.
     */
    public void closeAllWatchers() {
        if (watchers.isEmpty()) {
            return;
        }

        logger.info("Closing {} watchers (no longer leader)", watchers.size());
        for (var entry : watchers.entrySet()) {
            try {
                // Send error to force client to reconnect to new leader
                entry.getKey()
                        .onError(io.grpc.Status.FAILED_PRECONDITION
                                .withDescription("Coordinator stepped down from leader")
                                .asRuntimeException());
            } catch (Exception e) {
                logger.debug("Error closing watcher", e);
            }
        }
        watchers.clear();
    }

    /**
     * Registers a new watcher and sends the initial state if newer than fromVersion.
     *
     * @param observer the gRPC stream observer
     * @param fromVersion the client's current version (0 for full state)
     * @param snapshot the current snapshot to send initially
     */
    public void registerWatcher(
            StreamObserver<com.danieljhkim.kvdb.proto.coordinator.ShardMapDelta> observer,
            long fromVersion,
            ShardMapSnapshot snapshot) {

        WatcherContext context = new WatcherContext(observer, fromVersion, System.currentTimeMillis());
        watchers.put(observer, context);
        logger.info("Registered watcher (fromVersion={}), total watchers: {}", fromVersion, watchers.size());

        // Send initial full state if:
        // 1. Client is requesting from version 0 (needs full state)
        // 2. Snapshot is newer than client's version
        if (fromVersion == 0 || snapshot.getMapVersion() > fromVersion) {
            try {
                var protoState = ProtoConverter.toProto(snapshot);
                var delta = com.danieljhkim.kvdb.proto.coordinator.ShardMapDelta.newBuilder()
                        .setNewMapVersion(snapshot.getMapVersion())
                        .setFullState(protoState)
                        .build();
                observer.onNext(delta);
                logger.info(
                        "Sent initial state to watcher (version={}, shards={}, nodes={})",
                        snapshot.getMapVersion(),
                        snapshot.getShards().size(),
                        snapshot.getNodes().size());
            } catch (Exception e) {
                logger.warn("Failed to send initial state to watcher", e);
                unregisterWatcher(observer);
            }
        } else {
            logger.info(
                    "Watcher fromVersion={} is up to date with snapshot version={}",
                    fromVersion,
                    snapshot.getMapVersion());
        }
    }

    /**
     * Unregisters a watcher (called on client disconnect or error).
     */
    public void unregisterWatcher(StreamObserver<com.danieljhkim.kvdb.proto.coordinator.ShardMapDelta> observer) {
        WatcherContext removed = watchers.remove(observer);
        if (removed != null) {
            logger.info("Unregistered watcher, remaining watchers: {}", watchers.size());
        }
    }

    /**
     * Receives delta events from the Raft state machine and broadcasts to all watchers. This is the
     * Consumer<ShardMapDelta> implementation.
     */
    @Override
    public void accept(ShardMapDelta delta) {
        if (watchers.isEmpty()) {
            logger.debug("No watchers to broadcast delta (version={})", delta.newMapVersion());
            return;
        }

        logger.info("Broadcasting delta (version={}) to {} watchers", delta.newMapVersion(), watchers.size());
        var protoDelta = ProtoConverter.toProto(delta);
        broadcastDelta(protoDelta);
    }

    /**
     * Broadcasts a proto delta to all registered watchers.
     */
    private void broadcastDelta(com.danieljhkim.kvdb.proto.coordinator.ShardMapDelta delta) {
        int successCount = 0;
        for (var entry : watchers.entrySet()) {
            try {
                entry.getKey().onNext(delta);
                successCount++;
            } catch (io.grpc.StatusRuntimeException e) {
                // Handle client disconnections gracefully
                if (e.getStatus().getCode() == io.grpc.Status.Code.CANCELLED) {
                    logger.debug("Watcher cancelled/disconnected, removing");
                } else {
                    logger.warn(
                            "Failed to send delta to watcher ({}), removing",
                            e.getStatus().getCode());
                }
                watchers.remove(entry.getKey());
            } catch (Exception e) {
                logger.warn("Unexpected error sending delta to watcher, removing", e);
                watchers.remove(entry.getKey());
            }
        }
        logger.info(
                "Broadcast delta (version={}) sent to {}/{} watchers",
                delta.getNewMapVersion(),
                successCount,
                watchers.size());
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
                .setNewMapVersion(0)
                .build();

        for (var entry : watchers.entrySet()) {
            try {
                entry.getKey().onNext(heartbeat);
            } catch (io.grpc.StatusRuntimeException e) {
                // Handle client disconnections gracefully
                if (e.getStatus().getCode() == io.grpc.Status.Code.CANCELLED) {
                    logger.debug("Watcher cancelled/disconnected during heartbeat, removing");
                } else {
                    logger.warn(
                            "Heartbeat failed for watcher ({}), removing",
                            e.getStatus().getCode());
                }
                watchers.remove(entry.getKey());
            } catch (Exception e) {
                logger.warn("Unexpected error during heartbeat, removing watcher", e);
                watchers.remove(entry.getKey());
            }
        }
        logger.debug("Sent heartbeat to {} watchers", watchers.size());
    }

    /**
     * Returns the number of active watchers.
     */
    public int getWatcherCount() {
        return watchers.size();
    }
}
