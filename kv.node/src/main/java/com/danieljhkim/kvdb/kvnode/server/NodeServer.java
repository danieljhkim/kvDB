package com.danieljhkim.kvdb.kvnode.server;

import com.danieljhkim.kvdb.kvcommon.config.AppConfig;
import com.danieljhkim.kvdb.kvcommon.grpc.GlobalExceptionInterceptor;
import com.danieljhkim.kvdb.kvnode.cache.ShardMapCache;
import com.danieljhkim.kvdb.kvnode.client.CoordinatorShardMapClient;
import com.danieljhkim.kvdb.kvnode.client.ReplicaWriteClient;
import com.danieljhkim.kvdb.kvnode.client.WatchShardMapClient;
import com.danieljhkim.kvdb.kvnode.service.KVServiceImpl;
import com.danieljhkim.kvdb.kvnode.storage.ShardStoreRegistry;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeServer {
    private static final Logger logger = LoggerFactory.getLogger(NodeServer.class);

    private final Server server;
    private final ShardMapCache shardMapCache;
    private final CoordinatorShardMapClient coordinatorClient;
    private final WatchShardMapClient watchShardMapClient;
    private final ShardStoreRegistry shardStores;
    private final ReplicaWriteClient replicaWriteClient;

    public NodeServer(String nodeId, AppConfig appConfig) {
        // Find this node's configuration
        AppConfig.NodeConfig thisNode = findNodeConfig(nodeId, appConfig);
        if (thisNode == null) {
            throw new IllegalArgumentException("Node configuration not found for nodeId: " + nodeId);
        }

        // Get coordinator configuration
        String coordinatorHost = "localhost";
        int coordinatorPort = 9000;
        if (appConfig.getStorageNodes() != null && appConfig.getStorageNodes().getCoordinator() != null) {
            coordinatorHost = appConfig.getStorageNodes().getCoordinator().getHost();
            coordinatorPort = appConfig.getStorageNodes().getCoordinator().getPort();
        }

        this.coordinatorClient = new CoordinatorShardMapClient(coordinatorHost, coordinatorPort);
        this.shardMapCache = new ShardMapCache();

        // Get persistence configuration with defaults
        AppConfig.PersistenceConfig persistenceConfig = appConfig.getPersistence();
        if (persistenceConfig == null) {
            persistenceConfig = new AppConfig.PersistenceConfig();
        }

        String baseDir = thisNode.getDataDir();
        String snapshotFileName = persistenceConfig.getSnapshotFileName();
        String walFileName = persistenceConfig.getWalFileName();
        int flushInterval = persistenceConfig.getAutoFlushIntervalMs();
        boolean enableAutoFlush = persistenceConfig.isEnableAutoFlush();

        // Get replication configuration with defaults
        AppConfig.ReplicationConfig replicationConfig = appConfig.getReplication();
        long replicationTimeoutMs = replicationConfig != null ? replicationConfig.getTimeoutMs() : 500;

        this.shardStores =
                new ShardStoreRegistry(baseDir, snapshotFileName, walFileName, flushInterval, enableAutoFlush);
        this.replicaWriteClient = new ReplicaWriteClient(Duration.ofMillis(replicationTimeoutMs));

        this.watchShardMapClient = new WatchShardMapClient(
                coordinatorHost,
                coordinatorPort,
                shardMapCache,
                () -> Thread.startVirtualThread(this::refreshShardMapIfPossible));

        KVServiceImpl kvservice = new KVServiceImpl(
                nodeId, shardMapCache, shardStores, replicaWriteClient, Duration.ofMillis(replicationTimeoutMs));
        ServerServiceDefinition interceptedService =
                ServerInterceptors.intercept(kvservice, new GlobalExceptionInterceptor());

        this.server = NettyServerBuilder.forPort(thisNode.getPort())
                .addService(interceptedService)
                .build();

        logger.info("Initialized NodeServer: nodeId={}, port={}, dataDir={}", nodeId, thisNode.getPort(), baseDir);
    }

    private AppConfig.NodeConfig findNodeConfig(String nodeId, AppConfig appConfig) {
        if (appConfig.getStorageNodes() == null || appConfig.getStorageNodes().getNodes() == null) {
            return null;
        }

        return appConfig.getStorageNodes().getNodes().stream()
                .filter(node -> nodeId.equals(node.getId()))
                .findFirst()
                .orElse(null);
    }

    public void start() throws IOException, InterruptedException {
        // Best-effort initial shard map fetch before accepting writes
        refreshShardMapIfPossible();
        watchShardMapClient.start(shardMapCache.getMapVersion());

        server.start();
        server.awaitTermination();
    }

    public void shutdown() throws InterruptedException {
        try {
            watchShardMapClient.shutdown();
        } catch (Exception e) {
            logger.warn("Failed to shutdown WatchShardMapClient", e);
        }
        try {
            coordinatorClient.shutdown();
        } catch (Exception e) {
            logger.warn("Failed to shutdown CoordinatorShardMapClient", e);
        }
        try {
            replicaWriteClient.shutdown();
        } catch (Exception e) {
            logger.warn("Failed to shutdown ReplicaWriteClient", e);
        }
        try {
            shardStores.shutdown();
        } catch (Exception e) {
            logger.warn("Failed to shutdown shard stores", e);
        }

        if (server != null) {
            server.shutdown().awaitTermination(3, TimeUnit.SECONDS);
            logger.info("NodeServer stopped");
        }
    }

    private void refreshShardMapIfPossible() {
        try {
            long current = shardMapCache.getMapVersion();
            var state = coordinatorClient.fetchShardMap(current);
            if (state != null) {
                shardMapCache.refreshFromFullState(state);
            }
        } catch (Exception e) {
            logger.debug("Initial shard map fetch failed (continuing)", e);
        }
    }
}
