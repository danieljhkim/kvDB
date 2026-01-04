package com.danieljhkim.kvdb.kvgateway.server;

import com.danieljhkim.kvdb.kvcommon.cache.ShardMapCache;
import com.danieljhkim.kvdb.kvcommon.config.AppConfig;
import com.danieljhkim.kvdb.kvcommon.grpc.CoordinatorClientManager;
import com.danieljhkim.kvdb.kvcommon.grpc.GlobalExceptionInterceptor;
import com.danieljhkim.kvdb.kvcommon.grpc.WatchShardMapClient;
import com.danieljhkim.kvdb.kvgateway.cache.NodeFailureTracker;
import com.danieljhkim.kvdb.kvgateway.client.NodeConnectionPool;
import com.danieljhkim.kvdb.kvgateway.retry.RequestExecutor;
import com.danieljhkim.kvdb.kvgateway.retry.RetryPolicy;
import com.danieljhkim.kvdb.kvgateway.service.KvGatewayServiceImpl;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * gRPC server for the KvGateway service. Manages the lifecycle of the gateway components including streaming shard map
 * updates from the coordinator.
 */
public class GatewayServer {

    private static final Logger logger = LoggerFactory.getLogger(GatewayServer.class);

    @Getter
    private final int port;

    private final Server grpcServer;
    private final CoordinatorClientManager coordinatorClientManager;
    private final NodeConnectionPool nodePool;
    private final WatchShardMapClient watchShardMapClient;

    @Getter
    private final ShardMapCache shardMapCache;

    public GatewayServer(AppConfig appConfig) { // Get gateway configuration with defaults
        AppConfig.GatewayConfig gatewayConfig = appConfig.getGateway();
        if (gatewayConfig == null) {
            gatewayConfig = new AppConfig.GatewayConfig();
        }

        this.port = gatewayConfig.getPort();

        // Initialize core components
        this.coordinatorClientManager = new CoordinatorClientManager(appConfig);
        this.nodePool = new NodeConnectionPool();
        this.shardMapCache = new ShardMapCache();

        // Initialize retry infrastructure
        NodeFailureTracker failureTracker = new NodeFailureTracker();
        RetryPolicy retryPolicy = RetryPolicy.defaults();
        RequestExecutor requestExecutor = new RequestExecutor(nodePool, failureTracker, retryPolicy, 5000);

        // Create streaming client for real-time shard map updates
        this.watchShardMapClient = new WatchShardMapClient(shardMapCache, coordinatorClientManager);

        // Create the service with retry-enabled executor
        KvGatewayServiceImpl gatewayService = new KvGatewayServiceImpl(shardMapCache, requestExecutor);
        ServerServiceDefinition interceptedService =
                ServerInterceptors.intercept(gatewayService, new GlobalExceptionInterceptor());

        this.grpcServer =
                NettyServerBuilder.forPort(port).addService(interceptedService).build();

        logger.info("GatewayServer initialized on port {}", port);
    }

    /**
     * Starts the gateway server. Fetches the initial shard map, starts streaming updates, and starts the gRPC server.
     */
    public void start() throws IOException {
        logger.info("Starting GatewayServer...");

        // Fetch initial shard map from coordinator
        long initialVersion = 0;
        try {
            var state = coordinatorClientManager.fetchShardMap(0);
            if (state != null) {
                shardMapCache.refreshFromFullState(state);
                initialVersion = shardMapCache.getMapVersion();
                logger.info("Initial shard map loaded successfully, version: {}", initialVersion);
            } else {
                logger.warn("Could not load initial shard map from coordinator. "
                        + "Gateway will start but may fail requests until coordinator is available.");
            }
        } catch (Exception e) {
            logger.warn("Failed to load initial shard map, continuing anyway", e);
        }
        try {
            watchShardMapClient.start(initialVersion);
            logger.info("Started WatchShardMap streaming client from version {}", initialVersion);
        } catch (Exception e) {
            logger.warn("Failed to start WatchShardMap client, will rely on polling", e);
        }
        grpcServer.start();
        logger.info("GatewayServer started on port {}", port);
    }

    /**
     * Blocks until the server shuts down.
     */
    public void awaitTermination() throws InterruptedException {
        grpcServer.awaitTermination();
    }

    /**
     * Shuts down the gateway server gracefully.
     */
    public void shutdown() throws InterruptedException {
        logger.info("Shutting down GatewayServer...");
        watchShardMapClient.shutdown();
        grpcServer.shutdown();
        if (!grpcServer.awaitTermination(10, TimeUnit.SECONDS)) {
            logger.warn("gRPC server did not terminate in time, forcing shutdown");
            grpcServer.shutdownNow();
        }
        nodePool.closeAll();
        coordinatorClientManager.shutdown();
        logger.info("GatewayServer shutdown complete");
    }

    /**
     * Checks if the streaming client is connected to the coordinator.
     *
     * @return true if connected to coordinator streaming
     */
    public boolean isStreamingConnected() {
        return watchShardMapClient.isConnected();
    }
}
