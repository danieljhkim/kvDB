package com.danieljhkim.kvdb.kvgateway.server;

import com.danieljhkim.kvdb.kvcommon.grpc.GlobalExceptionInterceptor;
import com.danieljhkim.kvdb.kvgateway.cache.NodeFailureTracker;
import com.danieljhkim.kvdb.kvgateway.cache.ShardMapCache;
import com.danieljhkim.kvdb.kvgateway.cache.ShardRoutingFailureTracker;
import com.danieljhkim.kvdb.kvgateway.client.CoordinatorClient;
import com.danieljhkim.kvdb.kvgateway.client.NodeConnectionPool;
import com.danieljhkim.kvdb.kvgateway.client.WatchShardMapClient;
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
    private final CoordinatorClient coordinatorClient;
    private final NodeConnectionPool nodePool;
    private final WatchShardMapClient watchShardMapClient;
    private final NodeFailureTracker failureTracker;
    private final ShardRoutingFailureTracker shardRoutingFailureTracker;
    private final RequestExecutor requestExecutor;

    @Getter
    private final ShardMapCache shardMapCache;

    public GatewayServer(int port, String coordinatorHost, int coordinatorPort) {
        this.port = port;

        // Initialize core components
        this.coordinatorClient = new CoordinatorClient(coordinatorHost, coordinatorPort);
        this.nodePool = new NodeConnectionPool();
        this.shardMapCache = new ShardMapCache(coordinatorClient);

        // Initialize retry infrastructure
        this.failureTracker = new NodeFailureTracker();
        this.shardRoutingFailureTracker = new ShardRoutingFailureTracker();
        RetryPolicy retryPolicy = RetryPolicy.defaults();
        this.requestExecutor = new RequestExecutor(
                shardMapCache, nodePool, failureTracker, shardRoutingFailureTracker, retryPolicy, 5000);

        // Create streaming client for real-time shard map updates
        // The ShardMapCache implements Consumer<ShardMapDelta> to receive updates
        this.watchShardMapClient = new WatchShardMapClient(coordinatorHost, coordinatorPort, shardMapCache);

        // Create the service with retry-enabled executor
        KvGatewayServiceImpl gatewayService = new KvGatewayServiceImpl(shardMapCache, requestExecutor);
        ServerServiceDefinition interceptedService =
                ServerInterceptors.intercept(gatewayService, new GlobalExceptionInterceptor());

        // Build the gRPC server
        this.grpcServer =
                NettyServerBuilder.forPort(port).addService(interceptedService).build();

        logger.info("GatewayServer initialized on port {}, coordinator: {}:{}", port, coordinatorHost, coordinatorPort);
    }

    /**
     * Starts the gateway server. Fetches the initial shard map, starts streaming updates, and starts the gRPC server.
     */
    public void start() throws IOException {
        logger.info("Starting GatewayServer...");

        // Fetch initial shard map from coordinator via polling
        long initialVersion = 0;
        try {
            boolean refreshed = shardMapCache.refresh();
            if (refreshed) {
                initialVersion = shardMapCache.getMapVersion();
                logger.info("Initial shard map loaded successfully, version: {}", initialVersion);
            } else {
                logger.warn("Could not load initial shard map from coordinator. "
                        + "Gateway will start but may fail requests until coordinator is available.");
            }
        } catch (Exception e) {
            logger.warn("Failed to load initial shard map, continuing anyway", e);
        }

        // Start streaming client for real-time updates
        // Pass the initial version so we don't re-fetch what we already have
        try {
            watchShardMapClient.start(initialVersion);
            logger.info("Started WatchShardMap streaming client from version {}", initialVersion);
        } catch (Exception e) {
            logger.warn("Failed to start WatchShardMap client, will rely on polling", e);
        }

        // Start gRPC server
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

        // Stop streaming client first (prevents new updates during shutdown)
        watchShardMapClient.shutdown();

        // Shutdown gRPC server
        grpcServer.shutdown();
        if (!grpcServer.awaitTermination(10, TimeUnit.SECONDS)) {
            logger.warn("gRPC server did not terminate in time, forcing shutdown");
            grpcServer.shutdownNow();
        }

        // Close node connections
        nodePool.closeAll();

        // Close coordinator client
        coordinatorClient.shutdown();

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
