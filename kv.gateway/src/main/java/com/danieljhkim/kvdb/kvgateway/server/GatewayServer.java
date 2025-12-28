package com.danieljhkim.kvdb.kvgateway.server;

import com.danieljhkim.kvdb.kvgateway.cache.ShardMapCache;
import com.danieljhkim.kvdb.kvgateway.client.CoordinatorClient;
import com.danieljhkim.kvdb.kvgateway.client.NodeConnectionPool;
import com.danieljhkim.kvdb.kvgateway.service.KvGatewayServiceImpl;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import lombok.Getter;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * gRPC server for the KvGateway service.
 * Manages the lifecycle of the gateway components.
 */
public class GatewayServer {

	private static final Logger LOGGER = Logger.getLogger(GatewayServer.class.getName());

	/**
	 * -- GETTER --
	 * Gets the port the server is running on.
	 */
	@Getter
	private final int port;
	private final Server grpcServer;
	private final CoordinatorClient coordinatorClient;
	private final NodeConnectionPool nodePool;
	/**
	 * -- GETTER --
	 * Gets the shard map cache for external access (e.g., for refresh triggers).
	 */
	@Getter
	private final ShardMapCache shardMapCache;

	public GatewayServer(int port, String coordinatorHost, int coordinatorPort) {
		this.port = port;

		// Initialize components
		this.coordinatorClient = new CoordinatorClient(coordinatorHost, coordinatorPort);
		this.nodePool = new NodeConnectionPool();
		this.shardMapCache = new ShardMapCache(coordinatorClient);

		// Create the service
		KvGatewayServiceImpl gatewayService = new KvGatewayServiceImpl(shardMapCache, nodePool);

		// Build the gRPC server
		this.grpcServer = NettyServerBuilder.forPort(port)
				.addService(gatewayService)
				.build();

		LOGGER.info("GatewayServer initialized on port " + port
				+ ", coordinator: " + coordinatorHost + ":" + coordinatorPort);
	}

	/**
	 * Starts the gateway server.
	 * Fetches the initial shard map and starts the gRPC server.
	 */
	public void start() throws IOException {
		LOGGER.info("Starting GatewayServer...");

		// Fetch initial shard map from coordinator
		try {
			boolean refreshed = shardMapCache.refresh();
			if (refreshed) {
				LOGGER.info("Initial shard map loaded successfully, version: " + shardMapCache.getMapVersion());
			} else {
				LOGGER.warning("Could not load initial shard map from coordinator. "
						+ "Gateway will start but may fail requests until coordinator is available.");
			}
		} catch (Exception e) {
			LOGGER.log(Level.WARNING, "Failed to load initial shard map, continuing anyway", e);
		}

		// Start gRPC server
		grpcServer.start();
		LOGGER.info("GatewayServer started on port " + port);
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
		LOGGER.info("Shutting down GatewayServer...");

		// Shutdown gRPC server
		grpcServer.shutdown();
		if (!grpcServer.awaitTermination(10, TimeUnit.SECONDS)) {
			LOGGER.warning("gRPC server did not terminate in time, forcing shutdown");
			grpcServer.shutdownNow();
		}

		// Close node connections
		nodePool.closeAll();

		// Close coordinator client
		coordinatorClient.shutdown();

		LOGGER.info("GatewayServer shutdown complete");
	}

}
