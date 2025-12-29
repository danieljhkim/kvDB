package com.danieljhkim.kvdb.kvgateway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.danieljhkim.kvdb.kvcommon.config.SystemConfig;
import com.danieljhkim.kvdb.kvgateway.server.GatewayServer;

/**
 * Main entry point for the KvGateway gRPC server.
 *
 * <p>
 * Usage: java -jar kv-gateway.jar [port]
 *
 * <p>
 * The gateway connects to the coordinator to fetch the shard map,
 * then routes client requests to the appropriate storage nodes.
 */
public class GatewayApplication {

	private static final Logger logger = LoggerFactory.getLogger(GatewayApplication.class);
	private static final SystemConfig CONFIG = SystemConfig.getInstance("gateway");

	private static final int DEFAULT_PORT = 7000;
	private static final String DEFAULT_COORDINATOR_HOST = "localhost";
	private static final int DEFAULT_COORDINATOR_PORT = 9000;

	public static void main(String[] args) {
		logger.info("Starting KvGateway...");

		// Resolve gateway port
		int port = resolvePort(args);

		// Resolve coordinator connection
		String coordinatorHost = CONFIG.getProperty(
				"kvdb.gateway.coordinator.host", DEFAULT_COORDINATOR_HOST);
		int coordinatorPort = parsePort(
				CONFIG.getProperty(
						"kvdb.gateway.coordinator.port", String.valueOf(DEFAULT_COORDINATOR_PORT)),
				DEFAULT_COORDINATOR_PORT,
				"coordinator config");

		try {
			logger.info("Gateway will connect to coordinator at {}:{}", coordinatorHost, coordinatorPort);

			GatewayServer gatewayServer = new GatewayServer(port, coordinatorHost, coordinatorPort);

			// Add shutdown hook
			Runtime.getRuntime().addShutdownHook(new Thread(() -> {
				logger.info("Shutting down KvGateway...");
				try {
					gatewayServer.shutdown();
				} catch (Exception e) {
					logger.error("Error during shutdown", e);
					Thread.currentThread().interrupt();
				}
			}));

			// Start and block
			gatewayServer.start();
			logger.info("KvGateway gRPC server started on port {}", port);
			gatewayServer.awaitTermination();

		} catch (Exception e) {
			logger.error("Failed to start KvGateway", e);
			System.exit(1);
		}
	}

	private static int resolvePort(String[] args) {
		// First try config
		String configuredPortStr = CONFIG.getProperty("kvdb.gateway.port", String.valueOf(DEFAULT_PORT));
		int port = parsePort(configuredPortStr, DEFAULT_PORT, "config");

		// CLI override
		if (args.length > 0) {
			port = parsePort(args[0], port, "CLI");
		}

		return port;
	}

	private static int parsePort(String value, int fallback, String source) {
		try {
			int port = Integer.parseInt(value);
			if (port <= 0 || port > 65535) {
				throw new NumberFormatException("Port out of range: " + port);
			}
			logger.debug("Using port {} from {}", port, source);
			return port;
		} catch (NumberFormatException e) {
			logger.warn("Invalid {} port '{}', using fallback: {}", source, value, fallback);
			return fallback;
		}
	}
}
