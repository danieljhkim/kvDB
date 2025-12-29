package com.danieljhkim.kvdb.kvclustercoordinator;

import com.danieljhkim.kvdb.kvclustercoordinator.health.NodeHealthChecker;
import com.danieljhkim.kvdb.kvclustercoordinator.scheduler.HealthCheckScheduler;
import com.danieljhkim.kvdb.kvclustercoordinator.server.CoordinatorServer;
import com.danieljhkim.kvdb.kvcommon.config.SystemConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Main application class for the Coordinator node.
 * Starts the gRPC server with the Coordinator service and health check
 * scheduler.
 */
public class ClusterServerApplication {

	private static final Logger logger = LoggerFactory.getLogger(ClusterServerApplication.class);
	private static final SystemConfig CONFIG = SystemConfig.getInstance("coordinator");
	private static final int DEFAULT_PORT = 9000;

	public static void main(String[] args) throws IOException, InterruptedException {
		logger.info("Starting Coordinator server...");

		int port = DEFAULT_PORT;
		String portStr = CONFIG.getProperty("kvdb.coordinator.port");
		if (portStr != null && !portStr.isEmpty()) {
			port = Integer.parseInt(portStr);
		}

		if (args.length > 0) {
			try {
				port = Integer.parseInt(args[0]);
				logger.info("Port overridden from args: {}", port);
			} catch (NumberFormatException e) {
				logger.warn("Invalid port argument, using: {}", port);
			}
		}
		CoordinatorServer coordServer = new CoordinatorServer(port);
		NodeHealthChecker healthChecker = new NodeHealthChecker(coordServer.getRaftStateMachine());
		HealthCheckScheduler healthCheckScheduler = new HealthCheckScheduler(healthChecker, CONFIG);

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info("Shutting down Coordinator server...");
			try {
				healthCheckScheduler.shutdown();
				coordServer.shutdown();
			} catch (InterruptedException e) {
				logger.error("Error during shutdown", e);
				Thread.currentThread().interrupt();
			}
		}));

		logger.info("Coordinator gRPC server started on port {}", port);
		healthCheckScheduler.start();
		coordServer.start();
	}
}
