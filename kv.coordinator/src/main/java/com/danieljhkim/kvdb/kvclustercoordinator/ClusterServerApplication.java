package com.danieljhkim.kvdb.kvclustercoordinator;

import com.danieljhkim.kvdb.kvclustercoordinator.health.NodeHealthChecker;
import com.danieljhkim.kvdb.kvclustercoordinator.scheduler.HealthCheckScheduler;
import com.danieljhkim.kvdb.kvclustercoordinator.server.CoordinatorServer;
import com.danieljhkim.kvdb.kvcommon.config.SystemConfig;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Main application class for the Coordinator node.
 * Starts the gRPC server with the Coordinator service and health check
 * scheduler.
 */
public class ClusterServerApplication {

    private static final Logger LOGGER = Logger.getLogger(ClusterServerApplication.class.getName());
    private static final SystemConfig CONFIG = SystemConfig.getInstance("coordinator");
    private static final int DEFAULT_PORT = 9000;

    public static void main(String[] args) throws IOException, InterruptedException {
        LOGGER.info("Starting Coordinator server...");

        int port = DEFAULT_PORT;
        String portStr = CONFIG.getProperty("kvdb.coordinator.port");
        if (portStr != null && !portStr.isEmpty()) {
            port = Integer.parseInt(portStr);
        }

        // Override with command line argument if provided
        if (args.length > 0) {
            try {
                port = Integer.parseInt(args[0]);
                LOGGER.info("Port overridden from args: " + port);
            } catch (NumberFormatException e) {
                LOGGER.warning("Invalid port argument, using: " + port);
            }
        }
        CoordinatorServer coordServer = new CoordinatorServer(port);

        // Initialize health check scheduler
        NodeHealthChecker healthChecker = new NodeHealthChecker(coordServer.getRaftStateMachine());
        HealthCheckScheduler healthCheckScheduler = new HealthCheckScheduler(healthChecker, CONFIG);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Shutting down Coordinator server...");
            try {
                healthCheckScheduler.shutdown();
                coordServer.shutdown();
            } catch (InterruptedException e) {
                LOGGER.log(Level.SEVERE, "Error during shutdown", e);
                Thread.currentThread().interrupt();
            }
        }));

        // Start health check scheduler
        healthCheckScheduler.start();

        int finalPort = port;
        LOGGER.info(() -> "Coordinator gRPC server started on port " + finalPort);
        coordServer.start();
    }
}
