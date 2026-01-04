package com.danieljhkim.kvdb.kvclustercoordinator;

import com.danieljhkim.kvdb.kvclustercoordinator.health.NodeHealthChecker;
import com.danieljhkim.kvdb.kvclustercoordinator.scheduler.HealthCheckScheduler;
import com.danieljhkim.kvdb.kvclustercoordinator.server.CoordinatorServer;
import com.danieljhkim.kvdb.kvcommon.config.AppConfig;
import com.danieljhkim.kvdb.kvcommon.config.ConfigLoader;
import com.danieljhkim.kvdb.kvcommon.config.SystemConfig;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main application class for the Coordinator node. Starts the gRPC server with the Coordinator service and health check
 * scheduler.
 */
public class ClusterServerApplication {

    private static final Logger logger = LoggerFactory.getLogger(ClusterServerApplication.class);
    private static final SystemConfig CONFIG = SystemConfig.getInstance("coordinator");

    public static void main(String[] args) throws IOException, InterruptedException {
        logger.info("Starting Coordinator server...");

        String nodeId = System.getenv().getOrDefault("COORDINATOR_NODE_ID", "coordinator-1");
        logger.info("Starting coordinator with nodeId: {}", nodeId);

        AppConfig appConfig = ConfigLoader.load();
        logger.info("Loaded application configuration: {}", appConfig);

        CoordinatorServer coordServer = new CoordinatorServer(nodeId, appConfig);
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

        logger.info("Coordinator gRPC server starting for node: {}", nodeId);
        healthCheckScheduler.start();
        coordServer.start();
    }
}
