package com.danieljhkim.kvdb.kvgateway;

import com.danieljhkim.kvdb.kvcommon.config.SystemConfig;
import com.danieljhkim.kvdb.kvgateway.cluster.ClusterManager;
import com.danieljhkim.kvdb.kvgateway.config.ClusterConfig;
import com.danieljhkim.kvdb.kvgateway.server.ClusterServer;
import com.danieljhkim.kvdb.kvgateway.sharding.BasicShardingStrategy;
import com.danieljhkim.kvdb.kvgateway.sharding.ShardingStrategy;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Main entry point for the ClusterServer application.
 *
 * <p>
 * Usage: java -jar kv.coordinator.jar [port] [configFilePath]
 *
 * <p>
 * Args: port (optional) TCP port for the coordinator (overrides config)
 * configFilePath
 * (optional) path to YAML cluster config (default: cluster-config.yaml)
 */
public class GatewayApplication {

    private static final Logger LOGGER = Logger.getLogger(GatewayApplication.class.getName());
    private static final SystemConfig CONFIG = SystemConfig.getInstance("coordinator");
    private static final String DEFAULT_CONFIG_PATH = "cluster-config.yaml";

    public static void main(String[] args) {
        // 1. Resolve port (config first, then CLI override)
        int port = resolvePort(args);

        // 2. Resolve config file path
        String configFilePath = resolveConfigPath(args);

        try {
            LOGGER.info("Starting ClusterServer on port " + port + " with config file: " + configFilePath);

            ClusterConfig clusterConfig = new ClusterConfig(configFilePath);
            ShardingStrategy shardingStrategy = new BasicShardingStrategy();
            ClusterManager clusterManager = new ClusterManager(clusterConfig, shardingStrategy);
            ClusterServer clusterServer = new ClusterServer(port, clusterManager);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOGGER.info("Shutting down Node gRPC server...");
                try {
                    clusterServer.shutdown();
                } catch (Exception e) {
                    LOGGER.log(Level.SEVERE, "Error during shutdown", e);
                    Thread.currentThread().interrupt();
                }
            }));

            // 5. Block running the server
            clusterServer.start();

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to start ClusterServer", e);
            System.exit(1);
        }
    }

    private static int resolvePort(String[] args) {
        String configuredPortStr = CONFIG.getProperty("kvdb.server.port", "8080");
        int port = parsePort(configuredPortStr, 8080, "config");

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
            LOGGER.info("Using port " + port + " from " + source);
            return port;
        } catch (NumberFormatException e) {
            LOGGER.warning(
                    "Invalid " + source + " port '" + value + "', using fallback: " + fallback);
            return fallback;
        }
    }

    private static String resolveConfigPath(String[] args) {
        if (args.length > 1 && args[1] != null && !args[1].isBlank()) {
            return args[1];
        }
        return DEFAULT_CONFIG_PATH;
    }
}
