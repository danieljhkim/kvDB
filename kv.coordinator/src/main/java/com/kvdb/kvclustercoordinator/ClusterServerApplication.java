package com.kvdb.kvclustercoordinator;

import com.kvdb.kvclustercoordinator.cluster.ClusterManager;
import com.kvdb.kvclustercoordinator.config.ClusterConfig;
import com.kvdb.kvclustercoordinator.server.ClusterServer;
import com.kvdb.kvclustercoordinator.sharding.BasicShardingStrategy;
import com.kvdb.kvclustercoordinator.sharding.ShardingStrategy;
import com.kvdb.kvcommon.config.SystemConfig;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Main entry point for the ClusterServer application.
 * Accepts command-line arguments for port and configuration file path.
 *
 * @param args Command-line arguments: [port] [configFilePath]
 */
public class ClusterServerApplication {

    private static final Logger LOGGER = Logger.getLogger(ClusterServerApplication.class.getName());
    private static final SystemConfig CONFIG = SystemConfig.getInstance("coordinator");
    private static final String DEFAULT_CONFIG_PATH = "cluster-config.yaml";

    public static void main(String[] args) {
        int port = Integer.parseInt(CONFIG.getProperty("kvdb.server.port"));
        String configFilePath = DEFAULT_CONFIG_PATH;

        if (args.length > 0) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                LOGGER.warning("Invalid port provided, using default port: " + port);
            }
        }
        if (args.length > 1) {
            configFilePath = args[1];
        }

        try {
            LOGGER.info("Starting ClusterServer on port " + port + " with config file: " + configFilePath);
            ClusterConfig clusterConfig = new ClusterConfig(configFilePath);
            ShardingStrategy basicSharding = new BasicShardingStrategy();
            ClusterManager clusterManager = new ClusterManager(clusterConfig, basicSharding);
            ClusterServer clusterServer = new ClusterServer(port, clusterManager);

            Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
                try {
                    LOGGER.severe("Uncaught exception in thread " + t.getName());
                    LOGGER.log(Level.SEVERE, "Exception details: ", e);}
                finally {
                    e.printStackTrace(System.err);  // fallback if logger is broken
                    System.err.flush();
                }
            });

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    LOGGER.info("Shutting down ClusterServer...");
                    clusterServer.shutdown();
                    Thread.sleep(1000); // give time for the logs to flush
                } catch (Exception e) {
                    LOGGER.log(Level.SEVERE, "Error during ClusterServer shutdown", e);
                }
            }));

            clusterServer.start();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to start ClusterServer", e);
            System.exit(1);
        }
    }

}