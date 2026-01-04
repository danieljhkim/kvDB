package com.danieljhkim.kvdb.kvgateway;

import com.danieljhkim.kvdb.kvcommon.config.AppConfig;
import com.danieljhkim.kvdb.kvcommon.config.ConfigLoader;
import com.danieljhkim.kvdb.kvgateway.server.GatewayServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main entry point for the KvGateway gRPC server.
 *
 * <p>
 * Usage: java -jar kv-gateway.jar
 *
 * <p>
 * The gateway connects to the coordinator to fetch the shard map, then routes client requests to the appropriate
 * storage nodes.
 */
public class GatewayApplication {

    private static final Logger logger = LoggerFactory.getLogger(GatewayApplication.class);

    public static void main(String[] args) {
        logger.info("Starting KvGateway...");

        try {
            AppConfig appConfig = ConfigLoader.load();
            logger.info("Loaded application configuration");
            AppConfig.GatewayConfig gatewayConfig = appConfig.getGateway();
            GatewayServer gatewayServer = new GatewayServer(appConfig);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutting down KvGateway...");
                try {
                    gatewayServer.shutdown();
                } catch (Exception e) {
                    logger.error("Error during shutdown", e);
                    Thread.currentThread().interrupt();
                }
            }));

            gatewayServer.start();
            logger.info("KvGateway gRPC server started on port {}", gatewayConfig.getPort());
            gatewayServer.awaitTermination();

        } catch (Exception e) {
            logger.error("Failed to start KvGateway", e);
            System.exit(1);
        }
    }
}
