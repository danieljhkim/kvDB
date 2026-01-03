package com.danieljhkim.kvdb.kvnode;

import com.danieljhkim.kvdb.kvcommon.config.AppConfig;
import com.danieljhkim.kvdb.kvcommon.config.ConfigLoader;
import com.danieljhkim.kvdb.kvnode.server.NodeServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KvNodeApplication {

    private static final Logger logger = LoggerFactory.getLogger(KvNodeApplication.class);

    public static void main(String[] args) {
        try {
            String nodeId;
            if (args.length > 0) {
                nodeId = args[0];
                logger.info("Starting node with ID from args: {}", nodeId);
            } else {
                nodeId = System.getenv().getOrDefault("STORAGE_NODE_ID", "node-1");
                logger.info("Starting node with ID from env: {}", nodeId);
            }

            AppConfig appConfig = ConfigLoader.load();
            logger.info("Loaded application configuration");
            NodeServer nodeServer = new NodeServer(nodeId, appConfig);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutting down Node gRPC server...");
                try {
                    nodeServer.shutdown();
                } catch (InterruptedException e) {
                    logger.error("Error during shutdown", e);
                    Thread.currentThread().interrupt();
                }
            }));

            logger.info("Storage Node gRPC server started for node: {}", nodeId);
            nodeServer.start();
        } catch (Exception e) {
            logger.error("Server failed to start", e);
            System.exit(1);
        }
    }
}
