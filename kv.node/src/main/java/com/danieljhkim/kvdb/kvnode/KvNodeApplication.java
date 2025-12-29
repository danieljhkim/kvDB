package com.danieljhkim.kvdb.kvnode;

import com.danieljhkim.kvdb.kvcommon.config.SystemConfig;
import com.danieljhkim.kvdb.kvnode.server.NodeServer;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KvNodeApplication {

	private static final Logger logger = LoggerFactory.getLogger(KvNodeApplication.class);
	private static final int DEFAULT_PORT = 8001;
	private static SystemConfig CONFIG;

	public static void main(String[] args) {
		try {
			if (args.length < 1) {
				CONFIG = SystemConfig.getInstance("node-1");
			} else {
				logger.info("{}", Arrays.toString(args));
				CONFIG = SystemConfig.getInstance(args[0]);
			}

			int port = getPort();
			NodeServer nodeServer = new NodeServer(port);

			Runtime.getRuntime().addShutdownHook(new Thread(() -> {
				logger.info("Shutting down Node gRPC server...");
				try {
					nodeServer.shutdown();
				} catch (InterruptedException e) {
					logger.error("Error during shutdown", e);
					Thread.currentThread().interrupt();
				}
			}));

			logger.info("IndexNode gRPC server started on port {}", port);
			nodeServer.start();
		} catch (Exception e) {
			logger.error("Server failed to start", e);
			System.exit(1);
		}
	}

	private static int getPort() {
		return Integer.parseInt(
				CONFIG.getProperty(
						"kvdb.server.port", String.valueOf(KvNodeApplication.DEFAULT_PORT)));
	}
}
