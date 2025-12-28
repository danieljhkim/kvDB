package com.danieljhkim.kvdb.kvclustercoordinator;

import com.danieljhkim.kvdb.kvclustercoordinator.cluster.ClusterManager;
import com.danieljhkim.kvdb.kvclustercoordinator.config.ClusterConfig;
import com.danieljhkim.kvdb.kvclustercoordinator.server.ClusterServer;
import com.danieljhkim.kvdb.kvclustercoordinator.sharding.BasicShardingStrategy;
import com.danieljhkim.kvdb.kvclustercoordinator.sharding.ShardingStrategy;
import com.danieljhkim.kvdb.kvcommon.config.SystemConfig;

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
public class ClusterServerApplication {

	private static final Logger LOGGER = Logger.getLogger(ClusterServerApplication.class.getName());
	private static final SystemConfig CONFIG = SystemConfig.getInstance("coordinator");
	private static final String DEFAULT_CONFIG_PATH = "cluster-config.yaml";

	public static void main(String[] args) {
		// 1. Resolve port (config first, then CLI override)
		int port = resolvePort(args);

		// 2. Resolve config file path
		String configFilePath = resolveConfigPath(args);

		// 3. Set a global uncaught exception handler as early as possible
		Thread.setDefaultUncaughtExceptionHandler(
				(t, e) -> {
					try {
						LOGGER.severe("Uncaught exception in thread " + t.getName());
						LOGGER.log(Level.SEVERE, "Exception details:", e);
					} finally {
						// Fallback if logging is misconfigured / broken
						e.printStackTrace(System.err);
						System.err.flush();
					}
				});

		try {
			LOGGER.info(
					"Starting ClusterServer on port "
							+ port
							+ " with config file: "
							+ configFilePath);

			ClusterConfig clusterConfig = new ClusterConfig(configFilePath);
			ShardingStrategy shardingStrategy = new BasicShardingStrategy();
			ClusterManager clusterManager = new ClusterManager(clusterConfig, shardingStrategy);
			ClusterServer clusterServer = new ClusterServer(port, clusterManager);

			// 4. Register a shutdown hook to gracefully stop the server
			Runtime.getRuntime()
					.addShutdownHook(
							new Thread(
									() -> {
										try {
											LOGGER.info(
													"Shutdown hook triggered. Shutting down ClusterServer...");
											clusterServer.shutdown();
											// Give logging / background tasks a brief moment to
											// flush
											Thread.sleep(1000);
										} catch (Exception e) {
											LOGGER.log(
													Level.SEVERE,
													"Error during ClusterServer shutdown",
													e);
										}
									},
									"cluster-server-shutdown-hook"));

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
