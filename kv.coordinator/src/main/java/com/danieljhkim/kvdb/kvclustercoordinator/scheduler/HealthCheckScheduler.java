package com.danieljhkim.kvdb.kvclustercoordinator.scheduler;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.danieljhkim.kvdb.kvclustercoordinator.health.NodeHealthChecker;
import com.danieljhkim.kvdb.kvcommon.config.SystemConfig;

/**
 * Scheduler for performing periodic health checks on storage nodes.
 * Manages its own ScheduledExecutorService and delegates to NodeHealthChecker.
 */
public class HealthCheckScheduler {

	private static final Logger LOGGER = Logger.getLogger(HealthCheckScheduler.class.getName());

	private final ScheduledExecutorService clusterHealthScheduler;
	private final NodeHealthChecker healthChecker;
	private final int refreshIntervalSeconds;
	private final boolean enabled;

	public HealthCheckScheduler(NodeHealthChecker healthChecker, SystemConfig config) {
		this.healthChecker = healthChecker;
		this.refreshIntervalSeconds = Integer.parseInt(
				config.getProperty("kvdb.server.healthCheckInterval", "30"));
		this.enabled = true; // Always enabled for now
		this.clusterHealthScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
			Thread t = new Thread(r, "node-health-checker");
			t.setDaemon(true);
			return t;
		});
	}

	public void start() {
		if (!enabled) {
			LOGGER.info("Health check scheduler is disabled.");
			return;
		}
		LOGGER.info("Starting health check scheduler with interval " + refreshIntervalSeconds + " seconds");
		this.clusterHealthScheduler.scheduleAtFixedRate(
				this::checkClusterHealth,
				refreshIntervalSeconds, refreshIntervalSeconds,
				TimeUnit.SECONDS);
	}

	public void shutdown() throws InterruptedException {
		LOGGER.info("Shutting down health check scheduler...");
		this.clusterHealthScheduler.shutdown();
		if (!this.clusterHealthScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
			LOGGER.warning("Health check scheduler did not terminate in time; forcing shutdown");
			this.clusterHealthScheduler.shutdownNow();
		}
		// Shutdown health checker to clean up cached channels
		healthChecker.shutdown();
	}

	private void checkClusterHealth() {
		try {
			LOGGER.fine("Performing scheduled health check on storage nodes");
			healthChecker.checkAllNodes();
		} catch (Exception e) {
			LOGGER.log(Level.WARNING, "Error during scheduled health check", e);
		}
	}
}
