package com.danieljhkim.kvdb.kvclustercoordinator.scheduler;

import com.danieljhkim.kvdb.kvclustercoordinator.health.NodeHealthChecker;
import com.danieljhkim.kvdb.kvcommon.config.SystemConfig;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scheduler for performing periodic health checks on storage nodes. Manages its own ScheduledExecutorService and
 * delegates to NodeHealthChecker.
 */
public class HealthCheckScheduler {

    private static final Logger logger = LoggerFactory.getLogger(HealthCheckScheduler.class);

    private final ScheduledExecutorService clusterHealthScheduler;
    private final NodeHealthChecker healthChecker;
    private final int refreshIntervalSeconds;
    private final boolean enabled;

    public HealthCheckScheduler(NodeHealthChecker healthChecker, SystemConfig config) {
        this.healthChecker = healthChecker;
        this.refreshIntervalSeconds = Integer.parseInt(config.getProperty("kvdb.server.healthCheckInterval", "10"));
        this.enabled = true; // Always enabled for now
        this.clusterHealthScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "node-health-checker");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        if (!enabled) {
            logger.info("Health check scheduler is disabled.");
            return;
        }
        logger.info("Starting health check scheduler with interval {} seconds", refreshIntervalSeconds);
        this.clusterHealthScheduler.scheduleAtFixedRate(
                this::checkClusterHealth, refreshIntervalSeconds, refreshIntervalSeconds, TimeUnit.SECONDS);
    }

    public void shutdown() throws InterruptedException {
        logger.info("Shutting down health check scheduler...");
        this.clusterHealthScheduler.shutdown();
        if (!this.clusterHealthScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
            logger.warn("Health check scheduler did not terminate in time; forcing shutdown");
            this.clusterHealthScheduler.shutdownNow();
        }
        // Shutdown health checker to clean up cached channels
        healthChecker.shutdown();
    }

    private void checkClusterHealth() {
        try {
            logger.debug("Performing scheduled health check on storage nodes");
            healthChecker.checkAllNodes();
        } catch (Exception e) {
            logger.warn("Error during scheduled health check", e);
        }
    }
}
