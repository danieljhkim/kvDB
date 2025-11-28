package com.kvdb.kvclustercoordinator.server;

import com.kvdb.kvclustercoordinator.cluster.ClusterManager;
import com.kvdb.kvclustercoordinator.handler.ClusterClientHandler;
import com.kvdb.kvcommon.config.SystemConfig;
import com.kvdb.kvcommon.exception.NoHealthyNodesAvailable;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * ClusterServer is the main server class that manages cluster nodes and handles client connections.
 * It initializes cluster nodes from a configuration file and accepts client connections to handle
 * requests.
 */
public class ClusterServer {
    private static final Logger LOGGER = Logger.getLogger(ClusterServer.class.getName());
    private static final SystemConfig CONFIG = SystemConfig.getInstance();

    private final int healthCheckInterval =
            Integer.parseInt(CONFIG.getProperty("kvdb.server.healthCheckInterval", "5"));

    private final int port;
    private final ClusterManager clusterManager;
    private final ExecutorService threadPool;
    private final ScheduledExecutorService healthCheckScheduler;

    private volatile boolean running = false;
    private ServerSocket serverSocket;

    public ClusterServer(int port, ClusterManager clusterManager) {
        this.port = port;
        this.clusterManager = clusterManager;
        this.threadPool = Executors.newVirtualThreadPerTaskExecutor();

        this.healthCheckScheduler =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "kvdb-health-check");
                            t.setDaemon(true);
                            return t;
                        });
    }

    public void start() {
        if (running) {
            LOGGER.warning("Server is already running");
            return;
        }

        LOGGER.info("Starting ClusterServer...");
        this.clusterManager.initializeClusterNodes();
        LOGGER.info("Cluster nodes initialized.");

        startHealthCheckScheduler();

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            this.serverSocket = serverSocket;
            running = true;
            LOGGER.info("ClusterServer started on port " + port);
            acceptConnectionLoop();
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to start server on port " + port, e);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Unexpected error during server startup", e);
        } finally {
            LOGGER.info("Shutting down the server...");
            shutdown();
        }
    }

    private void startHealthCheckScheduler() {
        // initial delay 10s, then run every healthCheckInterval seconds
        healthCheckScheduler.scheduleAtFixedRate(
                () -> {
                    try {
                        LOGGER.fine("Performing scheduled health check");
                        clusterManager.checkNodeHealths();
                    } catch (Exception e) {
                        LOGGER.log(Level.WARNING, "Error during scheduled health check", e);
                    }
                },
                10L,
                healthCheckInterval,
                TimeUnit.SECONDS);
    }

    private void acceptConnectionLoop() {
        while (running) {
            try {
                Socket clientSocket = serverSocket.accept();
                LOGGER.info(
                        "Accepted connection from "
                                + clientSocket.getInetAddress()
                                + ":"
                                + clientSocket.getPort());
                threadPool.execute(new ClusterClientHandler(clientSocket, clusterManager));
            } catch (IOException e) {
                if (running) {
                    LOGGER.log(Level.WARNING, "Error accepting client connection", e);
                } else {
                    LOGGER.info("Server socket closed; stopping accept loop");
                    break;
                }
            } catch (NoHealthyNodesAvailable e) {
                LOGGER.warning(
                        "No healthy nodes available. Waiting for health check to recover nodes.");
                try {
                    Thread.sleep(10_000); // Small wait before trying again
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    LOGGER.warning("Accept loop interrupted while waiting for healthy nodes");
                    break;
                }
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Unexpected error in connection loop", e);
            }
        }
    }

    /**
     * Gracefully shut down the server: - stop accepting new connections - stop health checks - stop
     * worker threads
     */
    public void shutdown() {
        running = false;

        // Close server socket to unblock accept()
        if (serverSocket != null && !serverSocket.isClosed()) {
            try {
                serverSocket.close();
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Error while closing server socket", e);
            }
        }

        // Stop health check scheduler
        healthCheckScheduler.shutdown();
        try {
            if (!healthCheckScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                LOGGER.warning(
                        "Health check scheduler did not terminate in time; forcing shutdown");
                healthCheckScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            LOGGER.warning("Interrupted during health check scheduler shutdown");
            healthCheckScheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // Stop worker thread pool
        threadPool.shutdown();
        try {
            if (!threadPool.awaitTermination(10, TimeUnit.SECONDS)) {
                LOGGER.warning("Worker thread pool did not terminate in time; forcing shutdown");
                threadPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            LOGGER.warning("Interrupted during worker thread pool shutdown");
            threadPool.shutdownNow();
            Thread.currentThread().interrupt();
        }

        LOGGER.info("ClusterServer shut down successfully.");
    }
}
