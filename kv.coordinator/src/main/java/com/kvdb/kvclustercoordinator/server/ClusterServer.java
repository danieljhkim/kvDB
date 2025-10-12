package com.kvdb.kvclustercoordinator.server;

import com.kvdb.kvclustercoordinator.cluster.ClusterManager;
import com.kvdb.kvclustercoordinator.handler.ClusterClientHandler;
import com.kvdb.kvcommon.exception.NoHealthyNodesAvailable;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * ClusterServer is the main server class that manages cluster nodes and handles client connections.
 * It initializes cluster nodes from a configuration file and accepts client connections to handle requests.
 */
public class ClusterServer {

    private static final Logger LOGGER = Logger.getLogger(ClusterServer.class.getName());
    private final ClusterManager clusterManager;
    private int port;
    private ServerSocket serverSocket;
    private boolean running = false;
    private final int threadPoolSize = 10;
    private ExecutorService threadPool;
    private final ScheduledExecutorService healthCheckScheduler = Executors.newSingleThreadScheduledExecutor();

    public ClusterServer(int port, ClusterManager clusterManager) {
        this.port = port;
        this.clusterManager = clusterManager;
        this.threadPool = Executors.newFixedThreadPool(threadPoolSize);
    }

    public void start() {
        if (running) {
            LOGGER.warning("Server is already running");
            return;
        }
        LOGGER.info("Starting ClusterServer...");
        this.clusterManager.initializeClusterNodes();
        LOGGER.info("ClusterServer started successfully.");

        // Start the health check scheduler
        startHealthCheckScheduler();

        try {
            serverSocket = new ServerSocket(port);
            running = true;
            LOGGER.info("Server started on port " + port);
            acceptConnectionLoop();
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to start server on port " + port, e);
            shutdown();
        }
    }

    private void startHealthCheckScheduler() {
        healthCheckScheduler.scheduleAtFixedRate(() -> {
            try {
                LOGGER.info("Performing scheduled health check");
                clusterManager.checkNodeHealths();
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Error during scheduled health check", e);
            }
        }, 0, 5, TimeUnit.SECONDS);
    }

    public void acceptConnectionLoop() {
        while (running) {
            try {
                Socket clientSocket = serverSocket.accept();
                LOGGER.info("Accepted connection from " + clientSocket.getInetAddress() + ":" + clientSocket.getPort());
                threadPool.execute(new ClusterClientHandler(clientSocket, clusterManager));
            } catch (IOException e) {
                if (running) {
                    LOGGER.log(Level.WARNING, "Error accepting client connection", e);
                }
            } catch (NoHealthyNodesAvailable e) {
                LOGGER.warning("No healthy nodes available. Waiting for health check to recover nodes.");
                try {
                    Thread.sleep(1000); // Small wait before trying again
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    public void shutdown() {
        healthCheckScheduler.shutdown();
        try {
            if (!healthCheckScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                healthCheckScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            healthCheckScheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }

        LOGGER.info("ClusterServer shut down successfully.");
    }
}