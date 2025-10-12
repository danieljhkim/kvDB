package com.kvdb.kvcommon.server;

import com.kvdb.kvcommon.handler.ClientHandlerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class KVServer implements BaseServer {

    private static final Logger LOGGER = Logger.getLogger(KVServer.class.getName());
    private static final int DEFAULT_THREAD_POOL_SIZE = 10;

    private final int port;
    private final int threadPoolSize;
    private ExecutorService threadPool;
    private ServerSocket serverSocket;
    private volatile boolean running = false;
    private ClientHandlerFactory handlerFactory;

    public KVServer(int port, int threadPoolSize) {
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("Port must be between 1 and 65535");
        }
        if (threadPoolSize <= 0) {
            throw new IllegalArgumentException("Thread pool size must be positive");
        }
        this.port = port;
        this.threadPoolSize = threadPoolSize;
    }

    public KVServer(int port, int threadPoolSize, ClientHandlerFactory handlerFactory) {
        this.handlerFactory = handlerFactory;
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("Port must be between 1 and 65535");
        }
        if (threadPoolSize <= 0) {
            throw new IllegalArgumentException("Thread pool size must be positive");
        }
        this.port = port;
        this.threadPoolSize = threadPoolSize;
    }

    public void start() {
        if (running) {
            LOGGER.warning("Server is already running");
            return;
        }
        threadPool = Executors.newFixedThreadPool(threadPoolSize);
        try {
            serverSocket = new ServerSocket(port);
            running = true;
            LOGGER.info("Server started on port " + port);
            acceptConnectionLoop();
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to start server on port " + port, e);
        } finally {
            shutdown();
        }
    }

    public void acceptConnectionLoop() {
        while (running) {
            try {
                Socket clientSocket = serverSocket.accept();
                threadPool.execute(this.handlerFactory.createHandler(clientSocket));
            } catch (IOException e) {
                if (running) {
                    LOGGER.log(Level.WARNING, "Error accepting client connection", e);
                }
            }
        }
    }

    public void shutdown() {
        if (!running) {
            return;
        }
        running = false;
        LOGGER.info("Shutting down server...");
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Error closing server socket", e);
        }
        if (threadPool != null) {
            threadPool.shutdown();
            try {
                if (!threadPool.awaitTermination(10, TimeUnit.SECONDS)) {
                    threadPool.shutdownNow();
                }
            } catch (InterruptedException e) {
                threadPool.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        LOGGER.info("Server shutdown complete");
    }

    public boolean isRunning() {
        return running;
    }

    public int getPort() {
        return port;
    }
}