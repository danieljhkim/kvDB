package com.danieljhkim.kvdb.kvcommon.server;

import com.danieljhkim.kvdb.kvcommon.handler.ClientHandlerFactory;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KVServer implements BaseServer {

    private static final Logger logger = LoggerFactory.getLogger(KVServer.class);

    @Getter
    private final int port;

    private ExecutorService threadPool;
    private ServerSocket serverSocket;

    @Getter
    private volatile boolean running = false;

    private ClientHandlerFactory handlerFactory;

    public KVServer(int port) {
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("Port must be between 1 and 65535");
        }
        this.port = port;
    }

    public KVServer(int port, ClientHandlerFactory handlerFactory) {
        this.handlerFactory = handlerFactory;
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("Port must be between 1 and 65535");
        }
        this.port = port;
    }

    public void start() {
        if (running) {
            logger.warn("Server is already running");
            return;
        }
        threadPool = Executors.newVirtualThreadPerTaskExecutor();
        try {
            serverSocket = new ServerSocket(port);
            running = true;
            logger.info("Server started on port {}", port);
            acceptConnectionLoop(serverSocket);
        } catch (IOException e) {
            logger.error("Failed to start server on port {}", port, e);
        } finally {
            shutdown();
        }
    }

    public void acceptConnectionLoop(ServerSocket serverSocket) {
        while (running) {
            try {
                Socket clientSocket = serverSocket.accept();
                threadPool.execute(this.handlerFactory.createHandler(clientSocket));
            } catch (IOException e) {
                if (running) {
                    logger.warn("Error accepting client connection", e);
                }
            }
        }
    }

    public void shutdown() {
        if (!running) {
            return;
        }
        running = false;
        logger.info("Shutting down server...");
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException e) {
            logger.warn("Error closing server socket", e);
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
        logger.info("Server shutdown complete");
    }
}
