package com.danieljhkim.kvdb.kvcommon.observability;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Exposes unauthenticated, payload-free liveness, readiness, and metrics endpoints on a management port. */
public final class HealthHttpServer implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(HealthHttpServer.class);
    private final HttpServer server;
    private final ExecutorService executor;

    public HealthHttpServer(int port, ServiceLifecycle lifecycle, Supplier<Boolean> ready) throws IOException {
        this(new InetSocketAddress("127.0.0.1", port), lifecycle, ready);
    }

    public HealthHttpServer(InetSocketAddress address, ServiceLifecycle lifecycle, Supplier<Boolean> ready)
            throws IOException {
        server = HttpServer.create(address, 0);
        executor = Executors.newSingleThreadExecutor(r -> {
            Thread thread = new Thread(r, "kvdb-health-http");
            thread.setDaemon(true);
            return thread;
        });
        server.setExecutor(executor);
        server.createContext(
                "/health/live", exchange -> respond(exchange, 200, "{\"status\":\"UP\"}\n", "application/json"));
        server.createContext("/health/ready", exchange -> {
            boolean isReady = lifecycle.isAccepting() && isDependencyReady(ready);
            respond(
                    exchange,
                    isReady ? 200 : 503,
                    "{\"status\":\"" + (isReady ? "UP" : "DOWN") + "\"}\n",
                    "application/json");
        });
        server.createContext(
                "/metrics", exchange -> respond(exchange, 200, Metrics.scrape(), "text/plain; version=0.0.4"));
    }

    public void start() {
        server.start();
        logger.info("Health HTTP server listening on {}", server.getAddress());
    }

    public InetSocketAddress getAddress() {
        return server.getAddress();
    }

    public int getPort() {
        return server.getAddress().getPort();
    }

    @Override
    public void close() {
        server.stop(0);
        executor.shutdownNow();
    }

    private static void respond(HttpExchange exchange, int code, String body, String contentType) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(code, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

    private static boolean isDependencyReady(Supplier<Boolean> ready) {
        try {
            return Boolean.TRUE.equals(ready.get());
        } catch (RuntimeException ignored) {
            return false;
        }
    }
}
