package com.danieljhkim.kvdb.kvadmin.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * gRPC client for Gateway admin operations (stats, routing cache, etc.).
 *
 * <p>
 * Note: Gateway admin APIs are not yet defined in the proto. This is a placeholder.
 */
public class GatewayAdminClient {

    private static final Logger logger = LoggerFactory.getLogger(GatewayAdminClient.class);

    private final ManagedChannel channel;
    private final long timeoutSeconds;

    public GatewayAdminClient(String host, int port, long timeout, TimeUnit timeUnit) {
        this.channel =
                ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        this.timeoutSeconds = timeUnit.toSeconds(timeout);
        logger.info("GatewayAdminClient created for {}:{}", host, port);
    }

    // TODO: Add gateway admin RPCs when proto is defined

    public void shutdown() {
        logger.info("Shutting down GatewayAdminClient");
        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.warn("Interrupted while shutting down GatewayAdminClient");
            channel.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
