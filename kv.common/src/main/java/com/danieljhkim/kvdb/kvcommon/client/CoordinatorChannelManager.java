package com.danieljhkim.kvdb.kvcommon.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

/**
 * Manages gRPC channels to coordinator nodes with leader tracking.
 * Thread-safe channel pooling with automatic channel creation and cleanup.
 */
@Slf4j
public class CoordinatorChannelManager implements AutoCloseable {

    private final CoordinatorClientConfig config;
    private final ConcurrentHashMap<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final AtomicReference<String> currentLeaderAddress = new AtomicReference<>();
    private final AtomicInteger roundRobinIndex = new AtomicInteger(0);

    public CoordinatorChannelManager(CoordinatorClientConfig config) {
        this.config = config;
        // Initialize with first address as default
        if (!config.getAddresses().isEmpty()) {
            currentLeaderAddress.set(config.getAddresses().get(0));
        }
    }

    /**
     * Gets or creates a channel for the given address.
     */
    public ManagedChannel getChannel(String address) {
        return channels.computeIfAbsent(address, this::createChannel);
    }

    /**
     * Gets channel for the current known leader.
     */
    public ManagedChannel getLeaderChannel() {
        String leader = currentLeaderAddress.get();
        if (leader == null && !config.getAddresses().isEmpty()) {
            leader = config.getAddresses().get(0);
        }
        return getChannel(leader);
    }

    /**
     * Updates the known leader address.
     *
     * @param leaderAddress new leader address in "host:port" format
     */
    public void updateLeader(String leaderAddress) {
        if (leaderAddress != null && !leaderAddress.equals(currentLeaderAddress.get())) {
            log.info("Updating leader address: {} -> {}", currentLeaderAddress.get(), leaderAddress);
            currentLeaderAddress.set(leaderAddress);
        }
    }

    /**
     * Gets the next address using round-robin strategy.
     * Used as a fallback when no leader hint is available.
     */
    public String getNextAddress() {
        List<String> addresses = config.getAddresses();
        if (addresses.isEmpty()) {
            return null;
        }
        int index = roundRobinIndex.getAndUpdate(i -> (i + 1) % addresses.size());
        return addresses.get(index);
    }

    /**
     * Gets current leader address (may be stale).
     */
    public String getCurrentLeaderAddress() {
        return currentLeaderAddress.get();
    }

    /**
     * Gets all configured coordinator addresses.
     */
    public List<String> getAllAddresses() {
        return config.getAddresses();
    }

    private ManagedChannel createChannel(String address) {
        log.debug("Creating channel to coordinator at {}", address);
        String[] parts = address.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);

        return ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
    }

    @Override
    public void close() {
        log.info("Shutting down CoordinatorChannelManager");
        for (ManagedChannel channel : channels.values()) {
            try {
                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                channel.shutdownNow();
            }
        }
        channels.clear();
    }
}
