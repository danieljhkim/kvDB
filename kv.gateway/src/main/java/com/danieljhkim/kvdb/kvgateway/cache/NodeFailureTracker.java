package com.danieljhkim.kvdb.kvgateway.cache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks recently failed nodes to avoid immediate retry to the same node. Acts as a negative cache to improve retry
 * efficiency.
 */
public class NodeFailureTracker {

    private static final Logger logger = LoggerFactory.getLogger(NodeFailureTracker.class);

    private final Map<String, Long> failedNodes = new ConcurrentHashMap<>();
    private final long failureTtlMs;

    /**
     * Creates a NodeFailureTracker with default TTL (5 seconds).
     */
    public NodeFailureTracker() {
        this(5000);
    }

    /**
     * Creates a NodeFailureTracker with custom TTL.
     *
     * @param failureTtlMs how long to remember a node failure in milliseconds
     */
    public NodeFailureTracker(long failureTtlMs) {
        this.failureTtlMs = failureTtlMs;
    }

    /**
     * Records a failure for a node.
     *
     * @param nodeAddress the address of the failed node (e.g., "localhost:8001")
     */
    public void recordFailure(String nodeAddress) {
        if (nodeAddress == null || nodeAddress.isEmpty()) {
            return;
        }
        long now = System.currentTimeMillis();
        failedNodes.put(nodeAddress, now);
        logger.debug("Recorded failure for node: {}", nodeAddress);
    }

    /**
     * Checks if a node has failed recently (within TTL).
     *
     * @param nodeAddress the address to check
     * @return true if the node failed recently
     */
    public boolean isRecentlyFailed(String nodeAddress) {
        if (nodeAddress == null || nodeAddress.isEmpty()) {
            return false;
        }

        Long failureTime = failedNodes.get(nodeAddress);
        if (failureTime == null) {
            return false;
        }

        long now = System.currentTimeMillis();
        if (now - failureTime > failureTtlMs) {
            // Expired, remove from cache
            failedNodes.remove(nodeAddress, failureTime);
            return false;
        }

        return true;
    }

    /**
     * Clears the failure record for a node (e.g., on successful request).
     *
     * @param nodeAddress the address to clear
     */
    public void clearFailure(String nodeAddress) {
        if (nodeAddress == null || nodeAddress.isEmpty()) {
            return;
        }
        if (failedNodes.remove(nodeAddress) != null) {
            logger.debug("Cleared failure for node: {}", nodeAddress);
        }
    }

    /**
     * Gets the number of currently tracked failed nodes.
     */
    public int getFailedNodeCount() {
        // Clean up expired entries
        long now = System.currentTimeMillis();
        failedNodes.entrySet().removeIf(entry -> now - entry.getValue() > failureTtlMs);
        return failedNodes.size();
    }

    /**
     * Clears all failure records.
     */
    public void clear() {
        failedNodes.clear();
    }
}
