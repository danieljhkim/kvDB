package com.danieljhkim.kvdb.kvclustercoordinator.raft;

import lombok.Getter;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

/**
 * Configuration for Raft consensus protocol.
 *
 * <p>
 * This class contains all the tunable parameters for Raft operation, including timeouts, cluster membership, and
 * performance settings.
 */
@Getter
public class RaftConfiguration {

    private final String nodeId;
    private final Map<String, String> clusterMembers; // nodeId -> gRPC address
    private final Duration electionTimeoutMin;
    private final Duration electionTimeoutMax;
    private final Duration heartbeatInterval;
    private final int maxEntriesPerAppendRequest;
    private final long snapshotThreshold;
    private final String dataDirectory;

    private RaftConfiguration(Builder builder) {
        this.nodeId = Objects.requireNonNull(builder.nodeId, "nodeId cannot be null");
        this.clusterMembers =
                Map.copyOf(Objects.requireNonNull(builder.clusterMembers, "clusterMembers cannot be null"));
        this.electionTimeoutMin =
                Objects.requireNonNull(builder.electionTimeoutMin, "electionTimeoutMin cannot be null");
        this.electionTimeoutMax =
                Objects.requireNonNull(builder.electionTimeoutMax, "electionTimeoutMax cannot be null");
        this.heartbeatInterval = Objects.requireNonNull(builder.heartbeatInterval, "heartbeatInterval cannot be null");
        this.maxEntriesPerAppendRequest = builder.maxEntriesPerAppendRequest;
        this.snapshotThreshold = builder.snapshotThreshold;
        this.dataDirectory = Objects.requireNonNull(builder.dataDirectory, "dataDirectory cannot be null");

        // Validation
        if (electionTimeoutMin.compareTo(electionTimeoutMax) > 0) {
            throw new IllegalArgumentException("electionTimeoutMin must be <= electionTimeoutMax");
        }
        if (heartbeatInterval.compareTo(electionTimeoutMin) >= 0) {
            throw new IllegalArgumentException("heartbeatInterval must be < electionTimeoutMin");
        }
        if (!clusterMembers.containsKey(nodeId)) {
            throw new IllegalArgumentException("nodeId must be present in clusterMembers");
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns the number of nodes in the cluster.
     */
    public int getClusterSize() {
        return clusterMembers.size();
    }

    /**
     * Returns the number of nodes required for a quorum (majority).
     */
    public int getQuorumSize() {
        return (clusterMembers.size() / 2) + 1;
    }

    /**
     * Returns all peer node IDs (excluding this node).
     */
    public Map<String, String> getPeers() {
        return clusterMembers.entrySet().stream()
                .filter(entry -> !entry.getKey().equals(nodeId))
                .collect(java.util.stream.Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static class Builder {

        private String nodeId;
        private Map<String, String> clusterMembers = Map.of();
        private Duration electionTimeoutMin = Duration.ofMillis(150);
        private Duration electionTimeoutMax = Duration.ofMillis(300);
        private Duration heartbeatInterval = Duration.ofMillis(50);
        private int maxEntriesPerAppendRequest = 100;
        private long snapshotThreshold = 10000;
        private String dataDirectory = "./data/raft";

        public Builder nodeId(String nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public Builder clusterMembers(Map<String, String> clusterMembers) {
            this.clusterMembers = clusterMembers;
            return this;
        }

        public Builder electionTimeoutMin(Duration electionTimeoutMin) {
            this.electionTimeoutMin = electionTimeoutMin;
            return this;
        }

        public Builder electionTimeoutMax(Duration electionTimeoutMax) {
            this.electionTimeoutMax = electionTimeoutMax;
            return this;
        }

        public Builder heartbeatInterval(Duration heartbeatInterval) {
            this.heartbeatInterval = heartbeatInterval;
            return this;
        }

        public Builder maxEntriesPerAppendRequest(int maxEntriesPerAppendRequest) {
            this.maxEntriesPerAppendRequest = maxEntriesPerAppendRequest;
            return this;
        }

        public Builder snapshotThreshold(long snapshotThreshold) {
            this.snapshotThreshold = snapshotThreshold;
            return this;
        }

        public Builder dataDirectory(String dataDirectory) {
            this.dataDirectory = dataDirectory;
            return this;
        }

        public RaftConfiguration build() {
            return new RaftConfiguration(this);
        }
    }

    @Override
    public String toString() {
        return "RaftConfiguration{" + "nodeId='" + nodeId + '\'' + ", clusterSize=" + clusterMembers.size()
                + ", electionTimeoutMin=" + electionTimeoutMin + ", electionTimeoutMax=" + electionTimeoutMax
                + ", heartbeatInterval=" + heartbeatInterval + ", maxEntriesPerAppendRequest="
                + maxEntriesPerAppendRequest + ", snapshotThreshold=" + snapshotThreshold + ", dataDirectory='"
                + dataDirectory + '\'' + '}';
    }
}
