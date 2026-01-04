package com.danieljhkim.kvdb.kvcommon.config;

import java.util.List;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class AppConfig {

    private NodeGroupConfig coordinatorNodes;
    private StorageNodeGroupConfig storageNodes;
    private GatewayConfig gateway;
    private PersistenceConfig persistence;
    private ReplicationConfig replication;
    private RaftConfig raft;

    @Setter
    @Getter
    public static class NodeGroupConfig {
        private List<NodeConfig> nodes;
        private String componentLabel;

        @Override
        public String toString() {
            return "NodeGroupConfig{" + "nodes=" + nodes + ", componentLabel='" + componentLabel + '}';
        }
    }

    @Setter
    @Getter
    public static class StorageNodeGroupConfig {
        private List<NodeConfig> nodes;
        private String componentLabel;

        @Override
        public String toString() {
            return "StorageNodeGroupConfig{" + "nodes="
                    + nodes + ", componentLabel='"
                    + componentLabel + '\'' + ", coordinator="
                    + '}';
        }
    }

    @Setter
    @Getter
    public static class NodeConfig {
        private String id;
        private String host;
        private int port;
        private int healthPort;
        private String role;
        private String dataDir;

        @Override
        public String toString() {
            return "NodeConfig{" + "id='"
                    + id + '\'' + ", host='"
                    + host + '\'' + ", port="
                    + port + ", healthPort="
                    + healthPort + ", role='"
                    + role + '\'' + ", dataDir='"
                    + dataDir + '\'' + '}';
        }
    }

    @Setter
    @Getter
    public static class PersistenceConfig {
        private boolean enableAutoFlush = true;
        private int autoFlushIntervalMs = 2000;
        private String snapshotFileName = "kvstore.dat";
        private String walFileName = "kvstore.wal";

        @Override
        public String toString() {
            return "PersistenceConfig{" + "enableAutoFlush="
                    + enableAutoFlush + ", autoFlushIntervalMs="
                    + autoFlushIntervalMs + ", snapshotFileName='"
                    + snapshotFileName + '\'' + ", walFileName='"
                    + walFileName + '\'' + '}';
        }
    }

    @Setter
    @Getter
    public static class ReplicationConfig {
        private long timeoutMs = 500;

        @Override
        public String toString() {
            return "ReplicationConfig{" + "timeoutMs=" + timeoutMs + '}';
        }
    }

    @Setter
    @Getter
    public static class RaftConfig {
        private int heartbeatIntervalMs = 50;
        private int electionTimeoutMinMs = 150;
        private int electionTimeoutMaxMs = 300;
        private int maxEntriesPerRequest = 100;

        @Override
        public String toString() {
            return "RaftConfig{" + "heartbeatIntervalMs="
                    + heartbeatIntervalMs + ", electionTimeoutMinMs="
                    + electionTimeoutMinMs + ", electionTimeoutMaxMs="
                    + electionTimeoutMaxMs + ", maxEntriesPerRequest="
                    + maxEntriesPerRequest + '}';
        }
    }

    @Setter
    @Getter
    public static class GatewayConfig {
        private int port = 7000;

        @Override
        public String toString() {
            return "GatewayConfig{" + "port=" + port + '}';
        }
    }
}
