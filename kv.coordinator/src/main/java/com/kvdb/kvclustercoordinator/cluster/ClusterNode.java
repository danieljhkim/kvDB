package com.kvdb.kvclustercoordinator.cluster;
import com.kvdb.kvcommon.persistence.WALManager;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

@Setter
@Getter
public class ClusterNode {

    private final String id;
    private final ClusterNodeClient client;
    private final String host;
    private final int port;
    public boolean isGrpc;
    private boolean isRunning = false;
    private String walFileName;
    private WALManager walManager; // for when a node is down
    private volatile boolean canAccess = true; // during recovery, we need to lock access to this node
    private final ReentrantLock accessLock = new ReentrantLock();

    public ClusterNode(String id, String host, int port, boolean useGrpc, int grpcPort) {
        this.id = id;
        this.host = host;
        this.port = port;
        this.isGrpc = useGrpc;
        this.client = useGrpc ? new GrpcClusterNodeClient(host, grpcPort) : new HttpClusterNodeClient(host, port);
        initWALManager("./wal/" + id + ".log");
    }

    public ClusterNode(String id, String host, int port) {
        this(id, host, port, false, 0);
    }

    public boolean sendSet(String key, String value) {
        while (!canAccess) {
            // temporary solution for recovery phase
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return client.sendSet(key, value);
    }

    public String sendGet(String key) {
        return client.sendGet(key);
    }

    public boolean sendDelete(String key) {
        while (!canAccess) {
            // temporary solution for recovery phase
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return client.sendSet(key, "");
    }

    public boolean isRunning() {
        isRunning = this.client.ping();
        return isRunning;
    }

    public void shutdown() {
        if (client != null) {
            client.shutdown();
        }
        if (walManager != null) {
            walManager.clear();
        }
    }

    public void logWal(String operation, String key, String value) {
        if (walManager != null) {
            walManager.log(operation, key, value);
        }
    }

    public void clearWal() {
        if (walManager != null) {
            walManager.clear();
        }
    }

    public Map<String, String[]> replayWal() {
        if (walManager != null) {
            return walManager.replayAsMap();
        }
        return Map.of();
    }

    private void initWALManager(String walDir) {
        this.walManager = new WALManager(walDir);
        this.walFileName = walDir;
    }

    public void setCanAccess(boolean enabled) {
        accessLock.lock();
        try {
            this.canAccess = enabled;
        } finally {
            accessLock.unlock();
        }
    }
}