package com.kvdb.kvclustercoordinator.cluster;
import com.kvdb.kvcommon.persistence.WALManager;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

@Setter
@Getter
public class ClusterNode {

    Logger LOGGER = Logger.getLogger(ClusterNode.class.getName());
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
    private final Condition accessCondition = accessLock.newCondition();

    public ClusterNode(String id, String host, int port, boolean useGrpc) {
        this.id = id;
        this.host = host;
        this.port = port;
        this.isGrpc = useGrpc;
        this.client = useGrpc ? new GrpcClusterNodeClient(host, port) : new HttpClusterNodeClient(host, port);
        initWALManager("data/" + id + ".wal");
    }

    public ClusterNode(String id, String host, int port) {
        this(id, host, port, false);
    }

    public boolean sendSet(String key, String value) {
        if (!waitForAccess()) {
            return false;
        }
        return client.sendSet(key, value);
    }

    public String sendGet(String key) {
        return client.sendGet(key);
    }

    public boolean sendDelete(String key) {
        if (!waitForAccess()) {
            return false;
        }
        return client.sendSet(key, "");
    }

    public boolean isRunning() {
        try {
            isRunning = this.client.ping();
        } catch (Exception e) {
            LOGGER.severe("Ping to node " + id + " failed: " + e.getMessage());
            isRunning = false;
        }
        return isRunning;
    }

    public void shutdown() {
        if (client != null) {
            client.shutdown();
        }
        clearWal();
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
            if (enabled) {
                accessCondition.signalAll();
            }
        } finally {
            accessLock.unlock();
        }
    }

    private boolean waitForAccess() {
        accessLock.lock();
        try {
            while (!canAccess) {
                accessCondition.await();
            }
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } finally {
            accessLock.unlock();
        }
    }
}