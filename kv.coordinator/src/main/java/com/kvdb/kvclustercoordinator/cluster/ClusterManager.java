package com.kvdb.kvclustercoordinator.cluster;

import com.kvdb.kvclustercoordinator.config.ClusterConfig;
import com.kvdb.kvclustercoordinator.sharding.ShardingStrategy;
import com.kvdb.kvcommon.constants.AppStatus;
import com.kvdb.kvcommon.exception.CodeRedException;
import com.kvdb.kvcommon.exception.NoHealthyNodesAvailable;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ClusterManager {

    Logger LOGGER = Logger.getLogger(ClusterManager.class.getName());
    private final ClusterConfig clusterConfig;
    private final ShardingStrategy shardingStrategy;
    private final List<ClusterNode> clusterNodes = new ArrayList<>();
    private final List<String> unhealthyNodeIds = new ArrayList<>();
    private final Map<String, ClusterNode> delegationMap = new HashMap<>(); // this maps down nodes to their delegate nodes
    private final ReentrantLock syncLock = new ReentrantLock(); // only one thread can execute syncDelegatedWrites
    private boolean initialized = false;
    private AppStatus systemStatus = AppStatus.GRAY;

    public ClusterManager(ClusterConfig clusterConfig, ShardingStrategy shardingStrategy) {
        this.clusterConfig = clusterConfig;
        this.shardingStrategy = shardingStrategy;
    }

    public void initializeClusterNodes() {
        this.clusterNodes.clear();
        try {
            clusterNodes.addAll(clusterConfig.getNodes());
            LOGGER.info("Initialized " + clusterNodes.size() + " cluster nodes from configuration");
            this.initialized = true;
        } catch (Exception e) {
            this.initialized = false;
            LOGGER.log(Level.SEVERE, "Failed to initialize cluster nodes", e);
            systemStatus = AppStatus.RED;
            throw new CodeRedException("Failed to initialize cluster nodes", e);
        }
    }

    public ClusterNode getShardedNode(String[] command) throws IllegalArgumentException, IllegalStateException, NoHealthyNodesAvailable {
        if (!initialized || clusterNodes.isEmpty()) {
            LOGGER.warning("Cluster nodes not initialized");
            throw new NoHealthyNodesAvailable("No healthy nodes available");
        }
        if (command.length < 3) {
            return clusterNodes.getFirst();
        }
        if (command[1].equalsIgnoreCase("SHUTDOWN")) {
            String nodeId = command[2];
            for (ClusterNode node : clusterNodes) {
                if (node.getId().equals(nodeId)) {
                    clusterNodes.remove(node);
                    return node;
                }
            }
            LOGGER.warning("Node with ID " + nodeId + " not found for shutdown");
            throw new IllegalArgumentException("Node with ID " + nodeId + " not found for shutdown");
        }
        String key = command[2];
        String operation = command[1].toUpperCase();
        ClusterNode node = shardingStrategy.getShardWithKey(key, clusterNodes);
        if (!unhealthyNodeIds.contains(node.getId())) {
            return node;
        }

        // this is the delegation mode - get a healthy node to handle the request
        ClusterNode delegateNode;
        if (operation.equals("GET")) {
            delegateNode = delegateRead(key, node);
        } else {
            // for WRITES, we want to delegate to another node and save the key-val in WAL, so we can sync later
            delegateNode = delegateWrite(key, operation, command.length > 3 ? command[3] : null, node);
        }
        return delegateNode;
    }

    private void syncDelegatedWrites(ClusterNode recoveredNode) {
        // TODO: failed sync recovery
        if (!syncLock.tryLock()) {
            LOGGER.info("Sync operation already in progress, skipping this attempt");
            return;
        }
        try {
            LOGGER.info("Starting sync of delegated writes");
            ClusterNode delegateNode = delegationMap.get(recoveredNode.getId());
            recoveredNode.setCanAccess(false); // hold off on any new requests to this node during sync

            Map<String, String[]> ops = recoveredNode.replayWal();
            for (Map.Entry<String, String[]> op : ops.entrySet()) {
                String[] parts = op.getValue();
                if (parts.length < 2) continue;
                String key = op.getKey();
                String operation = parts[0];
                String value = parts.length > 2 ? parts[2] : null;
                try {
                    if (operation.equals("SET")) {
                        recoveredNode.sendSet(key, value);
                        // might be easier to just run a scheduled cleanup job later
                        delegateNode.sendDelete(key);
                    } else if (operation.equals("DEL")) {
                        recoveredNode.sendDelete(key);
                    }
                } catch (Exception e) {
                    LOGGER.log(Level.WARNING, "Failed to sync delegated write to node " + recoveredNode.getId(), e);
                }
            }
            recoveredNode.clearWal();
            delegationMap.remove(recoveredNode.getId());
            unhealthyNodeIds.remove(recoveredNode.getId());
            LOGGER.info("Completed sync of delegated writes");
        } finally {
            recoveredNode.setCanAccess(true);
            syncLock.unlock();
        }
    }

    private ClusterNode delegateRead(String key, ClusterNode downNode) throws NoHealthyNodesAvailable {
        if (delegationMap.containsKey(downNode.getId())) {
            LOGGER.info("Delegating read operation for key " + key + " from down node " + downNode.getId());
            return delegationMap.get(downNode.getId());
        }
        return null;
    }

    private ClusterNode delegateWrite(String key, String operation, String value, ClusterNode downNode) throws NoHealthyNodesAvailable {
        // TODO: handle when delegate node goes down too
        downNode.logWal(operation, key, value); // WAL it for later reconciliation when node comes back up
        if (delegationMap.containsKey(downNode.getId())) {
            return delegationMap.get(downNode.getId());
        }
        ClusterNode delegateNode = shardingStrategy.getNextHealthyShard(clusterNodes);
        delegationMap.put(downNode.getId(), delegateNode);
        LOGGER.info("Delegating write operation for key " + key + " from down node " + downNode.getId() + " to delegate node " + delegateNode.getId());
        return delegateNode;
    }

    public AppStatus checkNodeHealths() {
        for (ClusterNode node : clusterNodes) {
            if (!node.isRunning()) {
                unhealthyNodeIds.add(node.getId());
                LOGGER.warning("Node " + node.getId() + " is unhealthy");
            } else {
                if (unhealthyNodeIds.contains(node.getId())) {
                    LOGGER.info("Node " + node.getId() + " has recovered and is now healthy");
                    syncDelegatedWrites(node); // sync any delegated writes
                }
                LOGGER.info("Node " + node.getId() + " is healthy");
            }
        }
        if (unhealthyNodeIds.isEmpty()) {
            systemStatus = AppStatus.GREEN;
        } else if (unhealthyNodeIds.size() == clusterNodes.size()) {
            systemStatus = AppStatus.RED;
        } else {
            systemStatus = AppStatus.YELLOW;
        }
        return systemStatus;
    }

    public void shutdownClusterNodes() {
        LOGGER.info("Shutting down cluster nodes...");
        for (ClusterNode node : clusterNodes) {
            try {
                node.shutdown();
                LOGGER.info("Node " + node.getId() + " shut down successfully.");
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Failed to shut down node: " + node.getId(), e);
            }
        }
        clusterNodes.clear();
        initialized = false;
    }

}
