package com.danieljhkim.kvdb.kvgateway.cluster;

import com.danieljhkim.kvdb.kvgateway.config.ClusterConfig;
import com.danieljhkim.kvdb.kvgateway.sharding.ShardingStrategy;
import com.danieljhkim.kvdb.kvcommon.constants.AppStatus;
import com.danieljhkim.kvdb.kvcommon.exception.CodeRedException;
import com.danieljhkim.kvdb.kvcommon.exception.NoHealthyNodesAvailable;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ClusterManager {

	private static final Logger LOGGER = Logger.getLogger(ClusterManager.class.getName());

	private final ClusterConfig clusterConfig;
	private final ShardingStrategy shardingStrategy;

	// Thread-safe collections: node count is small, CopyOnWriteArrayList is fine.
	private final List<ClusterNode> clusterNodes = new CopyOnWriteArrayList<>();
	private final Set<String> unhealthyNodeIds = ConcurrentHashMap.newKeySet();

	/** Maps down nodes to their delegate nodes for delegated writes/reads. */
	private final Map<String, ClusterNode> delegationMap = new ConcurrentHashMap<>();

	/** Only one thread should run syncDelegatedWrites at a time. */
	private final ReentrantLock syncLock = new ReentrantLock();

	private volatile boolean initialized = false;
	private volatile AppStatus systemStatus = AppStatus.GRAY;

	public ClusterManager(ClusterConfig clusterConfig, ShardingStrategy shardingStrategy) {
		this.clusterConfig = clusterConfig;
		this.shardingStrategy = shardingStrategy;
	}

	public void initializeClusterNodes() {
		clusterNodes.clear();
		unhealthyNodeIds.clear();
		delegationMap.clear();

		try {
			clusterNodes.addAll(clusterConfig.getNodes());
			LOGGER.info("Initialized " + clusterNodes.size() + " cluster nodes from configuration");
			this.initialized = true;
			this.systemStatus = clusterNodes.isEmpty() ? AppStatus.RED : AppStatus.GREEN;
		} catch (Exception e) {
			this.initialized = false;
			LOGGER.log(Level.SEVERE, "Failed to initialize cluster nodes", e);
			systemStatus = AppStatus.RED;
			throw new CodeRedException("Failed to initialize cluster nodes", e);
		}
	}

	public ClusterNode getShardedNode(String[] command)
			throws IllegalArgumentException, IllegalStateException, NoHealthyNodesAvailable {

		if (!initialized || clusterNodes.isEmpty()) {
			LOGGER.warning("Cluster nodes not initialized or empty");
			throw new NoHealthyNodesAvailable("No healthy nodes available");
		}

		// Commands with no key → just route to the first node (e.g., management
		// commands)
		if (command.length < 3) {
			return clusterNodes.getFirst();
		}

		String op = command[1].toUpperCase();

		// Special case: shutdown request for a specific node
		if ("SHUTDOWN".equals(op)) {
			String nodeId = command[2];
			ClusterNode target = null;
			for (ClusterNode node : clusterNodes) {
				if (node.getId().equals(nodeId)) {
					target = node;
					break;
				}
			}
			if (target != null) {
				clusterNodes.remove(target);
				unhealthyNodeIds.remove(target.getId());
				delegationMap.remove(target.getId());
				LOGGER.info("Node " + nodeId + " removed from clusterNodes via SHUTDOWN command");
				return target;
			}
			LOGGER.warning("Node with ID " + nodeId + " not found for shutdown");
			throw new IllegalArgumentException(
					"Node with ID " + nodeId + " not found for shutdown");
		}

		String key = command[2];
		ClusterNode node = shardingStrategy.getShardWithKey(key, clusterNodes);
		if (node == null) {
			LOGGER.warning("Sharding strategy returned null for key=" + key);
			throw new NoHealthyNodesAvailable("No healthy nodes available");
		}

		// Primary node is healthy → go there
		if (!unhealthyNodeIds.contains(node.getId())) {
			return node;
		}

		// Node is unhealthy → delegate
		if ("GET".equals(op)) {
			ClusterNode delegate = delegateRead(key, node);
			if (delegate == null) {
				LOGGER.warning(
						"No delegate available for read key="
								+ key
								+ " from down node "
								+ node.getId());
				throw new NoHealthyNodesAvailable("No healthy nodes available for delegated read");
			}
			return delegate;
		} else {
			String value = (command.length > 3) ? command[3] : null;
			return delegateWrite(key, op, value, node);
		}
	}

	private void syncDelegatedWrites(ClusterNode recoveredNode) {
		// TODO: failed sync recovery & better retry behavior
		if (!syncLock.tryLock()) {
			LOGGER.info("Sync operation already in progress, skipping this attempt");
			return;
		}
		try {
			LOGGER.info("Starting sync of delegated writes for node " + recoveredNode.getId());
			ClusterNode delegateNode = delegationMap.get(recoveredNode.getId());
			recoveredNode.setCanAccess(false); // hold off on new requests during sync

			Map<String, String[]> ops = recoveredNode.replayWal();
			for (Map.Entry<String, String[]> opEntry : ops.entrySet()) {
				String[] parts = opEntry.getValue();
				if (parts == null || parts.length < 1) {
					continue;
				}
				String key = opEntry.getKey();
				String operation = parts[0];
				String value = (parts.length > 2) ? parts[2] : null;

				try {
					switch (operation) {
						case "SET" -> {
							recoveredNode.sendSet(key, value);
							if (delegateNode != null) {
								// Best-effort cleanup on delegate
								delegateNode.sendDelete(key);
							}
						}
						case "DEL" -> recoveredNode.sendDelete(key);
						default ->
							LOGGER.warning(
									"Unknown WAL operation '"
											+ operation
											+ "' for key="
											+ key
											+ " on node "
											+ recoveredNode.getId());
					}
				} catch (Exception e) {
					LOGGER.log(
							Level.WARNING,
							"Failed to sync delegated write to node "
									+ recoveredNode.getId()
									+ " for key="
									+ key,
							e);
					// TODO: consider re-logging unsynced ops for a later retry
				}
			}

			recoveredNode.clearWal();
			delegationMap.remove(recoveredNode.getId());
			unhealthyNodeIds.remove(recoveredNode.getId());
			LOGGER.info("Completed sync of delegated writes for node " + recoveredNode.getId());
		} finally {
			recoveredNode.setCanAccess(true);
			syncLock.unlock();
		}
	}

	/**
	 * Delegate a read to an existing or newly chosen healthy node. For GETs, it's
	 * safe to route to
	 * any healthy node that has the data.
	 */
	private ClusterNode delegateRead(String key, ClusterNode downNode)
			throws NoHealthyNodesAvailable {
		ClusterNode existing = delegationMap.get(downNode.getId());
		if (existing != null && !unhealthyNodeIds.contains(existing.getId())) {
			LOGGER.info(
					"Delegating read operation for key "
							+ key
							+ " from down node "
							+ downNode.getId()
							+ " to existing delegate node "
							+ existing.getId());
			return existing;
		}

		// Choose a new healthy delegate
		ClusterNode delegateNode = shardingStrategy.getNextHealthyShard(clusterNodes);
		if (delegateNode == null) {
			throw new NoHealthyNodesAvailable("No healthy nodes available for delegated read");
		}

		delegationMap.put(downNode.getId(), delegateNode);
		LOGGER.info(
				"Delegating read operation for key "
						+ key
						+ " from down node "
						+ downNode.getId()
						+ " to new delegate node "
						+ delegateNode.getId());
		return delegateNode;
	}

	private ClusterNode delegateWrite(
			String key, String operation, String value, ClusterNode downNode)
			throws NoHealthyNodesAvailable {

		// WAL it for later reconciliation when node comes back up
		downNode.logWal(operation, key, value);

		ClusterNode existing = delegationMap.get(downNode.getId());
		if (existing != null && !unhealthyNodeIds.contains(existing.getId())) {
			LOGGER.info(
					"Using existing delegate node "
							+ existing.getId()
							+ " for write key="
							+ key
							+ " from down node "
							+ downNode.getId());
			return existing;
		}

		ClusterNode delegateNode = shardingStrategy.getNextHealthyShard(clusterNodes);
		if (delegateNode == null) {
			throw new NoHealthyNodesAvailable("No healthy nodes available for delegated write");
		}

		delegationMap.put(downNode.getId(), delegateNode);
		LOGGER.info(
				"Delegating write operation for key "
						+ key
						+ " from down node "
						+ downNode.getId()
						+ " to delegate node "
						+ delegateNode.getId());
		return delegateNode;
	}

	public AppStatus checkNodeHealths() {
		for (ClusterNode node : clusterNodes) {
			boolean running = node.isRunning();
			boolean wasUnhealthy = unhealthyNodeIds.contains(node.getId());

			if (!running) {
				if (!wasUnhealthy) {
					unhealthyNodeIds.add(node.getId());
					LOGGER.warning("Node " + node.getId() + " is unhealthy");
				}
			} else {
				if (wasUnhealthy) {
					LOGGER.info("Node " + node.getId() + " has recovered and is now healthy");
					syncDelegatedWrites(node);
				} else {
					LOGGER.fine("Node " + node.getId() + " is healthy");
				}
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
		unhealthyNodeIds.clear();
		delegationMap.clear();
		initialized = false;
		systemStatus = AppStatus.RED;
	}
}
