package com.danieljhkim.kvdb.kvclustercoordinator.health;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftCommand;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftStateMachine;
import com.danieljhkim.kvdb.kvclustercoordinator.state.NodeRecord;
import com.danieljhkim.kvdb.kvclustercoordinator.state.ShardMapSnapshot;
import com.kvdb.proto.kvstore.KVServiceGrpc;
import com.kvdb.proto.kvstore.PingRequest;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

/**
 * Performs health checks on storage nodes by pinging them via gRPC.
 * Updates node status in the coordinator state machine based on health check
 * results.
 * Reuses gRPC channels and stubs for efficiency.
 */
public class NodeHealthChecker {

	private static final Logger LOGGER = Logger.getLogger(NodeHealthChecker.class.getName());
	private static final int PING_TIMEOUT_SECONDS = 5;

	private final RaftStateMachine raftStateMachine;
	private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
	private final Map<String, KVServiceGrpc.KVServiceBlockingStub> stubs = new ConcurrentHashMap<>();

	static {
		// Register DNS resolver once at class initialization
		io.grpc.internal.DnsNameResolverProvider provider = new io.grpc.internal.DnsNameResolverProvider();
		io.grpc.NameResolverRegistry.getDefaultRegistry().register(provider);
	}

	public NodeHealthChecker(RaftStateMachine raftStateMachine) {
		this.raftStateMachine = raftStateMachine;
	}

	/**
	 * Performs health checks on all registered nodes.
	 * Updates node status via Raft commands if status changes are detected.
	 */
	public void checkAllNodes() {
		if (!raftStateMachine.isLeader()) {
			// Only the leader performs health checks
			return;
		}

		ShardMapSnapshot snapshot = raftStateMachine.getSnapshot();
		for (NodeRecord node : snapshot.getNodes().values()) {
			checkNode(node);
		}
	}

	/**
	 * Checks the health of a single node and updates its status if needed.
	 */
	private void checkNode(NodeRecord node) {
		boolean isHealthy = pingNode(node.address());

		NodeRecord.NodeStatus currentStatus = node.status();
		NodeRecord.NodeStatus newStatus;

		if (isHealthy) {
			if (currentStatus == NodeRecord.NodeStatus.DEAD) {
				// Node recovered from DEAD
				newStatus = NodeRecord.NodeStatus.ALIVE;
				LOGGER.info("Node " + node.nodeId() + " recovered, marking as ALIVE");
			} else if (currentStatus == NodeRecord.NodeStatus.SUSPECT) {
				// Node recovered from SUSPECT
				newStatus = NodeRecord.NodeStatus.ALIVE;
				LOGGER.info("Node " + node.nodeId() + " recovered, marking as ALIVE");
			} else {
				// Node is already ALIVE, no change needed
				return;
			}
		} else {
			if (currentStatus == NodeRecord.NodeStatus.ALIVE) {
				// Node became unhealthy, mark as SUSPECT first
				newStatus = NodeRecord.NodeStatus.SUSPECT;
				LOGGER.warning("Node " + node.nodeId() + " is unhealthy, marking as SUSPECT");
			} else if (currentStatus == NodeRecord.NodeStatus.SUSPECT) {
				// Node still unhealthy after being SUSPECT, mark as DEAD
				newStatus = NodeRecord.NodeStatus.DEAD;
				LOGGER.warning("Node " + node.nodeId() + " is still unhealthy, marking as DEAD");
			} else {
				// Node is already DEAD, no change needed
				return;
			}
		}

		// Update node status via Raft command
		try {
			RaftCommand.SetNodeStatus command = new RaftCommand.SetNodeStatus(node.nodeId(), newStatus);
			raftStateMachine.apply(command).get(5, TimeUnit.SECONDS);
		} catch (Exception e) {
			LOGGER.log(Level.WARNING, "Failed to update node status for " + node.nodeId(), e);
		}
	}

	/**
	 * Pings a node via gRPC to check if it's healthy.
	 * Reuses cached channels and stubs for efficiency.
	 *
	 * @param address
	 *            Node address in "host:port" format
	 * @return true if node responds, false otherwise
	 */
	private boolean pingNode(String address) {
		String[] parts = address.split(":");
		if (parts.length != 2) {
			LOGGER.warning("Invalid node address format: " + address);
			return false;
		}

		String host = parts[0];
		int port;
		try {
			port = Integer.parseInt(parts[1]);
		} catch (NumberFormatException e) {
			LOGGER.warning("Invalid port in address: " + address);
			return false;
		}

		// Get or create stub for this address
		KVServiceGrpc.KVServiceBlockingStub stub = getOrCreateStub(address, host, port);
		if (stub == null) {
			return false;
		}

		try {
			PingRequest request = PingRequest.newBuilder().build();
			stub.withDeadlineAfter(PING_TIMEOUT_SECONDS, TimeUnit.SECONDS)
					.ping(request);
			return true;
		} catch (StatusRuntimeException e) {
			LOGGER.fine("Node " + address + " ping failed: " + e.getStatus().getDescription());
			// Remove unhealthy channel/stub from cache
			removeStub(address);
			return false;
		} catch (Exception e) {
			LOGGER.fine("Node " + address + " ping failed: " + e.getMessage());
			// Remove unhealthy channel/stub from cache
			removeStub(address);
			return false;
		}
	}

	/**
	 * Gets or creates a gRPC stub for the given address.
	 * Channels and stubs are cached and reused.
	 */
	private KVServiceGrpc.KVServiceBlockingStub getOrCreateStub(String address, String host, int port) {
		return stubs.computeIfAbsent(address, addr -> {
			ManagedChannel channel = channels.computeIfAbsent(addr, a -> {
				LOGGER.fine("Creating gRPC channel for health check: " + addr);
				return ManagedChannelBuilder.forAddress(host, port)
						.usePlaintext()
						.build();
			});
			return KVServiceGrpc.newBlockingStub(channel);
		});
	}

	/**
	 * Removes a channel and stub from cache (e.g., when node is unhealthy).
	 * The channel will be shut down.
	 */
	private void removeStub(String address) {
		stubs.remove(address);
		ManagedChannel channel = channels.remove(address);
		if (channel != null) {
			LOGGER.fine("Removing unhealthy channel: " + address);
			channel.shutdown();
			try {
				channel.awaitTermination(1, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				channel.shutdownNow();
			}
		}
	}

	/**
	 * Shuts down all cached channels.
	 * Should be called when the health checker is no longer needed.
	 */
	public void shutdown() {
		LOGGER.info("Shutting down NodeHealthChecker, closing " + channels.size() + " channels");
		stubs.clear();
		for (Map.Entry<String, ManagedChannel> entry : channels.entrySet()) {
			try {
				entry.getValue().shutdown();
				entry.getValue().awaitTermination(2, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				entry.getValue().shutdownNow();
			}
		}
		channels.clear();
	}
}
