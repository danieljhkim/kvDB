package com.danieljhkim.kvdb.kvclustercoordinator.cluster;

import com.danieljhkim.kvdb.kvcommon.persistence.WALManager;

import lombok.Getter;

import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

@Getter
public class ClusterNode {

	private static final Logger LOGGER = Logger.getLogger(ClusterNode.class.getName());

	private final String id;
	private final ClusterNodeClient client;
	private final String host;
	private final int port;
	private final boolean grpc;

	/** WAL filename used for delegated writes when this node is down. */
	private final String walFileName;

	private final WALManager walManager;
	private final ReentrantLock accessLock = new ReentrantLock();
	private final Condition accessCondition = accessLock.newCondition();

	/**
	 * Set to false during recovery / WAL replay so we don't serve new requests
	 * while state is being
	 * reconciled.
	 */
	private volatile boolean canAccess = true;

	public ClusterNode(String id, String host, int port, boolean useGrpc) {
		this.id = id;
		this.host = host;
		this.port = port;
		this.grpc = useGrpc;
		this.client = useGrpc
				? new GrpcClusterNodeClient(host, port)
				: new HttpClusterNodeClient(host, port);

		String walPath = "data/" + id + ".wal";
		this.walManager = new WALManager(walPath);
		this.walFileName = walPath;
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
		if (!waitForAccess()) {
			return null; // or some NIL sentinel, depending on your protocol
		}
		return client.sendGet(key);
	}

	public boolean sendDelete(String key) {
		if (!waitForAccess()) {
			return false;
		}
		return client.sendDelete(key);
	}

	/**
	 * Actively ping the node to check if it's running. This does NOT cache state;
	 * it always calls
	 * through to the client.
	 */
	public boolean isRunning() {
		try {
			boolean healthy = this.client.ping();
			if (!healthy) {
				LOGGER.warning("Ping to node " + id + " returned unhealthy");
			}
			return healthy;
		} catch (Exception e) {
			LOGGER.severe("Ping to node " + id + " failed: " + e.getMessage());
			return false;
		}
	}

	public void shutdown() {
		try {
			client.shutdown();
		} catch (Exception e) {
			LOGGER.warning("Error shutting down client for node " + id + ": " + e.getMessage());
		}
		clearWal();
		try {
			walManager.close();
		} catch (Exception e) {
			LOGGER.warning("Error closing WAL manager for node " + id + ": " + e.getMessage());
		}
	}

	public void logWal(String operation, String key, String value) {
		walManager.log(operation, key, value);
	}

	public void clearWal() {
		walManager.clear();
	}

	/** Return the latest WAL operations per key (for recovery / sync). */
	public Map<String, String[]> replayWal() {
		return walManager.replayAsMap();
	}

	/**
	 * Enable or disable access to this node. When disabled, calls to
	 * sendSet/sendGet/sendDelete
	 * will block (unless interrupted).
	 */
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

	/**
	 * Block until this node is accessible again, or return false if interrupted.
	 */
	private boolean waitForAccess() {
		accessLock.lock();
		try {
			while (!canAccess) {
				accessCondition.await();
			}
			return true;
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			LOGGER.warning("Thread interrupted while waiting for access to node " + id);
			return false;
		} finally {
			accessLock.unlock();
		}
	}
}
