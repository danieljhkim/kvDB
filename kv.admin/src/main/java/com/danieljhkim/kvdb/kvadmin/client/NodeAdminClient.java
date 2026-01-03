package com.danieljhkim.kvdb.kvadmin.client;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kvdb.proto.kvstore.KVServiceGrpc;
import com.kvdb.proto.kvstore.PingRequest;
import com.kvdb.proto.kvstore.PingResponse;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

/**
 * gRPC client for node admin operations (health, stats, compaction).
 */
public class NodeAdminClient {

	private static final Logger logger = LoggerFactory.getLogger(NodeAdminClient.class);

	private final long timeoutSeconds;
	private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();

	public NodeAdminClient(long timeout, TimeUnit timeUnit) {
		this.timeoutSeconds = timeUnit.toSeconds(timeout);
	}

	/**
	 * Ping a node to check if it's alive.
	 * 
	 * @param nodeAddress
	 *            Node address in format "host:port"
	 * @return true if node responds, false otherwise
	 */
	public boolean ping(String nodeAddress) {
		// Use indexOf instead of split for better performance
		int colonIndex = nodeAddress.indexOf(':');
		if (colonIndex == -1) {
			throw new IllegalArgumentException("Invalid node address format (missing ':'): " + nodeAddress);
		}

		String host = nodeAddress.substring(0, colonIndex);
		String portStr = nodeAddress.substring(colonIndex + 1);
		int port;
		try {
			port = Integer.parseInt(portStr);
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Invalid port in node address: " + nodeAddress, e);
		}

		// Reuse channel instead of creating new one on each ping
		ManagedChannel channel = channels.computeIfAbsent(nodeAddress, addr -> {
			logger.debug("Creating gRPC channel to node: {}", addr);
			return ManagedChannelBuilder.forAddress(host, port)
					.usePlaintext()
					.build();
		});

		try {
			KVServiceGrpc.KVServiceBlockingStub stub = KVServiceGrpc.newBlockingStub(channel);
			PingRequest request = PingRequest.newBuilder().build();
			PingResponse response = stub
					.withDeadlineAfter(timeoutSeconds, TimeUnit.SECONDS)
					.ping(request);
			return response != null;
		} catch (StatusRuntimeException e) {
			logger.warn("Failed to ping node: {}", nodeAddress, e);
			return false;
		}
	}

	/**
	 * Trigger compaction on a node.
	 * 
	 * @param nodeAddress
	 *            Node address in format "host:port"
	 */
	public void triggerCompaction(String nodeAddress) {
		// TODO: Implement compaction RPC when available in kvstore.proto
		logger.info("Triggering compaction on node: {}", nodeAddress);
	}

	/**
	 * Close all channels gracefully. Should be called on shutdown.
	 */
	public void close() {
		logger.info("Closing all node admin client channels");
		for (Map.Entry<String, ManagedChannel> entry : channels.entrySet()) {
			try {
				entry.getValue().shutdown().awaitTermination(5, TimeUnit.SECONDS);
				logger.debug("Closed channel to {}", entry.getKey());
			} catch (InterruptedException e) {
				logger.warn("Interrupted while closing channel to {}", entry.getKey());
				entry.getValue().shutdownNow();
				Thread.currentThread().interrupt();
			}
		}
		channels.clear();
	}
}
