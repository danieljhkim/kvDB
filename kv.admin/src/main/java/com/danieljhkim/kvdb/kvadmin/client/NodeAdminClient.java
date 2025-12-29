package com.danieljhkim.kvdb.kvadmin.client;

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
		String[] parts = nodeAddress.split(":");
		if (parts.length != 2) {
			throw new IllegalArgumentException("Invalid node address format: " + nodeAddress);
		}

		String host = parts[0];
		int port = Integer.parseInt(parts[1]);

		ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
				.usePlaintext()
				.build();

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
		} finally {
			channel.shutdown();
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
}

