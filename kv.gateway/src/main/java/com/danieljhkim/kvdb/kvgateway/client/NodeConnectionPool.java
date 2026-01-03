package com.danieljhkim.kvdb.kvgateway.client;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kvdb.proto.kvstore.KVServiceGrpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

/**
 * Manages gRPC connections to storage nodes.
 * Creates channels on-demand and caches them by node address.
 */
public class NodeConnectionPool {

	private static final Logger logger = LoggerFactory.getLogger(NodeConnectionPool.class);

	private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
	private final Map<String, KVServiceGrpc.KVServiceBlockingStub> stubs = new ConcurrentHashMap<>();

	/**
	 * Gets a blocking stub for the given node address.
	 * Creates a new channel if one doesn't exist.
	 *
	 * @param nodeAddress
	 *            The node address in "host:port" format
	 * @return A blocking stub for the KVService
	 */
	public KVServiceGrpc.KVServiceBlockingStub getStub(String nodeAddress) {
		return stubs.computeIfAbsent(nodeAddress, addr -> {
			ManagedChannel channel = getOrCreateChannel(addr);
			return KVServiceGrpc.newBlockingStub(channel);
		});
	}

	private ManagedChannel getOrCreateChannel(String nodeAddress) {
		return channels.computeIfAbsent(nodeAddress, addr -> {
			// Use indexOf instead of split for better performance
			int colonIndex = addr.indexOf(':');
			if (colonIndex == -1) {
				throw new IllegalArgumentException("Invalid node address format (missing ':'): " + addr);
			}
			
			String host = addr.substring(0, colonIndex);
			String portStr = addr.substring(colonIndex + 1);
			int port;
			try {
				port = Integer.parseInt(portStr);
			} catch (NumberFormatException e) {
				throw new IllegalArgumentException("Invalid port in node address: " + addr, e);
			}

			// Register DNS resolver to bypass the resolver selection issue
			io.grpc.internal.DnsNameResolverProvider provider = new io.grpc.internal.DnsNameResolverProvider();
			io.grpc.NameResolverRegistry.getDefaultRegistry().register(provider);

			logger.info("Creating gRPC channel to storage node: {}", addr);
			return ManagedChannelBuilder.forAddress(host, port)
					.usePlaintext()
					.build();
		});
	}

	/**
	 * Closes all channels gracefully.
	 */
	public void closeAll() {
		logger.info("Closing all node connections");
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
		stubs.clear();
	}

	/**
	 * Removes a specific node from the pool (e.g., on failure).
	 *
	 * @param nodeAddress
	 *            The node address to remove
	 */
	public void removeNode(String nodeAddress) {
		stubs.remove(nodeAddress);
		ManagedChannel channel = channels.remove(nodeAddress);
		if (channel != null) {
			logger.info("Removing node from pool: {}", nodeAddress);
			channel.shutdownNow();
		}
	}
}
