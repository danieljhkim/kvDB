package com.danieljhkim.kvdb.kvnode.client;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.danieljhkim.kvdb.proto.coordinator.ClusterState;
import com.danieljhkim.kvdb.proto.coordinator.CoordinatorGrpc;
import com.danieljhkim.kvdb.proto.coordinator.GetShardMapRequest;
import com.danieljhkim.kvdb.proto.coordinator.GetShardMapResponse;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

/**
 * Node-side client for communicating with the Coordinator service.
 * Used to fetch shard map snapshots for leader/replica validation.
 */
public class CoordinatorShardMapClient {

	private static final Logger logger = LoggerFactory.getLogger(CoordinatorShardMapClient.class);

	private final ManagedChannel channel;
	private final CoordinatorGrpc.CoordinatorBlockingStub blockingStub;

	public CoordinatorShardMapClient(String host, int port) {
		// Register DNS resolver to bypass the resolver selection issue
		io.grpc.internal.DnsNameResolverProvider provider = new io.grpc.internal.DnsNameResolverProvider();
		io.grpc.NameResolverRegistry.getDefaultRegistry().register(provider);

		this.channel = ManagedChannelBuilder.forAddress(host, port)
				.usePlaintext()
				.build();
		this.blockingStub = CoordinatorGrpc.newBlockingStub(channel);
		logger.info("CoordinatorShardMapClient created for {}:{}", host, port);
	}

	public GetShardMapResponse getShardMap(long ifVersionGt) {
		GetShardMapRequest request = GetShardMapRequest.newBuilder()
				.setIfVersionGt(ifVersionGt)
				.build();
		return blockingStub
				.withDeadlineAfter(5, TimeUnit.SECONDS)
				.getShardMap(request);
	}

	/**
	 * Fetches the shard map snapshot.
	 *
	 * @param ifVersionGt
	 *            only return map if greater than this version
	 * @return ClusterState or null if not modified/unavailable
	 */
	public ClusterState fetchShardMap(long ifVersionGt) {
		try {
			GetShardMapResponse resp = getShardMap(ifVersionGt);
			if (resp.getNotModified()) {
				return null;
			}
			return resp.getState();
		} catch (StatusRuntimeException e) {
			logger.warn("Failed to fetch shard map from coordinator", e);
			return null;
		}
	}

	public void shutdown() {
		try {
			channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			channel.shutdownNow();
		}
	}
}
