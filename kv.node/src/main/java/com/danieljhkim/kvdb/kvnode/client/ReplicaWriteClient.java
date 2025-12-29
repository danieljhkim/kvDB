package com.danieljhkim.kvdb.kvnode.client;

import com.kvdb.proto.kvstore.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * Node-to-node client for synchronous replication.
 *
 * <p>
 * Uses a simple channel cache keyed by target address.
 */
public class ReplicaWriteClient {

	private static final Logger logger = LoggerFactory.getLogger(ReplicaWriteClient.class);

	private final ConcurrentMap<String, ManagedChannel> channelByAddress = new ConcurrentHashMap<>();
	private final Duration rpcTimeout;

	public ReplicaWriteClient(Duration rpcTimeout) {
		this.rpcTimeout = Objects.requireNonNull(rpcTimeout, "rpcTimeout");
	}

	public SetResponse replicateSet(String targetAddress, ReplicateSetRequest req) {
		KVServiceGrpc.KVServiceBlockingStub stub = blockingStub(targetAddress);
		return stub.withDeadlineAfter(rpcTimeout.toMillis(), TimeUnit.MILLISECONDS).replicateSet(req);
	}

	public DeleteResponse replicateDelete(String targetAddress, ReplicateDeleteRequest req) {
		KVServiceGrpc.KVServiceBlockingStub stub = blockingStub(targetAddress);
		return stub.withDeadlineAfter(rpcTimeout.toMillis(), TimeUnit.MILLISECONDS).replicateDelete(req);
	}

	private KVServiceGrpc.KVServiceBlockingStub blockingStub(String address) {
		ManagedChannel ch = channelByAddress.computeIfAbsent(address, a -> {
			logger.debug("Creating replication channel to {}", a);
			return ManagedChannelBuilder.forTarget(a).usePlaintext().build();
		});
		return KVServiceGrpc.newBlockingStub(ch);
	}

	public void shutdown() {
		for (ManagedChannel ch : channelByAddress.values()) {
			try {
				ch.shutdown();
			} catch (Exception e) {
				// ignore
			}
		}
		channelByAddress.clear();
	}
}
