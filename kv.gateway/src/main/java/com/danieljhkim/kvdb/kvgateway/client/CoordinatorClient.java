package com.danieljhkim.kvdb.kvgateway.client;

import com.danieljhkim.kvdb.proto.coordinator.ClusterState;
import com.danieljhkim.kvdb.proto.coordinator.CoordinatorGrpc;
import com.danieljhkim.kvdb.proto.coordinator.GetShardMapRequest;
import com.danieljhkim.kvdb.proto.coordinator.GetShardMapResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client for communicating with the Coordinator service. Used to fetch and refresh the shard map.
 */
public class CoordinatorClient {

    private static final Logger logger = LoggerFactory.getLogger(CoordinatorClient.class);

    private final ManagedChannel channel;
    private final CoordinatorGrpc.CoordinatorBlockingStub blockingStub;

    public CoordinatorClient(String host, int port) {
        // Register DNS resolver to bypass the resolver selection issue
        io.grpc.internal.DnsNameResolverProvider provider = new io.grpc.internal.DnsNameResolverProvider();
        io.grpc.NameResolverRegistry.getDefaultRegistry().register(provider);

        this.channel =
                ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        this.blockingStub = CoordinatorGrpc.newBlockingStub(channel);
        logger.info("CoordinatorClient created for {}:{}", host, port);
    }

    /**
     * Fetches the shard map from the coordinator.
     *
     * @param ifVersionGt Only return the map if version is greater than this value. Use 0 to always get the map.
     * @return The GetShardMapResponse containing the cluster state
     * @throws StatusRuntimeException if the RPC fails
     */
    public GetShardMapResponse getShardMap(long ifVersionGt) {
        GetShardMapRequest request =
                GetShardMapRequest.newBuilder().setIfVersionGt(ifVersionGt).build();

        try {
            return blockingStub.withDeadlineAfter(5, TimeUnit.SECONDS).getShardMap(request);
        } catch (StatusRuntimeException e) {
            logger.warn("Failed to fetch shard map from coordinator", e);
            throw e;
        }
    }

    /**
     * Fetches the full shard map from the coordinator (no version check).
     *
     * @return The cluster state, or null if unavailable
     */
    public ClusterState fetchShardMap() {
        try {
            GetShardMapResponse response = getShardMap(0);
            if (response.getNotModified()) {
                logger.debug("Shard map not modified");
                return null;
            }
            return response.getState();
        } catch (StatusRuntimeException e) {
            logger.warn("Failed to fetch shard map", e);
            return null;
        }
    }

    /**
     * Shuts down the channel gracefully.
     */
    public void shutdown() {
        logger.info("Shutting down CoordinatorClient");
        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.warn("Interrupted while shutting down CoordinatorClient");
            channel.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
