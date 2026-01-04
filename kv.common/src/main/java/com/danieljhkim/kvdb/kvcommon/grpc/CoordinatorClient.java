package com.danieljhkim.kvdb.kvcommon.grpc;

import com.danieljhkim.kvdb.proto.coordinator.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Single-node client for communicating with a Coordinator.
 * For multi-node leader-aware failover, use CoordinatorClientManager.
 */
public class CoordinatorClient {

    private static final Logger logger = LoggerFactory.getLogger(CoordinatorClient.class);
    private static final int TIMEOUT_SECONDS = 5;

    @Getter
    private final String address;

    @Getter
    private final ManagedChannel channel;

    private final CoordinatorGrpc.CoordinatorBlockingStub blockingStub;

    @Getter
    private final CoordinatorGrpc.CoordinatorStub asyncStub;

    public CoordinatorClient(String host, int port) {
        this.address = host + ":" + port;
        this.channel =
                ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        this.blockingStub = CoordinatorGrpc.newBlockingStub(channel);
        this.asyncStub = CoordinatorGrpc.newStub(channel);
        logger.info("CoordinatorClient created for {}", address);
    }

    private CoordinatorGrpc.CoordinatorBlockingStub getStub() {
        return blockingStub.withDeadlineAfter(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    public CoordinatorGrpc.CoordinatorBlockingStub getBlockingStub() {
        return getStub();
    }

    public String fetchLeaderId() {
        try {
            GetCoordinatorLeaderResponse response =
                    getStub().getCoordinatorLeader(GetCoordinatorLeaderRequest.getDefaultInstance());
            return response.getLeaderId().isEmpty() ? null : response.getLeaderId();
        } catch (StatusRuntimeException e) {
            logger.warn("Failed to fetch leader ID: {}", e.getStatus());
            return null;
        }
    }

    public GetShardMapResponse getShardMap(long ifVersionGt) {
        return getStub()
                .getShardMap(GetShardMapRequest.newBuilder()
                        .setIfVersionGt(ifVersionGt)
                        .build());
    }

    public ClusterState fetchShardMap(long ifVersionGt) {
        try {
            GetShardMapResponse resp = getShardMap(ifVersionGt);
            if (resp.getNotModified()) {
                return null;
            }
            return resp.getState();
        } catch (StatusRuntimeException e) {
            logger.warn("Failed to fetch shard map: {}", e.getStatus());
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
