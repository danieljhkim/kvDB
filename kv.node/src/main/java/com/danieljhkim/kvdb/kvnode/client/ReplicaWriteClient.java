package com.danieljhkim.kvdb.kvnode.client;

import com.danieljhkim.kvdb.kvcommon.grpc.InternalAuthChannels;
import com.danieljhkim.kvdb.kvcommon.observability.Metrics;
import com.kvdb.proto.kvstore.*;
import io.grpc.ManagedChannel;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public ReplicationAck replicateMutation(String targetAddress, ReplicateMutationRequest req) {
        try {
            KVServiceGrpc.KVServiceBlockingStub stub = blockingStub(targetAddress);
            ReplicationAck response = stub.withDeadlineAfter(rpcTimeout.toMillis(), TimeUnit.MILLISECONDS)
                    .replicateMutation(req);
            Metrics.increment("kvdb_replica_rpc_total", "node", "mutation", "ok");
            return response;
        } catch (RuntimeException e) {
            Metrics.increment("kvdb_replica_rpc_total", "node", "mutation", "error");
            throw e;
        }
    }

    public ReplicaRepairResponse repairReplica(String targetAddress, ReplicaRepairRequest req) {
        try {
            KVServiceGrpc.KVServiceBlockingStub stub = blockingStub(targetAddress);
            ReplicaRepairResponse response = stub.withDeadlineAfter(rpcTimeout.toMillis(), TimeUnit.MILLISECONDS)
                    .repairReplica(req);
            Metrics.increment("kvdb_replica_rpc_total", "node", "repair", "ok");
            return response;
        } catch (RuntimeException e) {
            Metrics.increment("kvdb_replica_rpc_total", "node", "repair", "error");
            throw e;
        }
    }

    public ReplicaStateResponse fetchReplicaState(String targetAddress, ReplicaStateRequest req) {
        try {
            KVServiceGrpc.KVServiceBlockingStub stub = blockingStub(targetAddress);
            ReplicaStateResponse response = stub.withDeadlineAfter(rpcTimeout.toMillis(), TimeUnit.MILLISECONDS)
                    .fetchReplicaState(req);
            Metrics.increment("kvdb_replica_rpc_total", "node", "fetch_state", "ok");
            return response;
        } catch (RuntimeException e) {
            Metrics.increment("kvdb_replica_rpc_total", "node", "fetch_state", "error");
            throw e;
        }
    }

    private KVServiceGrpc.KVServiceBlockingStub blockingStub(String address) {
        ManagedChannel ch = channelByAddress.computeIfAbsent(address, a -> {
            logger.debug("Creating replication channel to {}", a);
            return InternalAuthChannels.forTarget(a);
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
