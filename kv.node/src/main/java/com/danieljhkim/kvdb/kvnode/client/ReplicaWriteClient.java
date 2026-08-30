package com.danieljhkim.kvdb.kvnode.client;

import com.danieljhkim.kvdb.kvcommon.grpc.InternalAuthChannels;
import com.danieljhkim.kvdb.kvcommon.grpc.InternalAuthToken;
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
    private final String token;

    public ReplicaWriteClient(Duration rpcTimeout) {
        this(rpcTimeout, InternalAuthToken.resolve());
    }

    public ReplicaWriteClient(Duration rpcTimeout, String token) {
        this.rpcTimeout = Objects.requireNonNull(rpcTimeout, "rpcTimeout");
        this.token = token == null ? "" : token;
    }

    public SetResponse replicateSet(String targetAddress, ReplicateSetRequest req) {
        try {
            KVServiceGrpc.KVServiceBlockingStub stub = blockingStub(targetAddress);
            SetResponse response = stub.withDeadlineAfter(rpcTimeout.toMillis(), TimeUnit.MILLISECONDS)
                    .replicateSet(req);
            Metrics.increment("kvdb_replica_rpc_total", "node", "set", "ok");
            return response;
        } catch (RuntimeException e) {
            Metrics.increment("kvdb_replica_rpc_total", "node", "set", "error");
            throw e;
        }
    }

    public DeleteResponse replicateDelete(String targetAddress, ReplicateDeleteRequest req) {
        try {
            KVServiceGrpc.KVServiceBlockingStub stub = blockingStub(targetAddress);
            DeleteResponse response = stub.withDeadlineAfter(rpcTimeout.toMillis(), TimeUnit.MILLISECONDS)
                    .replicateDelete(req);
            Metrics.increment("kvdb_replica_rpc_total", "node", "delete", "ok");
            return response;
        } catch (RuntimeException e) {
            Metrics.increment("kvdb_replica_rpc_total", "node", "delete", "error");
            throw e;
        }
    }

    private KVServiceGrpc.KVServiceBlockingStub blockingStub(String address) {
        ManagedChannel ch = channelByAddress.computeIfAbsent(address, a -> {
            logger.debug("Creating replication channel to {}", a);
            return InternalAuthChannels.plaintextTarget(a, token);
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
