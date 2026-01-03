package com.danieljhkim.kvdb.kvnode.service;

import com.danieljhkim.kvdb.kvnode.cache.ShardMapCache;
import com.danieljhkim.kvdb.kvnode.client.ReplicaWriteClient;
import com.danieljhkim.kvdb.kvnode.cluster.ReplicationManager;
import com.danieljhkim.kvdb.kvnode.cluster.ShardLeadershipValidator;
import com.danieljhkim.kvdb.kvnode.cluster.ShardRouter;
import com.danieljhkim.kvdb.kvnode.storage.ShardKVStore;
import com.danieljhkim.kvdb.kvnode.storage.ShardStoreRegistry;
import com.danieljhkim.kvdb.proto.coordinator.ShardRecord;
import com.kvdb.proto.kvstore.*;
import io.grpc.stub.StreamObserver;
import java.time.Duration;

/**
 * gRPC service implementation for KV operations.
 * Delegates to specialized components for routing, leadership validation, and replication.
 */
public class KVServiceImpl extends KVServiceGrpc.KVServiceImplBase {

    private final String nodeId;
    private final ShardStoreRegistry shardStores;
    private final ShardRouter shardRouter;
    private final ShardLeadershipValidator leadershipValidator;
    private final ReplicationManager replicationManager;

    /**
     * Per-shard leader mode constructor: uses coordinator shard map to decide replica/leader for each key's shard.
     */
    public KVServiceImpl(String nodeId, ShardMapCache shardMapCache) {
        this.nodeId = nodeId;
        this.shardStores = null;
        this.shardRouter = new ShardRouter(shardMapCache, nodeId);
        this.leadershipValidator = new ShardLeadershipValidator(shardMapCache, nodeId);
        this.replicationManager = null;
    }

    /**
     * Full constructor with replication support.
     */
    public KVServiceImpl(
            String nodeId,
            ShardMapCache shardMapCache,
            ShardStoreRegistry shardStores,
            ReplicaWriteClient replicaWriteClient,
            Duration replicationTimeout) {
        this.nodeId = nodeId;
        this.shardStores = shardStores;
        this.shardRouter = new ShardRouter(shardMapCache, nodeId);
        this.leadershipValidator = new ShardLeadershipValidator(shardMapCache, nodeId);
        this.replicationManager = new ReplicationManager(nodeId, shardMapCache, replicaWriteClient, replicationTimeout);
    }

    /**
     * Constructor for testing with injected components.
     */
    public KVServiceImpl(
            String nodeId,
            ShardStoreRegistry shardStores,
            ShardRouter shardRouter,
            ShardLeadershipValidator leadershipValidator,
            ReplicationManager replicationManager) {
        this.nodeId = nodeId;
        this.shardStores = shardStores;
        this.shardRouter = shardRouter;
        this.leadershipValidator = leadershipValidator;
        this.replicationManager = replicationManager;
    }

    @Override
    public void get(KeyRequest request, StreamObserver<ValueResponse> responseObserver) {
        String key = request.getKey();

        // Route and validate
        String shardId = shardRouter.resolveShardId(key);
        leadershipValidator.validateReadReplica(shardId);

        // Execute read
        String value = shardStores.getOrCreate(shardId).get(key);

        ValueResponse response =
                ValueResponse.newBuilder().setValue(value != null ? value : "").build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void set(KeyValueRequest request, StreamObserver<SetResponse> responseObserver) {
        String key = request.getKey();
        String value = request.getValue();

        // Route and validate leadership
        String shardId = shardRouter.resolveShardId(key);
        leadershipValidator.validateWriteLeadership(shardId);

        // Execute local write
        shardStores.getOrCreate(shardId).set(key, value);

        // Replicate to followers
        if (replicationManager != null) {
            ShardRecord shard = shardRouter.getShardRecord(shardId);
            replicationManager.replicateSet(shardId, shard, key, value);
        }

        SetResponse response = SetResponse.newBuilder().setSuccess(true).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
        String key = request.getKey();

        // Route and validate leadership
        String shardId = shardRouter.resolveShardId(key);
        leadershipValidator.validateWriteLeadership(shardId);

        // Execute local delete
        boolean success = shardStores.getOrCreate(shardId).del(key);

        // Replicate to followers
        if (replicationManager != null) {
            ShardRecord shard = shardRouter.getShardRecord(shardId);
            replicationManager.replicateDelete(shardId, shard, key);
        }

        DeleteResponse response =
                DeleteResponse.newBuilder().setSuccess(success).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void replicateSet(ReplicateSetRequest request, StreamObserver<SetResponse> responseObserver) {
        String shardId = request.getShardId();
        String key = request.getKey();

        // Validate routing
        shardRouter.validateShardIdForKey(key, shardId);
        shardRouter.validateReplica(shardId);
        shardRouter.validateEpoch(shardId, request.getEpoch());

        // Execute local write
        ShardKVStore store = shardStores.getOrCreate(shardId);
        store.set(key, request.getValue());

        SetResponse response = SetResponse.newBuilder().setSuccess(true).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void replicateDelete(ReplicateDeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
        String shardId = request.getShardId();
        String key = request.getKey();

        // Validate routing
        shardRouter.validateShardIdForKey(key, shardId);
        shardRouter.validateReplica(shardId);
        shardRouter.validateEpoch(shardId, request.getEpoch());

        // Execute local delete
        ShardKVStore store = shardStores.getOrCreate(shardId);
        boolean success = store.del(key);

        DeleteResponse response =
                DeleteResponse.newBuilder().setSuccess(success).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void ping(PingRequest request, StreamObserver<PingResponse> responseObserver) {
        PingResponse response = PingResponse.newBuilder().setMessage("pong").build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void shutdown(ShutdownRequest request, StreamObserver<ShutdownResponse> responseObserver) {
        ShutdownResponse response =
                ShutdownResponse.newBuilder().setMessage("goodbye").build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
        System.exit(0);
    }
}
