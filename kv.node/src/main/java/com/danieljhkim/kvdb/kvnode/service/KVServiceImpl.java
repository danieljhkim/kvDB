package com.danieljhkim.kvdb.kvnode.service;

import com.danieljhkim.kvdb.kvcommon.cache.ShardMapCache;
import com.danieljhkim.kvdb.kvnode.client.ReplicaWriteClient;
import com.danieljhkim.kvdb.kvnode.cluster.ReplicationManager;
import com.danieljhkim.kvdb.kvnode.cluster.ReplicationManager.MutationResult;
import com.danieljhkim.kvdb.kvnode.cluster.ShardLeadershipValidator;
import com.danieljhkim.kvdb.kvnode.cluster.ShardRouter;
import com.danieljhkim.kvdb.kvnode.storage.ShardKVStore;
import com.danieljhkim.kvdb.kvnode.storage.ShardStoreRegistry;
import com.danieljhkim.kvdb.proto.coordinator.ShardRecord;
import com.kvdb.proto.kvstore.*;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.UUID;

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
        this.replicationManager =
                new ReplicationManager(nodeId, shardMapCache, shardStores, replicaWriteClient, replicationTimeout);
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
        if (replicationManager != null && leadershipValidator.isLeader(shardId)) {
            replicationManager.ensureLeaderReconciled(shardId, shardRouter.getShardRecord(shardId));
        }

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

        MutationResult result;
        if (replicationManager == null) {
            shardStores.getOrCreate(shardId).set(key, value);
            result = new MutationResult(stableRequestId(request.getRequestId()), 0, 1);
        } else {
            ShardRecord shard = shardRouter.getShardRecord(shardId);
            result = replicationManager.replicateSet(
                    shardId,
                    shard,
                    key,
                    value,
                    stableRequestId(request.getRequestId()),
                    normalizedDurability(request.getDurability()));
        }

        SetResponse response = SetResponse.newBuilder()
                .setSuccess(true)
                .setRequestId(result.requestId())
                .setVersion(result.version())
                .setDurable(true)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
        String key = request.getKey();

        // Route and validate leadership
        String shardId = shardRouter.resolveShardId(key);
        leadershipValidator.validateWriteLeadership(shardId);

        boolean success;
        MutationResult result;
        if (replicationManager == null) {
            success = shardStores.getOrCreate(shardId).del(key);
            result = new MutationResult(stableRequestId(request.getRequestId()), 0, 1);
        } else {
            ShardRecord shard = shardRouter.getShardRecord(shardId);
            result = replicationManager.replicateDelete(
                    shardId,
                    shard,
                    key,
                    stableRequestId(request.getRequestId()),
                    normalizedDurability(request.getDurability()));
            // A committed tombstone is a successful, idempotent delete even if the key was already absent.
            success = true;
        }

        DeleteResponse response = DeleteResponse.newBuilder()
                .setSuccess(success)
                .setRequestId(result.requestId())
                .setVersion(result.version())
                .setDurable(true)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void replicateMutation(ReplicateMutationRequest request, StreamObserver<ReplicationAck> responseObserver) {
        if (!request.hasMutation()) {
            responseObserver.onNext(rejectedReplication("mutation is required", 0, 0));
            responseObserver.onCompleted();
            return;
        }
        ReplicatedMutation mutation = request.getMutation();
        String shardId = mutation.getShardId();
        shardRouter.validateShardIdForKey(mutation.getKey(), shardId);
        shardRouter.validateReplica(shardId);
        shardRouter.validateEpoch(shardId, mutation.getEpoch());

        ShardKVStore store = shardStores.getOrCreate(shardId);
        ShardKVStore.MutationStatus status =
                switch (request.getPhase()) {
                    case PREPARE -> store.prepareMutation(mutation);
                    case COMMIT -> store.commitMutation(mutation);
                    case ABORT -> store.abortMutation(mutation);
                    default -> new ShardKVStore.MutationStatus(
                            false, false, "replication phase is required", store.committedVersion());
                };
        responseObserver.onNext(ReplicationAck.newBuilder()
                .setSuccess(status.success())
                .setDurable(status.durable())
                .setEpoch(store.shardEpoch())
                .setCommittedVersion(status.committedVersion())
                .setMessage(status.message())
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void repairReplica(ReplicaRepairRequest request, StreamObserver<ReplicaRepairResponse> responseObserver) {
        String shardId = request.getShardId();
        shardRouter.validateReplica(shardId);
        shardRouter.validateEpoch(shardId, request.getEpoch());
        ShardKVStore store = shardStores.getOrCreate(shardId);
        int applied = 0;
        for (ReplicatedMutation mutation : request.getCommittedMutationsList()) {
            if (!shardId.equals(mutation.getShardId())) {
                responseObserver.onNext(ReplicaRepairResponse.newBuilder()
                        .setSuccess(false)
                        .setDurable(false)
                        .setAppliedMutations(applied)
                        .setCommittedVersion(store.committedVersion())
                        .build());
                responseObserver.onCompleted();
                return;
            }
            shardRouter.validateShardIdForKey(mutation.getKey(), shardId);
            ShardKVStore.MutationStatus status = store.repairMutation(mutation);
            if (!status.success() || !status.durable()) {
                responseObserver.onNext(ReplicaRepairResponse.newBuilder()
                        .setSuccess(false)
                        .setDurable(false)
                        .setAppliedMutations(applied)
                        .setCommittedVersion(store.committedVersion())
                        .build());
                responseObserver.onCompleted();
                return;
            }
            if ("repaired".equals(status.message())) {
                applied++;
            }
        }
        responseObserver.onNext(ReplicaRepairResponse.newBuilder()
                .setSuccess(true)
                .setDurable(true)
                .setAppliedMutations(applied)
                .setCommittedVersion(store.committedVersion())
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void fetchReplicaState(ReplicaStateRequest request, StreamObserver<ReplicaStateResponse> responseObserver) {
        String shardId = request.getShardId();
        shardRouter.validateReplica(shardId);
        shardRouter.validateEpoch(shardId, request.getEpoch());
        ShardKVStore store = shardStores.getOrCreate(shardId);
        int limit = Math.min(128, Math.max(1, request.getMaxMutations()));
        var mutations = store.committedMutationsAfter(request.getAfterVersion(), limit);
        long lastVersion = mutations.isEmpty()
                ? request.getAfterVersion()
                : mutations.getLast().getVersion();
        boolean hasMore = !store.committedMutationsAfter(lastVersion, 1).isEmpty();
        responseObserver.onNext(ReplicaStateResponse.newBuilder()
                .setSuccess(true)
                .setDurable(true)
                .addAllCommittedMutations(mutations)
                .setHasMore(hasMore)
                .setCommittedVersion(store.committedVersion())
                .build());
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

    public void shutdownReplication() {
        if (replicationManager != null) {
            replicationManager.close();
        }
    }

    private static String stableRequestId(String requestId) {
        return requestId == null || requestId.isBlank() ? UUID.randomUUID().toString() : requestId;
    }

    private static WriteDurability normalizedDurability(WriteDurability durability) {
        return durability == WriteDurability.ALL_SYNC ? WriteDurability.ALL_SYNC : WriteDurability.QUORUM_SYNC;
    }

    private static ReplicationAck rejectedReplication(String message, long epoch, long committedVersion) {
        return ReplicationAck.newBuilder()
                .setSuccess(false)
                .setDurable(false)
                .setEpoch(epoch)
                .setCommittedVersion(committedVersion)
                .setMessage(message)
                .build();
    }
}
