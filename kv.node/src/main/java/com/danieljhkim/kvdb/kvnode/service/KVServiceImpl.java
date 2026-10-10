package com.danieljhkim.kvdb.kvnode.service;

import com.danieljhkim.kvdb.kvcommon.cache.ShardMapCache;
import com.danieljhkim.kvdb.kvcommon.config.AppConfig;
import com.danieljhkim.kvdb.kvcommon.exception.InvalidRequestException;
import com.danieljhkim.kvdb.kvcommon.exception.NodeUnavailableException;
import com.danieljhkim.kvdb.kvcommon.exception.PayloadTooLargeException;
import com.danieljhkim.kvdb.kvcommon.exception.PermissionDeniedException;
import com.danieljhkim.kvdb.kvcommon.grpc.GrpcIdentity;
import com.danieljhkim.kvdb.kvcommon.grpc.GrpcPeerIdentity;
import com.danieljhkim.kvdb.kvcommon.limits.KvRequestLimits;
import com.danieljhkim.kvdb.kvnode.client.ReplicaWriteClient;
import com.danieljhkim.kvdb.kvnode.cluster.ReplicationManager;
import com.danieljhkim.kvdb.kvnode.cluster.ReplicationManager.MutationResult;
import com.danieljhkim.kvdb.kvnode.cluster.ShardLeadershipValidator;
import com.danieljhkim.kvdb.kvnode.cluster.ShardRouter;
import com.danieljhkim.kvdb.kvnode.storage.ShardKVStore;
import com.danieljhkim.kvdb.kvnode.storage.ShardStoreRegistry;
import com.danieljhkim.kvdb.proto.coordinator.ShardRecord;
import com.google.protobuf.CodedOutputStream;
import com.kvdb.proto.kvstore.*;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.List;
import java.util.OptionalLong;
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
    private final KvRequestLimits limits;

    /**
     * Per-shard leader mode constructor: uses coordinator shard map to decide replica/leader for each key's shard.
     */
    public KVServiceImpl(String nodeId, ShardMapCache shardMapCache) {
        this.nodeId = nodeId;
        this.shardStores = null;
        this.shardRouter = new ShardRouter(shardMapCache, nodeId);
        this.leadershipValidator = new ShardLeadershipValidator(shardMapCache, nodeId);
        this.replicationManager = null;
        this.limits = new KvRequestLimits(null);
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
        this(nodeId, shardMapCache, shardStores, replicaWriteClient, replicationTimeout, new AppConfig.LimitsConfig());
    }

    public KVServiceImpl(
            String nodeId,
            ShardMapCache shardMapCache,
            ShardStoreRegistry shardStores,
            ReplicaWriteClient replicaWriteClient,
            Duration replicationTimeout,
            AppConfig.LimitsConfig limitsConfig) {
        this.nodeId = nodeId;
        this.shardStores = shardStores;
        this.shardRouter = new ShardRouter(shardMapCache, nodeId);
        this.leadershipValidator = new ShardLeadershipValidator(shardMapCache, nodeId);
        this.limits = new KvRequestLimits(limitsConfig);
        this.replicationManager = new ReplicationManager(
                nodeId, shardMapCache, shardStores, replicaWriteClient, replicationTimeout, limits);
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
        this(nodeId, shardStores, shardRouter, leadershipValidator, replicationManager, new KvRequestLimits(null));
    }

    KVServiceImpl(
            String nodeId,
            ShardStoreRegistry shardStores,
            ShardRouter shardRouter,
            ShardLeadershipValidator leadershipValidator,
            ReplicationManager replicationManager,
            KvRequestLimits limits) {
        this.nodeId = nodeId;
        this.shardStores = shardStores;
        this.shardRouter = shardRouter;
        this.leadershipValidator = leadershipValidator;
        this.replicationManager = replicationManager;
        this.limits = limits;
    }

    @Override
    public void get(KeyRequest request, StreamObserver<ValueResponse> responseObserver) {
        limits.validateMessage(request);
        limits.validateKey(request.getKey());
        var key = request.getKey();

        // EVENTUAL reads may use any replica. STRONG reads are leader-only and
        // cross a fresh quorum barrier before observing local committed state.
        String shardId = shardRouter.resolveShardId(key);
        if (request.getRequireLeader()) {
            leadershipValidator.validateWriteLeadership(shardId);
            ShardRecord shard = shardRouter.getShardRecord(shardId);
            if (replicationManager != null) {
                replicationManager.ensureStrongReadReady(shardId, shard);
            } else if (shard.getReplicasCount() > 1) {
                throw new NodeUnavailableException(
                        "Strong read quorum validation is unavailable on this node", shardId);
            }
        } else {
            leadershipValidator.validateReadReplica(shardId);
        }

        ShardKVStore.ReadResult read = shardStores.getOrCreate(shardId).read(key);

        ValueResponse response = ValueResponse.newBuilder()
                .setValue(request.getHeadOnly() ? com.google.protobuf.ByteString.EMPTY : read.value())
                .setVersion(read.version())
                .setAppliedVersion(read.appliedVersion())
                .setShardEpoch(read.shardEpoch())
                .setFound(read.found())
                .setCreateTimeMs(read.createTimeMs())
                .setUpdateTimeMs(read.updateTimeMs())
                .setExpireTimeMs(read.expireTimeMs())
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void set(KeyValueRequest request, StreamObserver<SetResponse> responseObserver) {
        limits.validateMessage(request);
        limits.validateKey(request.getKey());
        limits.validateValue(request.getValue());
        var key = request.getKey();
        var value = request.getValue();

        // Route and validate leadership
        String shardId = shardRouter.resolveShardId(key);
        leadershipValidator.validateWriteLeadership(shardId);

        MutationResult result;
        try {
            if (replicationManager == null) {
                if (request.getTtlMs() != 0 || request.hasIfVersionEquals() || request.getIfNotExists()) {
                    respondSetRejected(
                            responseObserver, MutationOutcome.INVALID_OPTIONS, "write options require replication");
                    return;
                }
                ShardKVStore store = shardStores.getOrCreate(shardId);
                store.set(key, value);
                result = new MutationResult(
                        stableRequestId(request.getRequestId()), store.read(key).version(), 1);
            } else {
                ShardRecord shard = shardRouter.getShardRecord(shardId);
                result = replicationManager.replicateSet(
                        shardId,
                        shard,
                        key,
                        value,
                        stableRequestId(request.getRequestId()),
                        normalizedDurability(request.getDurability()),
                        request.getTtlMs(),
                        optionalVersion(request.hasIfVersionEquals(), request.getIfVersionEquals()),
                        request.getIfNotExists());
            }
        } catch (ShardKVStore.ConditionalMutationException e) {
            respondSetRejected(responseObserver, e.outcome(), e.getMessage());
            return;
        } catch (ShardKVStore.InvalidMutationOptionsException e) {
            respondSetRejected(responseObserver, MutationOutcome.INVALID_OPTIONS, e.getMessage());
            return;
        }

        SetResponse response = SetResponse.newBuilder()
                .setSuccess(true)
                .setRequestId(result.requestId())
                .setVersion(result.version())
                .setDurable(true)
                .setOutcome(MutationOutcome.APPLIED)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
        limits.validateMessage(request);
        limits.validateKey(request.getKey());
        var key = request.getKey();

        if (request.getTtlMs() != 0 || request.getIfNotExists()) {
            respondDeleteRejected(
                    responseObserver, MutationOutcome.INVALID_OPTIONS, "TTL and create-only are invalid for delete");
            return;
        }

        // Route and validate leadership
        String shardId = shardRouter.resolveShardId(key);
        leadershipValidator.validateWriteLeadership(shardId);

        boolean success;
        MutationResult result;
        try {
            if (replicationManager == null) {
                if (request.hasIfVersionEquals()) {
                    respondDeleteRejected(
                            responseObserver, MutationOutcome.INVALID_OPTIONS, "CAS requires replication");
                    return;
                }
                ShardKVStore store = shardStores.getOrCreate(shardId);
                success = store.del(key);
                result = new MutationResult(stableRequestId(request.getRequestId()), store.committedVersion(), 1);
            } else {
                ShardRecord shard = shardRouter.getShardRecord(shardId);
                result = replicationManager.replicateDelete(
                        shardId,
                        shard,
                        key,
                        stableRequestId(request.getRequestId()),
                        normalizedDurability(request.getDurability()),
                        optionalVersion(request.hasIfVersionEquals(), request.getIfVersionEquals()));
                // A committed tombstone is a successful, idempotent delete even if the key was already absent.
                success = true;
            }
        } catch (ShardKVStore.ConditionalMutationException e) {
            respondDeleteRejected(responseObserver, e.outcome(), e.getMessage());
            return;
        } catch (ShardKVStore.InvalidMutationOptionsException e) {
            respondDeleteRejected(responseObserver, MutationOutcome.INVALID_OPTIONS, e.getMessage());
            return;
        }

        DeleteResponse response = DeleteResponse.newBuilder()
                .setSuccess(success)
                .setRequestId(result.requestId())
                .setVersion(result.version())
                .setDurable(true)
                .setOutcome(MutationOutcome.APPLIED)
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
        limits.validateMessage(request);
        limits.validateKey(mutation.getKey());
        limits.validateValue(mutation.getValue());
        String shardId = mutation.getShardId();
        shardRouter.validateShardIdForKey(mutation.getKey(), shardId);
        shardRouter.validateReplica(shardId);
        shardRouter.validateEpoch(shardId, mutation.getEpoch());
        String sender = verifiedReplicationPeer(shardId);
        if (!sender.equals(mutation.getOriginNodeId())) {
            throw new PermissionDeniedException(
                    "origin_node_id conflicts with the verified storage-node identity", shardId);
        }
        // Only the current leader may stage or expose a mutation. A sender may still abort its own hidden prepare
        // after a handoff: ABORT matches the exact staged mutation and cannot touch committed state.
        if (request.getPhase() != ReplicationPhase.ABORT) {
            shardRouter.validateReplicationLeader(shardId, sender);
        }

        ShardKVStore store = shardStores.getOrCreate(shardId);
        ShardKVStore.MutationStatus status =
                switch (request.getPhase()) {
                    case PREPARE -> store.prepareMutation(mutation);
                    case COMMIT -> store.commitMutation(mutation);
                    case ABORT -> store.abortMutation(mutation);
                    default ->
                        new ShardKVStore.MutationStatus(
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
        limits.validateMessage(request);
        limits.validateBatchSize(request.getCommittedMutationsCount() + request.getSupersededMutationsCount());
        String shardId = request.getShardId();
        shardRouter.validateReplica(shardId);
        shardRouter.validateEpoch(shardId, request.getEpoch());
        // Repair transfers committed history, so entries may originate from former leaders. The authority to push
        // that history belongs to the current leader only.
        shardRouter.validateReplicationLeader(shardId, verifiedReplicationPeer(shardId));
        ShardKVStore store = shardStores.getOrCreate(shardId);
        int applied = 0;
        for (ShardKVStore.TransferEntry entry : ShardKVStore.TransferEntry.merge(
                request.getCommittedMutationsList(), request.getSupersededMutationsList())) {
            ReplicatedMutation mutation = entry.mutation();
            limits.validateKey(mutation.getKey());
            limits.validateValue(mutation.getValue());
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
            ShardKVStore.MutationStatus status = store.applyTransferEntry(entry);
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
        limits.validateMessage(request);
        String shardId = request.getShardId();
        shardRouter.validateReplica(shardId);
        shardRouter.validateEpoch(shardId, request.getEpoch());
        ShardKVStore store = shardStores.getOrCreate(shardId);
        int requestedLimit = request.getMaxMutations() == 0 ? limits.maxBatchEntries() : request.getMaxMutations();
        limits.validateBatchSize(requestedLimit);
        int limit = Math.max(1, requestedLimit);
        // Older receivers neither ask for nor understand superseded entries, and a page holding only those would stall
        // their cursor. Keep their newest-per-key stream unchanged.
        boolean includeSuperseded = request.getIncludeSuperseded();
        var candidates = transferCandidates(store, includeSuperseded, request.getAfterVersion(), limit);
        var response = ReplicaStateResponse.newBuilder();
        int entries = 0;
        long lastVersion = request.getAfterVersion();
        int responseBytes = ReplicaStateResponse.newBuilder()
                .setSuccess(true)
                .setDurable(true)
                .setHasMore(true)
                .setCommittedVersion(store.committedVersion())
                .build()
                .getSerializedSize();
        boolean truncatedByMessageLimit = false;
        for (ShardKVStore.TransferEntry candidate : candidates) {
            int fieldNumber = candidate.superseded()
                    ? ReplicaStateResponse.SUPERSEDED_MUTATIONS_FIELD_NUMBER
                    : ReplicaStateResponse.COMMITTED_MUTATIONS_FIELD_NUMBER;
            int entryBytes = CodedOutputStream.computeMessageSize(fieldNumber, candidate.mutation());
            if (responseBytes + entryBytes > limits.maxMessageBytes()) {
                if (entries == 0) {
                    throw new PayloadTooLargeException("single replica mutation exceeds configured message limit");
                }
                truncatedByMessageLimit = true;
                break;
            }
            if (candidate.superseded()) {
                response.addSupersededMutations(candidate.mutation());
            } else {
                response.addCommittedMutations(candidate.mutation());
            }
            entries++;
            lastVersion = candidate.mutation().getVersion();
            responseBytes += entryBytes;
        }
        boolean hasMore = truncatedByMessageLimit
                || !transferCandidates(store, includeSuperseded, lastVersion, 1).isEmpty();
        responseObserver.onNext(response.setSuccess(true)
                .setDurable(true)
                .setHasMore(hasMore)
                .setCommittedVersion(store.committedVersion())
                .build());
        responseObserver.onCompleted();
    }

    private static List<ShardKVStore.TransferEntry> transferCandidates(
            ShardKVStore store, boolean includeSuperseded, long afterVersion, int limit) {
        if (includeSuperseded) {
            return store.committedHistoryAfter(afterVersion, limit);
        }
        return store.committedMutationsAfter(afterVersion, limit).stream()
                .map(mutation -> new ShardKVStore.TransferEntry(mutation, false))
                .toList();
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

    /** Returns the certificate-derived node identity of the replication sender; request fields are not trusted. */
    private static String verifiedReplicationPeer(String shardId) {
        GrpcIdentity peer = GrpcPeerIdentity.CURRENT.get();
        if (peer == null
                || peer.role() != GrpcIdentity.Role.STORAGE_NODE
                || peer.principal().isBlank()) {
            throw new PermissionDeniedException("Replication requires a verified storage-node identity", shardId);
        }
        return peer.principal();
    }

    private static String stableRequestId(String requestId) {
        return requestId == null || requestId.isBlank() ? UUID.randomUUID().toString() : requestId;
    }

    private static WriteDurability normalizedDurability(WriteDurability durability) {
        return switch (durability) {
            case WRITE_DURABILITY_UNSPECIFIED, QUORUM_SYNC -> WriteDurability.QUORUM_SYNC;
            case ALL_SYNC -> WriteDurability.ALL_SYNC;
            case LOCAL_SYNC -> WriteDurability.LOCAL_SYNC;
            case UNRECOGNIZED -> throw new InvalidRequestException("durability is unrecognized");
        };
    }

    private static OptionalLong optionalVersion(boolean present, long version) {
        return present ? OptionalLong.of(version) : OptionalLong.empty();
    }

    private static void respondSetRejected(
            StreamObserver<SetResponse> responseObserver, MutationOutcome outcome, String message) {
        responseObserver.onNext(SetResponse.newBuilder()
                .setSuccess(false)
                .setDurable(false)
                .setOutcome(outcome)
                .setMessage(message)
                .build());
        responseObserver.onCompleted();
    }

    private static void respondDeleteRejected(
            StreamObserver<DeleteResponse> responseObserver, MutationOutcome outcome, String message) {
        responseObserver.onNext(DeleteResponse.newBuilder()
                .setSuccess(false)
                .setDurable(false)
                .setOutcome(outcome)
                .setMessage(message)
                .build());
        responseObserver.onCompleted();
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
