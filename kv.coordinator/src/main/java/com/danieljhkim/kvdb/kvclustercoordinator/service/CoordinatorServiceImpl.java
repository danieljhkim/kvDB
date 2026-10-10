package com.danieljhkim.kvdb.kvclustercoordinator.service;

import com.danieljhkim.kvdb.kvclustercoordinator.converter.ProtoConverter;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftCommand;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftNode;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.statemachine.RaftStateMachine;
import com.danieljhkim.kvdb.kvclustercoordinator.state.NodeRecord;
import com.danieljhkim.kvdb.kvclustercoordinator.state.RejectedMutationException;
import com.danieljhkim.kvdb.kvclustercoordinator.state.ShardMapSnapshot;
import com.danieljhkim.kvdb.kvclustercoordinator.state.ShardMapValidator;
import com.danieljhkim.kvdb.kvclustercoordinator.state.ShardRecord;
import com.danieljhkim.kvdb.kvcommon.exception.NotLeaderException;
import com.danieljhkim.kvdb.proto.coordinator.*;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.concurrent.CompletionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * gRPC service implementation for the Coordinator. Handles both read APIs and admin write APIs. Exceptions are handled
 * by GlobalExceptionInterceptor.
 *
 * <p>
 * Shard-map mutations are validated against the latest snapshot before they are submitted, so an invalid request fails
 * with INVALID_ARGUMENT without reaching the Raft log. The state machine repeats the validation when the command is
 * applied; a command rejected there also fails with INVALID_ARGUMENT and leaves the shard map unchanged.
 */
public class CoordinatorServiceImpl extends CoordinatorGrpc.CoordinatorImplBase {

    private static final Logger logger = LoggerFactory.getLogger(CoordinatorServiceImpl.class);

    private final RaftNode raftNode;
    private final RaftStateMachine raftStateMachine;
    private final WatcherManager watcherManager;

    public CoordinatorServiceImpl(RaftNode raftNode, RaftStateMachine raftStateMachine, WatcherManager watcherManager) {
        this.raftNode = raftNode;
        this.raftStateMachine = raftStateMachine;
        this.watcherManager = watcherManager;
    }

    // ============================
    // Read APIs
    // ============================

    @Override
    public void getShardMap(GetShardMapRequest request, StreamObserver<GetShardMapResponse> responseObserver) {
        requireLeader();
        ShardMapSnapshot snapshot = raftStateMachine.getSnapshot();
        long clientVersion = request.getIfVersionGt();
        GetShardMapResponse.Builder response = GetShardMapResponse.newBuilder();
        if (snapshot.getMapVersion() > clientVersion) {
            response.setState(ProtoConverter.toProto(snapshot)).setNotModified(false);
        } else {
            response.setNotModified(true);
        }

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
        logger.debug("GetShardMap: clientVersion={}, currentVersion={}", clientVersion, snapshot.getMapVersion());
    }

    @Override
    public void watchShardMap(
            WatchShardMapRequest request,
            StreamObserver<com.danieljhkim.kvdb.proto.coordinator.ShardMapDelta> responseObserver) {
        requireLeader();
        long fromVersion = request.getFromVersion();
        ShardMapSnapshot snapshot = raftStateMachine.getSnapshot();

        // Register watcher (will send initial state if newer)
        watcherManager.registerWatcher(responseObserver, fromVersion, snapshot);
        logger.info("WatchShardMap: registered watcher fromVersion={}", fromVersion);

        // Note: Stream stays open. Client disconnect handled by gRPC.
        // We don't call onCompleted here - the stream remains open for deltas.
    }

    @Override
    public void resolveShard(ResolveShardRequest request, StreamObserver<ResolveShardResponse> responseObserver) {
        requireLeader();
        ShardMapSnapshot snapshot = raftStateMachine.getSnapshot();
        byte[] key = request.getKey().toByteArray();

        ShardRecord shard = snapshot.resolveShardForKey(key);
        ResolveShardResponse.Builder response = ResolveShardResponse.newBuilder();

        if (shard != null) {
            response.setShardId(shard.shardId()).setShard(ProtoConverter.toProto(shard));
        }

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getNode(GetNodeRequest request, StreamObserver<GetNodeResponse> responseObserver) {
        requireLeader();
        ShardMapSnapshot snapshot = raftStateMachine.getSnapshot();
        NodeRecord node = snapshot.getNode(request.getNodeId());

        GetNodeResponse.Builder response = GetNodeResponse.newBuilder();
        if (node != null) {
            response.setNode(ProtoConverter.toProto(node));
        }

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void listNodes(ListNodesRequest request, StreamObserver<ListNodesResponse> responseObserver) {
        requireLeader();
        ShardMapSnapshot snapshot = raftStateMachine.getSnapshot();

        ListNodesResponse.Builder response = ListNodesResponse.newBuilder();
        for (NodeRecord node : snapshot.getNodes().values()) {
            response.addNodes(ProtoConverter.toProto(node));
        }

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getCoordinatorLeader(
            GetCoordinatorLeaderRequest request, StreamObserver<GetCoordinatorLeaderResponse> responseObserver) {
        // This method doesn't require leader - it's used for leader discovery
        boolean isLeader = raftNode.isLeader();
        String leaderId = raftNode.getLeaderId();
        String leaderAddress = raftNode.getLeaderAddress();
        long term = raftNode.getCurrentTerm();

        GetCoordinatorLeaderResponse response = GetCoordinatorLeaderResponse.newBuilder()
                .setIsLeader(isLeader)
                .setLeaderId(leaderId != null ? leaderId : "")
                .setLeaderAddress(leaderAddress != null ? leaderAddress : "")
                .setTerm(term)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
        logger.debug("GetCoordinatorLeader: isLeader={}, leaderId={}, term={}", isLeader, leaderId, term);
    }

    // ============================
    // Non-Raft Operational APIs
    // ============================

    @Override
    public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
        // Heartbeats are high-frequency and not replicated via Raft.
        // We update the internal state directly (non-Raft path).
        String nodeId = request.getNodeId();
        long nowMs = request.getNowMs();

        logger.debug("Heartbeat received from node {} at {}", nodeId, nowMs);

        responseObserver.onNext(HeartbeatResponse.newBuilder().setAccepted(true).build());
        responseObserver.onCompleted();
    }

    @Override
    public void reportShardLeader(
            ReportShardLeaderRequest request, StreamObserver<ReportShardLeaderResponse> responseObserver) {
        requireLeader();

        // This triggers a Raft command to update the leader hint
        RaftCommand.SetShardLeader command =
                new RaftCommand.SetShardLeader(request.getShardId(), request.getEpoch(), request.getLeaderNodeId());
        validateShardLeader(command);

        raftNode.submitCommand(command)
                .thenAccept(v -> {
                    responseObserver.onNext(ReportShardLeaderResponse.newBuilder()
                            .setAccepted(true)
                            .build());
                    responseObserver.onCompleted();
                    logger.info(
                            "ReportShardLeader: shard={}, leader={}", request.getShardId(), request.getLeaderNodeId());
                })
                .exceptionally(e -> {
                    if (rejectIfInvalid(e, responseObserver)) {
                        return null;
                    }
                    logger.warn("ReportShardLeader failed", e);
                    responseObserver.onNext(ReportShardLeaderResponse.newBuilder()
                            .setAccepted(false)
                            .build());
                    responseObserver.onCompleted();
                    return null;
                });
    }

    // ============================
    // Admin APIs (Raft-replicated)
    // ============================

    @Override
    public void registerNode(RegisterNodeRequest request, StreamObserver<RegisterNodeResponse> responseObserver) {
        requireLeader();

        RaftCommand.RegisterNode command =
                new RaftCommand.RegisterNode(request.getNodeId(), request.getAddress(), request.getZone());
        ShardMapValidator.validateNodeAddress(command.address());

        raftNode.submitCommand(command)
                .thenAccept(v -> {
                    long version = raftStateMachine.getMapVersion();
                    responseObserver.onNext(RegisterNodeResponse.newBuilder()
                            .setSuccess(true)
                            .setMessage("Node registered successfully")
                            .setMapVersion(version)
                            .build());
                    responseObserver.onCompleted();
                    logger.info("RegisterNode: nodeId={}, address={}", request.getNodeId(), request.getAddress());
                })
                .exceptionally(e -> {
                    if (rejectIfInvalid(e, responseObserver)) {
                        return null;
                    }
                    logger.error("RegisterNode failed", e);
                    responseObserver.onNext(RegisterNodeResponse.newBuilder()
                            .setSuccess(false)
                            .setMessage(e.getMessage())
                            .build());
                    responseObserver.onCompleted();
                    return null;
                });
    }

    @Override
    public void initShards(InitShardsRequest request, StreamObserver<InitShardsResponse> responseObserver) {
        requireLeader();

        RaftCommand.InitShards command =
                new RaftCommand.InitShards(request.getNumShards(), request.getReplicationFactor());

        raftNode.submitCommand(command)
                .thenAccept(v -> {
                    ShardMapSnapshot snapshot = raftStateMachine.getSnapshot();
                    List<String> shardIds =
                            snapshot.getShards().keySet().stream().sorted().toList();
                    responseObserver.onNext(InitShardsResponse.newBuilder()
                            .setSuccess(true)
                            .setMessage("Shards initialized successfully")
                            .setMapVersion(snapshot.getMapVersion())
                            .addAllShardIds(shardIds)
                            .build());
                    responseObserver.onCompleted();
                    logger.info(
                            "InitShards: numShards={}, rf={}", request.getNumShards(), request.getReplicationFactor());
                })
                .exceptionally(e -> {
                    if (rejectIfInvalid(e, responseObserver)) {
                        return null;
                    }
                    logger.error("InitShards failed", e);
                    responseObserver.onNext(InitShardsResponse.newBuilder()
                            .setSuccess(false)
                            .setMessage(e.getMessage())
                            .build());
                    responseObserver.onCompleted();
                    return null;
                });
    }

    @Override
    public void setNodeStatus(SetNodeStatusRequest request, StreamObserver<SetNodeStatusResponse> responseObserver) {
        requireLeader();

        NodeRecord.NodeStatus status = ProtoConverter.fromProto(request.getStatus());
        RaftCommand.SetNodeStatus command = new RaftCommand.SetNodeStatus(request.getNodeId(), status);

        raftNode.submitCommand(command)
                .thenAccept(v -> {
                    long version = raftStateMachine.getMapVersion();
                    responseObserver.onNext(SetNodeStatusResponse.newBuilder()
                            .setSuccess(true)
                            .setMessage("Node status updated")
                            .setMapVersion(version)
                            .build());
                    responseObserver.onCompleted();
                    logger.info("SetNodeStatus: nodeId={}, status={}", request.getNodeId(), status);
                })
                .exceptionally(e -> {
                    if (rejectIfInvalid(e, responseObserver)) {
                        return null;
                    }
                    logger.error("SetNodeStatus failed", e);
                    responseObserver.onNext(SetNodeStatusResponse.newBuilder()
                            .setSuccess(false)
                            .setMessage(e.getMessage())
                            .build());
                    responseObserver.onCompleted();
                    return null;
                });
    }

    @Override
    public void setShardReplicas(
            SetShardReplicasRequest request, StreamObserver<SetShardReplicasResponse> responseObserver) {
        requireLeader();

        RaftCommand.SetShardReplicas command =
                new RaftCommand.SetShardReplicas(request.getShardId(), request.getReplicasList());
        ShardMapSnapshot current = raftStateMachine.getSnapshot();
        ShardMapValidator.validateShardReplicas(
                command.shardId(), command.replicas(), current.getNodes(), current.getShards());

        raftNode.submitCommand(command)
                .thenAccept(v -> {
                    ShardMapSnapshot snapshot = raftStateMachine.getSnapshot();
                    ShardRecord shard = snapshot.getShard(request.getShardId());
                    long newEpoch = shard != null ? shard.epoch() : 0;
                    responseObserver.onNext(SetShardReplicasResponse.newBuilder()
                            .setSuccess(true)
                            .setMessage("Shard replicas updated")
                            .setMapVersion(snapshot.getMapVersion())
                            .setNewEpoch(newEpoch)
                            .build());
                    responseObserver.onCompleted();
                    logger.info(
                            "SetShardReplicas: shardId={}, replicas={}",
                            request.getShardId(),
                            request.getReplicasList());
                })
                .exceptionally(e -> {
                    if (rejectIfInvalid(e, responseObserver)) {
                        return null;
                    }
                    logger.error("SetShardReplicas failed", e);
                    responseObserver.onNext(SetShardReplicasResponse.newBuilder()
                            .setSuccess(false)
                            .setMessage(e.getMessage())
                            .build());
                    responseObserver.onCompleted();
                    return null;
                });
    }

    @Override
    public void setShardLeader(SetShardLeaderRequest request, StreamObserver<SetShardLeaderResponse> responseObserver) {
        requireLeader();

        RaftCommand.SetShardLeader command =
                new RaftCommand.SetShardLeader(request.getShardId(), request.getEpoch(), request.getLeaderNodeId());
        validateShardLeader(command);

        raftNode.submitCommand(command)
                .thenAccept(v -> {
                    long version = raftStateMachine.getMapVersion();
                    responseObserver.onNext(SetShardLeaderResponse.newBuilder()
                            .setSuccess(true)
                            .setMessage("Shard leader updated")
                            .setMapVersion(version)
                            .build());
                    responseObserver.onCompleted();
                    logger.info(
                            "SetShardLeader: shardId={}, leader={}", request.getShardId(), request.getLeaderNodeId());
                })
                .exceptionally(e -> {
                    if (rejectIfInvalid(e, responseObserver)) {
                        return null;
                    }
                    logger.error("SetShardLeader failed", e);
                    responseObserver.onNext(SetShardLeaderResponse.newBuilder()
                            .setSuccess(false)
                            .setMessage(e.getMessage())
                            .build());
                    responseObserver.onCompleted();
                    return null;
                });
    }

    // ============================
    // Helper Methods
    // ============================

    private void validateShardLeader(RaftCommand.SetShardLeader command) {
        ShardMapSnapshot current = raftStateMachine.getSnapshot();
        ShardMapValidator.validateShardLeader(
                command.shardId(), command.epoch(), command.leaderNodeId(), current.getNodes(), current.getShards());
    }

    /**
     * Fails the call with INVALID_ARGUMENT if the state machine rejected the committed command. Returns false for any
     * other failure.
     */
    private static boolean rejectIfInvalid(Throwable error, StreamObserver<?> responseObserver) {
        Throwable cause = error;
        while (cause instanceof CompletionException && cause.getCause() != null) {
            cause = cause.getCause();
        }
        if (!(cause instanceof RejectedMutationException rejection)) {
            return false;
        }
        logger.warn("Rejected shard-map mutation: {}", rejection.getMessage());
        responseObserver.onError(
                Status.INVALID_ARGUMENT.withDescription(rejection.getMessage()).asRuntimeException());
        return true;
    }

    /**
     * Throws NotLeaderException with leader hint if this node is not the leader.
     */
    private void requireLeader() {
        if (!raftNode.isLeader()) {
            String leaderAddress = raftNode.getLeaderAddress();
            throw new NotLeaderException(leaderAddress);
        }
    }
}
