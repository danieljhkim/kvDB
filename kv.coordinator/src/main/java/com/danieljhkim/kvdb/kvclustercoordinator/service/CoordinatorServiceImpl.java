package com.danieljhkim.kvdb.kvclustercoordinator.service;

import com.danieljhkim.kvdb.kvclustercoordinator.converter.ProtoConverter;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftCommand;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftStateMachine;
import com.danieljhkim.kvdb.kvclustercoordinator.state.NodeRecord;
import com.danieljhkim.kvdb.kvclustercoordinator.state.ShardMapSnapshot;
import com.danieljhkim.kvdb.kvclustercoordinator.state.ShardRecord;
import com.danieljhkim.kvdb.kvcommon.exception.NotLeaderException;
import com.danieljhkim.kvdb.proto.coordinator.CoordinatorGrpc;
import com.danieljhkim.kvdb.proto.coordinator.GetNodeRequest;
import com.danieljhkim.kvdb.proto.coordinator.GetNodeResponse;
import com.danieljhkim.kvdb.proto.coordinator.GetShardMapRequest;
import com.danieljhkim.kvdb.proto.coordinator.GetShardMapResponse;
import com.danieljhkim.kvdb.proto.coordinator.HeartbeatRequest;
import com.danieljhkim.kvdb.proto.coordinator.HeartbeatResponse;
import com.danieljhkim.kvdb.proto.coordinator.InitShardsRequest;
import com.danieljhkim.kvdb.proto.coordinator.InitShardsResponse;
import com.danieljhkim.kvdb.proto.coordinator.ListNodesRequest;
import com.danieljhkim.kvdb.proto.coordinator.ListNodesResponse;
import com.danieljhkim.kvdb.proto.coordinator.RegisterNodeRequest;
import com.danieljhkim.kvdb.proto.coordinator.RegisterNodeResponse;
import com.danieljhkim.kvdb.proto.coordinator.ReportShardLeaderRequest;
import com.danieljhkim.kvdb.proto.coordinator.ReportShardLeaderResponse;
import com.danieljhkim.kvdb.proto.coordinator.ResolveShardRequest;
import com.danieljhkim.kvdb.proto.coordinator.ResolveShardResponse;
import com.danieljhkim.kvdb.proto.coordinator.SetNodeStatusRequest;
import com.danieljhkim.kvdb.proto.coordinator.SetNodeStatusResponse;
import com.danieljhkim.kvdb.proto.coordinator.SetShardLeaderRequest;
import com.danieljhkim.kvdb.proto.coordinator.SetShardLeaderResponse;
import com.danieljhkim.kvdb.proto.coordinator.SetShardReplicasRequest;
import com.danieljhkim.kvdb.proto.coordinator.SetShardReplicasResponse;
import com.danieljhkim.kvdb.proto.coordinator.WatchShardMapRequest;
import io.grpc.stub.StreamObserver;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * gRPC service implementation for the Coordinator. Handles both read APIs and admin write APIs. Exceptions are handled
 * by GlobalExceptionInterceptor.
 */
public class CoordinatorServiceImpl extends CoordinatorGrpc.CoordinatorImplBase {

    private static final Logger logger = LoggerFactory.getLogger(CoordinatorServiceImpl.class);

    private final RaftStateMachine raftStateMachine;
    private final WatcherManager watcherManager;

    public CoordinatorServiceImpl(RaftStateMachine raftStateMachine, WatcherManager watcherManager) {
        this.raftStateMachine = raftStateMachine;
        this.watcherManager = watcherManager;
    }

    // ============================
    // Read APIs
    // ============================

    @Override
    public void getShardMap(GetShardMapRequest request, StreamObserver<GetShardMapResponse> responseObserver) {
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
        ShardMapSnapshot snapshot = raftStateMachine.getSnapshot();

        ListNodesResponse.Builder response = ListNodesResponse.newBuilder();
        for (NodeRecord node : snapshot.getNodes().values()) {
            response.addNodes(ProtoConverter.toProto(node));
        }

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
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
        // This triggers a Raft command to update the leader hint
        RaftCommand.SetShardLeader command =
                new RaftCommand.SetShardLeader(request.getShardId(), request.getEpoch(), request.getLeaderNodeId());

        raftStateMachine
                .apply(command)
                .thenAccept(v -> {
                    responseObserver.onNext(ReportShardLeaderResponse.newBuilder()
                            .setAccepted(true)
                            .build());
                    responseObserver.onCompleted();
                    logger.info(
                            "ReportShardLeader: shard={}, leader={}", request.getShardId(), request.getLeaderNodeId());
                })
                .exceptionally(e -> {
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

        raftStateMachine
                .apply(command)
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

        raftStateMachine
                .apply(command)
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

        raftStateMachine
                .apply(command)
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

        raftStateMachine
                .apply(command)
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

        raftStateMachine
                .apply(command)
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

    /**
     * Throws NotLeaderException if this node is not the leader.
     */
    private void requireLeader() {
        if (!raftStateMachine.isLeader()) {
            throw new NotLeaderException();
        }
    }
}
