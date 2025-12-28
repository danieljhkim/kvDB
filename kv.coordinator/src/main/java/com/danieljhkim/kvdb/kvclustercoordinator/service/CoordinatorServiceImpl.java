package com.danieljhkim.kvdb.kvclustercoordinator.service;

import com.danieljhkim.kvdb.kvclustercoordinator.converter.ProtoConverter;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftCommand;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftStateMachine;
import com.danieljhkim.kvdb.kvclustercoordinator.state.NodeRecord;
import com.danieljhkim.kvdb.kvclustercoordinator.state.ShardMapSnapshot;
import com.danieljhkim.kvdb.kvclustercoordinator.state.ShardRecord;
import com.danieljhkim.kvdb.proto.coordinator.*;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * gRPC service implementation for the Coordinator.
 * Handles both read APIs and admin write APIs.
 */
public class CoordinatorServiceImpl extends CoordinatorGrpc.CoordinatorImplBase {

	private static final Logger LOGGER = Logger.getLogger(CoordinatorServiceImpl.class.getName());

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
		try {
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
			LOGGER.fine("GetShardMap: clientVersion=" + clientVersion + ", currentVersion=" + snapshot.getMapVersion());
		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, "GetShardMap failed", e);
			responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
		}
	}

	@Override
	public void watchShardMap(
			WatchShardMapRequest request,
			StreamObserver<com.danieljhkim.kvdb.proto.coordinator.ShardMapDelta> responseObserver) {
		try {
			long fromVersion = request.getFromVersion();
			ShardMapSnapshot snapshot = raftStateMachine.getSnapshot();

			// Register watcher (will send initial state if newer)
			watcherManager.registerWatcher(responseObserver, fromVersion, snapshot);
			LOGGER.info("WatchShardMap: registered watcher fromVersion=" + fromVersion);

			// Note: Stream stays open. Client disconnect handled by gRPC.
			// We don't call onCompleted here - the stream remains open for deltas.
		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, "WatchShardMap failed", e);
			responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
		}
	}

	@Override
	public void resolveShard(ResolveShardRequest request, StreamObserver<ResolveShardResponse> responseObserver) {
		try {
			ShardMapSnapshot snapshot = raftStateMachine.getSnapshot();
			byte[] key = request.getKey().toByteArray();

			ShardRecord shard = snapshot.resolveShardForKey(key);
			ResolveShardResponse.Builder response = ResolveShardResponse.newBuilder();

			if (shard != null) {
				response.setShardId(shard.shardId()).setShard(ProtoConverter.toProto(shard));
			}

			responseObserver.onNext(response.build());
			responseObserver.onCompleted();
		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, "ResolveShard failed", e);
			responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
		}
	}

	@Override
	public void getNode(GetNodeRequest request, StreamObserver<GetNodeResponse> responseObserver) {
		try {
			ShardMapSnapshot snapshot = raftStateMachine.getSnapshot();
			NodeRecord node = snapshot.getNode(request.getNodeId());

			GetNodeResponse.Builder response = GetNodeResponse.newBuilder();
			if (node != null) {
				response.setNode(ProtoConverter.toProto(node));
			}

			responseObserver.onNext(response.build());
			responseObserver.onCompleted();
		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, "GetNode failed", e);
			responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
		}
	}

	@Override
	public void listNodes(ListNodesRequest request, StreamObserver<ListNodesResponse> responseObserver) {
		try {
			ShardMapSnapshot snapshot = raftStateMachine.getSnapshot();

			ListNodesResponse.Builder response = ListNodesResponse.newBuilder();
			for (NodeRecord node : snapshot.getNodes().values()) {
				response.addNodes(ProtoConverter.toProto(node));
			}

			responseObserver.onNext(response.build());
			responseObserver.onCompleted();
		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, "ListNodes failed", e);
			responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
		}
	}

	// ============================
	// Non-Raft Operational APIs
	// ============================

	@Override
	public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
		try {
			// Heartbeats are high-frequency and not replicated via Raft.
			// We update the internal state directly (non-Raft path).
			// Note: In a multi-coordinator setup, this would need special handling.
			String nodeId = request.getNodeId();
			long nowMs = request.getNowMs();

			// For now, we just acknowledge. The actual heartbeat update
			// would need direct access to ClusterState, which StubRaftStateMachine
			// doesn't expose. In production, you'd have a separate non-Raft path.
			LOGGER.fine("Heartbeat received from node " + nodeId + " at " + nowMs);

			responseObserver.onNext(HeartbeatResponse.newBuilder().setAccepted(true).build());
			responseObserver.onCompleted();
		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, "Heartbeat failed", e);
			responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
		}
	}

	@Override
	public void reportShardLeader(
			ReportShardLeaderRequest request, StreamObserver<ReportShardLeaderResponse> responseObserver) {
		try {
			// This triggers a Raft command to update the leader hint
			RaftCommand.SetShardLeader command = new RaftCommand.SetShardLeader(request.getShardId(),
					request.getEpoch(), request.getLeaderNodeId());

			raftStateMachine
					.apply(command)
					.thenAccept(v -> {
						responseObserver.onNext(
								ReportShardLeaderResponse.newBuilder().setAccepted(true).build());
						responseObserver.onCompleted();
						LOGGER.info("ReportShardLeader: shard="
								+ request.getShardId()
								+ ", leader="
								+ request.getLeaderNodeId());
					})
					.exceptionally(e -> {
						LOGGER.log(Level.WARNING, "ReportShardLeader failed", e);
						responseObserver.onNext(
								ReportShardLeaderResponse.newBuilder().setAccepted(false).build());
						responseObserver.onCompleted();
						return null;
					});
		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, "ReportShardLeader failed", e);
			responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
		}
	}

	// ============================
	// Admin APIs (Raft-replicated)
	// ============================

	@Override
	public void registerNode(RegisterNodeRequest request, StreamObserver<RegisterNodeResponse> responseObserver) {
		try {
			if (!raftStateMachine.isLeader()) {
				responseObserver.onError(Status.FAILED_PRECONDITION
						.withDescription("Not the leader")
						.asRuntimeException());
				return;
			}

			RaftCommand.RegisterNode command = new RaftCommand.RegisterNode(request.getNodeId(), request.getAddress(),
					request.getZone());

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
						LOGGER.info(
								"RegisterNode: nodeId=" + request.getNodeId() + ", address=" + request.getAddress());
					})
					.exceptionally(e -> {
						LOGGER.log(Level.SEVERE, "RegisterNode failed", e);
						responseObserver.onNext(RegisterNodeResponse.newBuilder()
								.setSuccess(false)
								.setMessage(e.getMessage())
								.build());
						responseObserver.onCompleted();
						return null;
					});
		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, "RegisterNode failed", e);
			responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
		}
	}

	@Override
	public void initShards(InitShardsRequest request, StreamObserver<InitShardsResponse> responseObserver) {
		try {
			if (!raftStateMachine.isLeader()) {
				responseObserver.onError(Status.FAILED_PRECONDITION
						.withDescription("Not the leader")
						.asRuntimeException());
				return;
			}

			RaftCommand.InitShards command = new RaftCommand.InitShards(request.getNumShards(),
					request.getReplicationFactor());

			raftStateMachine
					.apply(command)
					.thenAccept(v -> {
						ShardMapSnapshot snapshot = raftStateMachine.getSnapshot();
						List<String> shardIds = snapshot.getShards().keySet().stream().sorted().toList();
						responseObserver.onNext(InitShardsResponse.newBuilder()
								.setSuccess(true)
								.setMessage("Shards initialized successfully")
								.setMapVersion(snapshot.getMapVersion())
								.addAllShardIds(shardIds)
								.build());
						responseObserver.onCompleted();
						LOGGER.info("InitShards: numShards="
								+ request.getNumShards()
								+ ", rf="
								+ request.getReplicationFactor());
					})
					.exceptionally(e -> {
						LOGGER.log(Level.SEVERE, "InitShards failed", e);
						responseObserver.onNext(InitShardsResponse.newBuilder()
								.setSuccess(false)
								.setMessage(e.getMessage())
								.build());
						responseObserver.onCompleted();
						return null;
					});
		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, "InitShards failed", e);
			responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
		}
	}

	@Override
	public void setNodeStatus(SetNodeStatusRequest request, StreamObserver<SetNodeStatusResponse> responseObserver) {
		try {
			if (!raftStateMachine.isLeader()) {
				responseObserver.onError(Status.FAILED_PRECONDITION
						.withDescription("Not the leader")
						.asRuntimeException());
				return;
			}

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
						LOGGER.info("SetNodeStatus: nodeId=" + request.getNodeId() + ", status=" + status);
					})
					.exceptionally(e -> {
						LOGGER.log(Level.SEVERE, "SetNodeStatus failed", e);
						responseObserver.onNext(SetNodeStatusResponse.newBuilder()
								.setSuccess(false)
								.setMessage(e.getMessage())
								.build());
						responseObserver.onCompleted();
						return null;
					});
		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, "SetNodeStatus failed", e);
			responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
		}
	}

	@Override
	public void setShardReplicas(
			SetShardReplicasRequest request, StreamObserver<SetShardReplicasResponse> responseObserver) {
		try {
			if (!raftStateMachine.isLeader()) {
				responseObserver.onError(Status.FAILED_PRECONDITION
						.withDescription("Not the leader")
						.asRuntimeException());
				return;
			}

			RaftCommand.SetShardReplicas command = new RaftCommand.SetShardReplicas(request.getShardId(),
					request.getReplicasList());

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
						LOGGER.info("SetShardReplicas: shardId="
								+ request.getShardId()
								+ ", replicas="
								+ request.getReplicasList());
					})
					.exceptionally(e -> {
						LOGGER.log(Level.SEVERE, "SetShardReplicas failed", e);
						responseObserver.onNext(SetShardReplicasResponse.newBuilder()
								.setSuccess(false)
								.setMessage(e.getMessage())
								.build());
						responseObserver.onCompleted();
						return null;
					});
		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, "SetShardReplicas failed", e);
			responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
		}
	}

	@Override
	public void setShardLeader(SetShardLeaderRequest request, StreamObserver<SetShardLeaderResponse> responseObserver) {
		try {
			if (!raftStateMachine.isLeader()) {
				responseObserver.onError(Status.FAILED_PRECONDITION
						.withDescription("Not the leader")
						.asRuntimeException());
				return;
			}

			RaftCommand.SetShardLeader command = new RaftCommand.SetShardLeader(
					request.getShardId(), request.getEpoch(), request.getLeaderNodeId());

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
						LOGGER.info("SetShardLeader: shardId="
								+ request.getShardId()
								+ ", leader="
								+ request.getLeaderNodeId());
					})
					.exceptionally(e -> {
						LOGGER.log(Level.SEVERE, "SetShardLeader failed", e);
						responseObserver.onNext(SetShardLeaderResponse.newBuilder()
								.setSuccess(false)
								.setMessage(e.getMessage())
								.build());
						responseObserver.onCompleted();
						return null;
					});
		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, "SetShardLeader failed", e);
			responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
		}
	}
}
