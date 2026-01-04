package com.danieljhkim.kvdb.kvclustercoordinator.raft.rpc;

import com.danieljhkim.kvdb.kvclustercoordinator.raft.election.RaftVoteHandler;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.replication.RaftAppendEntriesHandler;
import com.danieljhkim.kvdb.proto.raft.*;
import io.grpc.stub.StreamObserver;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

/**
 * gRPC service implementation for Raft consensus protocol.
 *
 * <p>This service exposes the Raft RPCs (RequestVote, AppendEntries, etc.) over gRPC,
 * allowing distributed Raft nodes to communicate.
 *
 * <p>Thread-safety: gRPC handles concurrent requests in separate threads, so this service
 * must be thread-safe. The underlying handlers are responsible for synchronization.
 */
@Slf4j
public class RaftGrpcService extends RaftServiceGrpc.RaftServiceImplBase {

    private final String nodeId;
    private final RaftVoteHandler voteHandler;
    private final RaftAppendEntriesHandler appendEntriesHandler;
    private final Supplier<Long> currentTermSupplier;
    private final Supplier<String> currentLeaderSupplier;

    /**
     * Creates a new Raft gRPC service.
     *
     * @param nodeId the ID of this node (for logging)
     * @param voteHandler handler for RequestVote RPCs
     * @param appendEntriesHandler handler for AppendEntries RPCs
     * @param currentTermSupplier supplier for current term (for placeholder RPCs)
     * @param currentLeaderSupplier supplier for current leader (for placeholder RPCs)
     */
    public RaftGrpcService(
            String nodeId,
            RaftVoteHandler voteHandler,
            RaftAppendEntriesHandler appendEntriesHandler,
            Supplier<Long> currentTermSupplier,
            Supplier<String> currentLeaderSupplier) {
        this.nodeId = nodeId;
        this.voteHandler = voteHandler;
        this.appendEntriesHandler = appendEntriesHandler;
        this.currentTermSupplier = currentTermSupplier;
        this.currentLeaderSupplier = currentLeaderSupplier;
    }

    /**
     * Handles RequestVote RPC from candidates during leader election.
     *
     * <p>Raft paper §5.2: Candidates send RequestVote RPCs to all other servers to gather votes.
     */
    @Override
    public void requestVote(RequestVoteRequest request, StreamObserver<RequestVoteResponse> responseObserver) {
        log.debug("[{}] Received RequestVote from {} for term {}", nodeId, request.getCandidateId(), request.getTerm());

        try {
            RequestVoteResponse response = voteHandler.handleRequestVote(request);
            responseObserver.onNext(response);
            responseObserver.onCompleted();

            log.debug(
                    "[{}] Responded to RequestVote from {}: granted={}",
                    nodeId,
                    request.getCandidateId(),
                    response.getVoteGranted());

        } catch (Exception e) {
            log.error(
                    "[{}] Error handling RequestVote from {}: {}", nodeId, request.getCandidateId(), e.getMessage(), e);
            responseObserver.onError(io.grpc.Status.INTERNAL
                    .withDescription("Error processing RequestVote: " + e.getMessage())
                    .asRuntimeException());
        }
    }

    /**
     * Handles AppendEntries RPC from the leader (heartbeats and log replication).
     *
     * <p>Raft paper §5.3: Leaders send AppendEntries RPCs to replicate log entries
     * and provide heartbeats to maintain authority.
     */
    @Override
    public void appendEntries(AppendEntriesRequest request, StreamObserver<AppendEntriesResponse> responseObserver) {
        // Log at trace level since this can be very frequent (heartbeats)
        log.trace(
                "[{}] Received AppendEntries from {} for term {} with {} entries",
                nodeId,
                request.getLeaderId(),
                request.getTerm(),
                request.getEntriesCount());

        try {
            AppendEntriesResponse response = appendEntriesHandler.handleAppendEntries(request);
            responseObserver.onNext(response);
            responseObserver.onCompleted();

            if (log.isTraceEnabled()) {
                log.trace(
                        "[{}] Responded to AppendEntries from {}: success={}, matchIndex={}",
                        nodeId,
                        request.getLeaderId(),
                        response.getSuccess(),
                        response.getMatchIndex());
            }

        } catch (Exception e) {
            log.error(
                    "[{}] Error handling AppendEntries from {}: {}", nodeId, request.getLeaderId(), e.getMessage(), e);
            responseObserver.onError(io.grpc.Status.INTERNAL
                    .withDescription("Error processing AppendEntries: " + e.getMessage())
                    .asRuntimeException());
        }
    }

    /**
     * Handles InstallSnapshot RPC from the leader (for catching up lagging followers).
     *
     * <p>Raft paper §7: Leaders use InstallSnapshot RPC to send snapshots to followers
     * that are too far behind to catch up via AppendEntries.
     *
     * <p>Note: Snapshot installation is not yet implemented (Phase 5).
     */
    @Override
    public void installSnapshot(
            InstallSnapshotRequest request, StreamObserver<InstallSnapshotResponse> responseObserver) {
        log.debug(
                "[{}] Received InstallSnapshot from {} for term {}", nodeId, request.getLeaderId(), request.getTerm());

        try {
            // TODO: Implement snapshot installation in Phase 5
            InstallSnapshotResponse response = InstallSnapshotResponse.newBuilder()
                    .setTerm(currentTermSupplier.get())
                    .setSuccess(false)
                    .setFollowerId(nodeId)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

            log.warn("[{}] InstallSnapshot not yet implemented (Phase 5)", nodeId);

        } catch (Exception e) {
            log.error(
                    "[{}] Error handling InstallSnapshot from {}: {}",
                    nodeId,
                    request.getLeaderId(),
                    e.getMessage(),
                    e);
            responseObserver.onError(io.grpc.Status.UNIMPLEMENTED
                    .withDescription("InstallSnapshot not yet implemented")
                    .asRuntimeException());
        }
    }

    /**
     * Handles AddServer RPC for dynamic cluster membership.
     *
     * <p>Note: Cluster membership changes are not yet implemented (Future work).
     */
    @Override
    public void addServer(AddServerRequest request, StreamObserver<AddServerResponse> responseObserver) {
        log.debug("[{}] Received AddServer request for {}", nodeId, request.getServerId());

        try {
            // TODO: Implement cluster membership changes in future phase
            AddServerResponse response = AddServerResponse.newBuilder()
                    .setStatus(AddServerResponse.Status.NOT_LEADER)
                    .setLeaderHint(currentLeaderSupplier.get())
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

            log.warn("[{}] AddServer not yet implemented (Future work)", nodeId);

        } catch (Exception e) {
            log.error("[{}] Error handling AddServer: {}", nodeId, e.getMessage(), e);
            responseObserver.onError(io.grpc.Status.UNIMPLEMENTED
                    .withDescription("AddServer not yet implemented")
                    .asRuntimeException());
        }
    }

    /**
     * Handles RemoveServer RPC for dynamic cluster membership.
     *
     * <p>Note: Cluster membership changes are not yet implemented (Future work).
     */
    @Override
    public void removeServer(RemoveServerRequest request, StreamObserver<RemoveServerResponse> responseObserver) {
        log.debug("[{}] Received RemoveServer request for {}", nodeId, request.getServerId());

        try {
            // TODO: Implement cluster membership changes in future phase
            RemoveServerResponse response = RemoveServerResponse.newBuilder()
                    .setStatus(RemoveServerResponse.Status.NOT_LEADER)
                    .setLeaderHint(currentLeaderSupplier.get())
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

            log.warn("[{}] RemoveServer not yet implemented (Future work)", nodeId);

        } catch (Exception e) {
            log.error("[{}] Error handling RemoveServer: {}", nodeId, e.getMessage(), e);
            responseObserver.onError(io.grpc.Status.UNIMPLEMENTED
                    .withDescription("RemoveServer not yet implemented")
                    .asRuntimeException());
        }
    }
}
