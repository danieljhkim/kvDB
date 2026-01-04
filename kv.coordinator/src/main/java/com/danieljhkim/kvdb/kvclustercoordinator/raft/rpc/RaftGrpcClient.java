package com.danieljhkim.kvdb.kvclustercoordinator.raft.rpc;

import com.danieljhkim.kvdb.proto.raft.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

/**
 * gRPC client for sending Raft RPCs to other nodes in the cluster.
 *
 * <p>This client manages connections to all peer nodes and provides methods
 * for sending RequestVote and AppendEntries RPCs with proper timeout handling
 * and error recovery.
 *
 * <p>Thread-safety: This class is thread-safe. Multiple threads can send RPCs concurrently.
 */
@Slf4j
public class RaftGrpcClient implements AutoCloseable {

    private final String nodeId;
    private final Map<String, ManagedChannel> channels;
    private final Map<String, RaftServiceGrpc.RaftServiceBlockingStub> blockingStubs;
    private final Map<String, RaftServiceGrpc.RaftServiceFutureStub> futureStubs;

    // Timeouts
    private final Duration rpcTimeout;
    private final Duration connectionTimeout;

    /**
     * Creates a new Raft gRPC client.
     *
     * @param nodeId the ID of this node
     * @param peerAddresses map of peer nodeId -> gRPC address (host:port)
     */
    public RaftGrpcClient(String nodeId, Map<String, String> peerAddresses) {
        this(nodeId, peerAddresses, Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    /**
     * Creates a new Raft gRPC client with custom timeouts.
     *
     * @param nodeId the ID of this node
     * @param peerAddresses map of peer nodeId -> gRPC address (host:port)
     * @param rpcTimeout timeout for individual RPC calls
     * @param connectionTimeout timeout for establishing connections
     */
    public RaftGrpcClient(
            String nodeId, Map<String, String> peerAddresses, Duration rpcTimeout, Duration connectionTimeout) {

        this.nodeId = nodeId;
        this.rpcTimeout = rpcTimeout;
        this.connectionTimeout = connectionTimeout;
        this.channels = new ConcurrentHashMap<>();
        this.blockingStubs = new ConcurrentHashMap<>();
        this.futureStubs = new ConcurrentHashMap<>();

        // Create channels and stubs for each peer
        for (Map.Entry<String, String> entry : peerAddresses.entrySet()) {
            String peerId = entry.getKey();
            String address = entry.getValue();
            createChannelForPeer(peerId, address);
        }

        log.info("[{}] Created gRPC client with {} peer connections", nodeId, peerAddresses.size());
    }

    /**
     * Creates a gRPC channel and stubs for a specific peer.
     */
    private void createChannelForPeer(String peerId, String address) {
        try {
            ManagedChannel channel = ManagedChannelBuilder.forTarget(address)
                    .usePlaintext() // TODO: Add TLS in production
                    .maxInboundMessageSize(10 * 1024 * 1024) // 10MB max message size
                    .build();

            RaftServiceGrpc.RaftServiceBlockingStub blockingStub = RaftServiceGrpc.newBlockingStub(channel);

            RaftServiceGrpc.RaftServiceFutureStub futureStub = RaftServiceGrpc.newFutureStub(channel);

            channels.put(peerId, channel);
            blockingStubs.put(peerId, blockingStub);
            futureStubs.put(peerId, futureStub);

            log.debug("[{}] Created channel to peer {} at {}", nodeId, peerId, address);

        } catch (Exception e) {
            log.error("[{}] Failed to create channel to peer {} at {}: {}", nodeId, peerId, address, e.getMessage(), e);
        }
    }

    /**
     * Sends a RequestVote RPC to a peer node.
     *
     * @param peerId the ID of the peer to send the request to
     * @param request the RequestVote request
     * @return CompletableFuture that completes with the response
     */
    public CompletableFuture<RequestVoteResponse> sendRequestVote(String peerId, RequestVoteRequest request) {
        log.debug("[{}] Sending RequestVote to {} for term {}", nodeId, peerId, request.getTerm());

        RaftServiceGrpc.RaftServiceFutureStub stub = futureStubs.get(peerId);
        if (stub == null) {
            log.warn("[{}] No stub available for peer {}", nodeId, peerId);
            return CompletableFuture.failedFuture(new IllegalArgumentException("No connection to peer: " + peerId));
        }

        CompletableFuture<RequestVoteResponse> future = new CompletableFuture<>();

        try {
            // Apply deadline per-call, not per-stub
            RaftServiceGrpc.RaftServiceFutureStub stubWithDeadline =
                    stub.withDeadlineAfter(rpcTimeout.toMillis(), TimeUnit.MILLISECONDS);

            com.google.common.util.concurrent.ListenableFuture<RequestVoteResponse> rpcFuture =
                    stubWithDeadline.requestVote(request);

            rpcFuture.addListener(
                    () -> {
                        try {
                            RequestVoteResponse response = rpcFuture.get();
                            future.complete(response);
                            log.debug(
                                    "[{}] Received RequestVote response from {}: granted={}",
                                    nodeId,
                                    peerId,
                                    response.getVoteGranted());
                        } catch (ExecutionException e) {
                            handleRpcError(peerId, "RequestVote", e.getCause(), future);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            future.completeExceptionally(e);
                        } catch (Exception e) {
                            future.completeExceptionally(e);
                        }
                    },
                    Runnable::run);

        } catch (Exception e) {
            log.error("[{}] Error sending RequestVote to {}: {}", nodeId, peerId, e.getMessage());
            future.completeExceptionally(e);
        }

        return future;
    }

    /**
     * Sends an AppendEntries RPC to a peer node.
     *
     * @param peerId the ID of the peer to send the request to
     * @param request the AppendEntries request
     * @return CompletableFuture that completes with the response
     */
    public CompletableFuture<AppendEntriesResponse> sendAppendEntries(String peerId, AppendEntriesRequest request) {
        log.trace(
                "[{}] Sending AppendEntries to {} for term {} with {} entries",
                nodeId,
                peerId,
                request.getTerm(),
                request.getEntriesCount());

        RaftServiceGrpc.RaftServiceFutureStub stub = futureStubs.get(peerId);
        if (stub == null) {
            log.warn("[{}] No stub available for peer {}", nodeId, peerId);
            return CompletableFuture.failedFuture(new IllegalArgumentException("No connection to peer: " + peerId));
        }

        CompletableFuture<AppendEntriesResponse> future = new CompletableFuture<>();

        try {
            // Apply deadline per-call, not per-stub
            RaftServiceGrpc.RaftServiceFutureStub stubWithDeadline =
                    stub.withDeadlineAfter(rpcTimeout.toMillis(), TimeUnit.MILLISECONDS);

            com.google.common.util.concurrent.ListenableFuture<AppendEntriesResponse> rpcFuture =
                    stubWithDeadline.appendEntries(request);

            rpcFuture.addListener(
                    () -> {
                        try {
                            AppendEntriesResponse response = rpcFuture.get();
                            future.complete(response);
                            log.trace(
                                    "[{}] Received AppendEntries response from {}: success={}",
                                    nodeId,
                                    peerId,
                                    response.getSuccess());
                        } catch (ExecutionException e) {
                            handleRpcError(peerId, "AppendEntries", e.getCause(), future);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            future.completeExceptionally(e);
                        } catch (Exception e) {
                            future.completeExceptionally(e);
                        }
                    },
                    Runnable::run);

        } catch (Exception e) {
            log.error("[{}] Error sending AppendEntries to {}: {}", nodeId, peerId, e.getMessage());
            future.completeExceptionally(e);
        }

        return future;
    }

    /**
     * Sends an InstallSnapshot RPC to a peer node.
     *
     * <p>Note: Not yet implemented (Phase 5).
     *
     * @param peerId the ID of the peer to send the request to
     * @param request the InstallSnapshot request
     * @return CompletableFuture that completes with the response
     */
    public CompletableFuture<InstallSnapshotResponse> sendInstallSnapshot(
            String peerId, InstallSnapshotRequest request) {

        log.debug("[{}] Sending InstallSnapshot to {} for term {}", nodeId, peerId, request.getTerm());

        // TODO: Implement in Phase 5
        return CompletableFuture.failedFuture(new UnsupportedOperationException("InstallSnapshot not yet implemented"));
    }

    /**
     * Handles RPC errors and completes the future with an appropriate exception.
     */
    private <T> void handleRpcError(String peerId, String rpcName, Throwable error, CompletableFuture<T> future) {
        if (error instanceof StatusRuntimeException) {
            StatusRuntimeException sre = (StatusRuntimeException) error;
            Status.Code code = sre.getStatus().getCode();

            if (code == Status.Code.UNAVAILABLE) {
                log.warn("[{}] Peer {} unavailable for {}", nodeId, peerId, rpcName);
            } else if (code == Status.Code.DEADLINE_EXCEEDED) {
                log.warn("[{}] {} to {} timed out", nodeId, rpcName, peerId);
            } else {
                log.warn(
                        "[{}] {} to {} failed with status {}: {}",
                        nodeId,
                        rpcName,
                        peerId,
                        code,
                        sre.getStatus().getDescription());
            }
        } else {
            log.error("[{}] {} to {} failed: {}", nodeId, rpcName, peerId, error.getMessage());
        }

        future.completeExceptionally(error);
    }

    /**
     * Checks if a connection to a peer is available.
     *
     * @param peerId the peer to check
     * @return true if a channel exists and is not shutdown
     */
    public boolean isConnected(String peerId) {
        ManagedChannel channel = channels.get(peerId);
        return channel != null && !channel.isShutdown();
    }

    /**
     * Adds a new peer connection at runtime.
     *
     * @param peerId the ID of the new peer
     * @param address the gRPC address of the new peer
     */
    public void addPeer(String peerId, String address) {
        if (channels.containsKey(peerId)) {
            log.warn("[{}] Peer {} already exists, not adding", nodeId, peerId);
            return;
        }

        createChannelForPeer(peerId, address);
        log.info("[{}] Added new peer connection: {} at {}", nodeId, peerId, address);
    }

    /**
     * Removes a peer connection and shuts down its channel.
     *
     * @param peerId the ID of the peer to remove
     */
    public void removePeer(String peerId) {
        ManagedChannel channel = channels.remove(peerId);
        if (channel != null) {
            blockingStubs.remove(peerId);
            futureStubs.remove(peerId);

            channel.shutdown();
            try {
                if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                    channel.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                channel.shutdownNow();
            }

            log.info("[{}] Removed peer connection: {}", nodeId, peerId);
        }
    }

    /**
     * Closes all connections to peers.
     */
    @Override
    public void close() {
        log.info("[{}] Closing all gRPC connections", nodeId);

        for (Map.Entry<String, ManagedChannel> entry : channels.entrySet()) {
            String peerId = entry.getKey();
            ManagedChannel channel = entry.getValue();

            try {
                channel.shutdown();
                if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                    log.warn("[{}] Channel to {} did not terminate in time, forcing shutdown", nodeId, peerId);
                    channel.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                channel.shutdownNow();
            }
        }

        channels.clear();
        blockingStubs.clear();
        futureStubs.clear();

        log.info("[{}] All gRPC connections closed", nodeId);
    }
}
