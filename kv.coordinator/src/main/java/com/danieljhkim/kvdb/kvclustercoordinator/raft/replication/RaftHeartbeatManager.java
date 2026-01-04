package com.danieljhkim.kvdb.kvclustercoordinator.raft.replication;

import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftConfiguration;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.state.RaftNodeState;
import com.danieljhkim.kvdb.proto.raft.AppendEntriesRequest;
import com.danieljhkim.kvdb.proto.raft.AppendEntriesResponse;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import lombok.extern.slf4j.Slf4j;

/**
 * Manages periodic heartbeats when node is a leader.
 *
 * <p>Heartbeats are empty AppendEntries RPCs sent to maintain leadership
 * and prevent followers from timing out.
 *
 * <p>Raft paper: §5.2 - "The leader sends periodic heartbeats
 * (AppendEntries RPCs that carry no log entries) to all followers
 * in order to maintain its authority"
 */
@Slf4j
public class RaftHeartbeatManager {

    private final String nodeId;
    private final RaftConfiguration config;
    private final RaftNodeState state;
    private final ScheduledExecutorService scheduler;
    private final BiFunction<String, AppendEntriesRequest, CompletableFuture<AppendEntriesResponse>> rpcClient;

    private final AtomicReference<ScheduledFuture<?>> heartbeatTask = new AtomicReference<>();

    public RaftHeartbeatManager(
            String nodeId,
            RaftConfiguration config,
            RaftNodeState state,
            ScheduledExecutorService scheduler,
            BiFunction<String, AppendEntriesRequest, CompletableFuture<AppendEntriesResponse>> rpcClient) {
        this.nodeId = nodeId;
        this.config = config;
        this.state = state;
        this.scheduler = scheduler;
        this.rpcClient = rpcClient;
    }

    /**
     * Starts sending periodic heartbeats to all followers.
     * Should be called when node becomes leader.
     */
    public void start() {
        stop(); // Stop any existing heartbeat

        long intervalMs = config.getHeartbeatInterval().toMillis();
        ScheduledFuture<?> task = scheduler.scheduleAtFixedRate(
                this::sendHeartbeats,
                0, // Send immediately
                intervalMs,
                TimeUnit.MILLISECONDS);

        heartbeatTask.set(task);
        log.info("[{}] Started heartbeat manager with interval {}ms", nodeId, intervalMs);
    }

    /**
     * Stops sending heartbeats.
     * Should be called when node steps down from leader role.
     */
    public void stop() {
        ScheduledFuture<?> task = heartbeatTask.getAndSet(null);
        if (task != null) {
            task.cancel(false);
            log.info("[{}] Stopped heartbeat manager", nodeId);
        }
    }

    /**
     * Returns true if heartbeats are currently active.
     */
    public boolean isActive() {
        ScheduledFuture<?> task = heartbeatTask.get();
        return task != null && !task.isCancelled() && !task.isDone();
    }

    /**
     * Sends heartbeat (empty AppendEntries) to all peers.
     */
    private void sendHeartbeats() {
        if (!state.isLeader()) {
            log.debug("[{}] Not leader, stopping heartbeats", nodeId);
            stop();
            return;
        }

        long currentTerm = state.getCurrentTerm();
        long commitIndex = state.getCommitIndex();

        config.getPeers().keySet().forEach(peerId -> {
            sendHeartbeat(peerId, currentTerm, commitIndex);
        });
    }

    /**
     * Sends a single heartbeat to a specific peer.
     */
    private void sendHeartbeat(String peerId, long term, long commitIndex) {
        try {
            // Get the previous log entry for consistency check
            Long nextIndex = state.getNextIndex(peerId);
            if (nextIndex == null) {
                log.warn("[{}] No nextIndex for peer {}, skipping heartbeat", nodeId, peerId);
                return;
            }

            long prevLogIndex = nextIndex - 1;
            long prevLogTerm = 0;

            if (prevLogIndex > 0) {
                prevLogTerm = state.getLog()
                        .getEntry(prevLogIndex)
                        .map(entry -> entry.term())
                        .orElse(0L);
            }

            AppendEntriesRequest request = AppendEntriesRequest.newBuilder()
                    .setTerm(term)
                    .setLeaderId(nodeId)
                    .setPrevLogIndex(prevLogIndex)
                    .setPrevLogTerm(prevLogTerm)
                    // Empty entries list = heartbeat
                    .setLeaderCommit(commitIndex)
                    .build();

            log.trace(
                    "[{}] Sending heartbeat to {} (term={}, prevLogIndex={}, prevLogTerm={})",
                    nodeId,
                    peerId,
                    term,
                    prevLogIndex,
                    prevLogTerm);

            rpcClient
                    .apply(peerId, request)
                    .whenComplete((response, error) -> handleHeartbeatResponse(peerId, response, error));

        } catch (Exception e) {
            log.error("[{}] Error sending heartbeat to {}: {}", nodeId, peerId, e.getMessage());
        }
    }

    /**
     * Handles response from heartbeat RPC.
     */
    private void handleHeartbeatResponse(String peerId, AppendEntriesResponse response, Throwable error) {
        if (error != null) {
            log.warn("[{}] Heartbeat to {} failed: {}", nodeId, peerId, error.getMessage());
            return;
        }

        // Check if we discovered a higher term
        if (response.getTerm() > state.getCurrentTerm()) {
            log.warn(
                    "[{}] Discovered higher term {} from {} during heartbeat, stepping down",
                    nodeId,
                    response.getTerm(),
                    peerId);
            state.updateTerm(response.getTerm());
            state.transitionToFollower(null);
            stop();
            return;
        }

        if (!response.getSuccess()) {
            // Log inconsistency detected - this will be handled by replication manager
            log.debug("[{}] Heartbeat to {} failed due to log inconsistency", nodeId, peerId);
        } else {
            log.trace("[{}] Heartbeat to {} succeeded", nodeId, peerId);
        }
    }
}
