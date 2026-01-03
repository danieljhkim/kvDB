package com.danieljhkim.kvdb.kvclustercoordinator.raft.replication;

import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftConfiguration;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftLog;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftLogEntry;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.state.RaftNodeState;
import com.danieljhkim.kvdb.proto.raft.AppendEntriesRequest;
import com.danieljhkim.kvdb.proto.raft.AppendEntriesResponse;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

/**
 * Manages log replication from leader to followers.
 *
 * <p>This implements the leader's log replication logic from Raft paper §5.3:
 * <ul>
 *   <li>Send AppendEntries RPCs to all followers in parallel</li>
 *   <li>Retry indefinitely until followers acknowledge</li>
 *   <li>Update nextIndex and matchIndex based on responses</li>
 *   <li>Advance commitIndex when majority replicated</li>
 * </ul>
 */
@Slf4j
public class RaftReplicationManager {

    private final String nodeId;
    private final RaftConfiguration config;
    private final RaftNodeState state;
    private final BiFunction<String, AppendEntriesRequest, CompletableFuture<AppendEntriesResponse>> rpcClient;

    // Track which peers are currently being replicated to (prevent concurrent replication to same peer)
    private final Map<String, CompletableFuture<Void>> activeReplications = new ConcurrentHashMap<>();

    public RaftReplicationManager(
            String nodeId,
            RaftConfiguration config,
            RaftNodeState state,
            BiFunction<String, AppendEntriesRequest, CompletableFuture<AppendEntriesResponse>> rpcClient) {
        this.nodeId = nodeId;
        this.config = config;
        this.state = state;
        this.rpcClient = rpcClient;
    }

    /**
     * Replicates log entries to all followers.
     * Called by leader when new entries are appended to the log.
     *
     * @return CompletableFuture that completes when majority has replicated
     */
    public CompletableFuture<Void> replicateToAll() {
        if (!state.isLeader()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Not a leader"));
        }

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (String peerId : config.getPeers().keySet()) {
            CompletableFuture<Void> future = replicateToPeer(peerId);
            futures.add(future);
        }

        // Wait for all replications to complete (or fail)
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenRun(this::updateCommitIndex);
    }

    /**
     * Replicates log entries to a specific peer.
     * If already replicating to this peer, returns the existing future.
     *
     * @param peerId the peer to replicate to
     * @return CompletableFuture that completes when replication succeeds
     */
    public CompletableFuture<Void> replicateToPeer(String peerId) {
        // Check if already replicating to this peer
        CompletableFuture<Void> existing = activeReplications.get(peerId);
        if (existing != null && !existing.isDone()) {
            log.trace("[{}] Already replicating to {}, returning existing future", nodeId, peerId);
            return existing;
        }

        CompletableFuture<Void> future = doReplication(peerId);
        activeReplications.put(peerId, future);

        future.whenComplete((result, error) -> {
            activeReplications.remove(peerId);
            if (error != null) {
                log.warn("[{}] Replication to {} failed: {}", nodeId, peerId, error.getMessage());
            }
        });

        return future;
    }

    /**
     * Performs the actual replication to a peer.
     */
    private CompletableFuture<Void> doReplication(String peerId) {
        if (!state.isLeader()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Not a leader"));
        }

        try {
            Long nextIndex = state.getNextIndex(peerId);
            if (nextIndex == null) {
                log.warn("[{}] No nextIndex for peer {}, cannot replicate", nodeId, peerId);
                return CompletableFuture.completedFuture(null);
            }

            RaftLog log = state.getLog();
            long currentTerm = state.getCurrentTerm();
            long commitIndex = state.getCommitIndex();

            // Get previous log entry for consistency check
            long prevLogIndex = nextIndex - 1;
            long prevLogTerm = 0;
            if (prevLogIndex > 0) {
                prevLogTerm = log.getEntry(prevLogIndex)
                        .map(RaftLogEntry::term)
                        .orElse(0L);
            }

            // Get entries to send (up to maxEntriesPerAppendRequest)
            List<RaftLogEntry> entriesToSend = getEntriesToSend(log, nextIndex);

            // Convert to proto format
            List<com.danieljhkim.kvdb.proto.raft.RaftLogEntry> protoEntries = new ArrayList<>();
            for (RaftLogEntry entry : entriesToSend) {
                protoEntries.add(com.danieljhkim.kvdb.proto.raft.RaftLogEntry.parseFrom(entry.toBytes()));
            }

            AppendEntriesRequest request = AppendEntriesRequest.newBuilder()
                    .setTerm(currentTerm)
                    .setLeaderId(nodeId)
                    .setPrevLogIndex(prevLogIndex)
                    .setPrevLogTerm(prevLogTerm)
                    .addAllEntries(protoEntries)
                    .setLeaderCommit(commitIndex)
                    .build();

            this.log.debug("[{}] Replicating {} entries to {} (nextIndex={}, prevLogIndex={}, prevLogTerm={})",
                    nodeId, entriesToSend.size(), peerId, nextIndex, prevLogIndex, prevLogTerm);

            return rpcClient.apply(peerId, request)
                    .thenCompose(response -> handleReplicationResponse(peerId, nextIndex, entriesToSend.size(), response));

        } catch (Exception e) {
            log.error("[{}] Error replicating to {}: {}", nodeId, peerId, e.getMessage(), e);
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * Gets entries to send to a follower, up to maxEntriesPerAppendRequest.
     */
    private List<RaftLogEntry> getEntriesToSend(RaftLog log, long fromIndex) throws IOException {
        List<RaftLogEntry> entries = new ArrayList<>();
        long maxEntries = config.getMaxEntriesPerAppendRequest();
        long lastIndex = log.size();

        for (long i = fromIndex; i <= lastIndex && entries.size() < maxEntries; i++) {
            log.getEntry(i).ifPresent(entries::add);
        }

        return entries;
    }

    /**
     * Handles the response from an AppendEntries RPC.
     */
    private CompletableFuture<Void> handleReplicationResponse(
            String peerId,
            long nextIndex,
            int entriesCount,
            AppendEntriesResponse response) {

        // Check for higher term
        if (response.getTerm() > state.getCurrentTerm()) {
            log.warn("[{}] Discovered higher term {} from {}, stepping down",
                    nodeId, response.getTerm(), peerId);
            state.updateTerm(response.getTerm());
            state.transitionToFollower(null);
            return CompletableFuture.completedFuture(null);
        }

        if (response.getSuccess()) {
            // Success - update matchIndex and nextIndex
            long newMatchIndex = response.getMatchIndex();
            state.setMatchIndex(peerId, newMatchIndex);

            log.debug("[{}] Successfully replicated to {} up to index {}",
                    nodeId, peerId, newMatchIndex);

            // Check if there are more entries to replicate
            if (newMatchIndex < state.getLog().size()) {
                log.trace("[{}] More entries to replicate to {}, continuing", nodeId, peerId);
                return doReplication(peerId);
            }

            return CompletableFuture.completedFuture(null);

        } else {
            // Failure - decrement nextIndex and retry
            handleReplicationFailure(peerId, nextIndex, response);
            return doReplication(peerId); // Retry with updated nextIndex
        }
    }

    /**
     * Handles a failed replication attempt by adjusting nextIndex.
     * Uses the fast log backtracking optimization if conflict info is available.
     */
    private void handleReplicationFailure(String peerId, long currentNextIndex, AppendEntriesResponse response) {
        long newNextIndex;

        if (response.getConflictIndex() > 0) {
            // Fast backtracking: use conflict index from response
            newNextIndex = response.getConflictIndex();
            log.debug("[{}] Log conflict with {}, adjusting nextIndex from {} to {} (conflictIndex={}, conflictTerm={})",
                    nodeId, peerId, currentNextIndex, newNextIndex, response.getConflictIndex(), response.getConflictTerm());
        } else {
            // Slow backtracking: decrement by 1
            newNextIndex = Math.max(1, currentNextIndex - 1);
            log.debug("[{}] AppendEntries to {} failed, decrementing nextIndex from {} to {}",
                    nodeId, peerId, currentNextIndex, newNextIndex);
        }

        state.setNextIndex(peerId, newNextIndex);
    }

    /**
     * Updates the commit index based on matchIndex values.
     * Raft paper §5.3, §5.4: Leader commits entry when majority has replicated it
     * AND it's from the current term.
     */
    private void updateCommitIndex() {
        if (!state.isLeader()) {
            return;
        }

        long currentTerm = state.getCurrentTerm();
        long currentCommitIndex = state.getCommitIndex();

        // Find the highest index replicated on a majority
        int clusterSize = config.getClusterSize();
        long majorityMatchIndex = state.computeMajorityMatchIndex(clusterSize);

        // Can only commit entries from current term (§5.4.2)
        // This prevents committing entries from previous terms directly
        if (majorityMatchIndex > currentCommitIndex) {
            try {
                // Verify the entry is from current term
                RaftLogEntry entry = state.getLog().getEntry(majorityMatchIndex).orElse(null);
                if (entry != null && entry.term() == currentTerm) {
                    state.advanceCommitIndex(majorityMatchIndex);
                    log.info("[{}] Advanced commitIndex to {} (majority replicated)", nodeId, majorityMatchIndex);
                } else {
                    log.trace("[{}] Entry at {} is not from current term (entry term={}), not committing",
                            nodeId, majorityMatchIndex, entry != null ? entry.term() : "null");
                }
            } catch (IOException e) {
                log.error("[{}] Error reading log entry at {}: {}", nodeId, majorityMatchIndex, e.getMessage());
            }
        }
    }

    /**
     * Clears all active replication futures.
     * Should be called when stepping down from leader.
     */
    public void clear() {
        activeReplications.clear();
        log.debug("[{}] Cleared replication manager", nodeId);
    }
}

