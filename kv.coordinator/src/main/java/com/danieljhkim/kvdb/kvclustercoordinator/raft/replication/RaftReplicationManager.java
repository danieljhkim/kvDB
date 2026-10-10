package com.danieljhkim.kvdb.kvclustercoordinator.raft.replication;

import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftConfiguration;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftLog;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftLogEntry;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftPersistentStateStore;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftSnapshotStore;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.state.RaftNodeState;
import com.danieljhkim.kvdb.proto.raft.AppendEntriesRequest;
import com.danieljhkim.kvdb.proto.raft.AppendEntriesResponse;
import com.danieljhkim.kvdb.proto.raft.InstallSnapshotRequest;
import com.danieljhkim.kvdb.proto.raft.InstallSnapshotResponse;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import lombok.extern.slf4j.Slf4j;

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
    private final BiFunction<String, InstallSnapshotRequest, CompletableFuture<InstallSnapshotResponse>>
            snapshotRpcClient;
    private final RaftSnapshotStore snapshotStore;
    private final RaftPersistentStateStore persistentStore;

    // Track which peers are currently being replicated to (prevent concurrent replication to same peer)
    private final Map<String, CompletableFuture<Void>> activeReplications = new ConcurrentHashMap<>();

    // Guarded by state, like term/role transitions and all replication callbacks.
    private long generation;

    private record Leadership(long term, long generation) {}

    private boolean isCurrent(Leadership leadership) {
        return state.isLeader() && state.getCurrentTerm() == leadership.term() && generation == leadership.generation();
    }

    private <T> CompletableFuture<T> supersededLeadership() {
        return CompletableFuture.failedFuture(new IllegalStateException("Replication leadership was superseded"));
    }

    public RaftReplicationManager(
            String nodeId,
            RaftConfiguration config,
            RaftNodeState state,
            RaftPersistentStateStore persistentStore,
            BiFunction<String, AppendEntriesRequest, CompletableFuture<AppendEntriesResponse>> rpcClient) {
        this(
                nodeId,
                config,
                state,
                persistentStore,
                rpcClient,
                (peer, request) -> CompletableFuture.failedFuture(
                        new IllegalStateException("InstallSnapshot RPC client is not configured")),
                null);
    }

    public RaftReplicationManager(
            String nodeId,
            RaftConfiguration config,
            RaftNodeState state,
            RaftPersistentStateStore persistentStore,
            BiFunction<String, AppendEntriesRequest, CompletableFuture<AppendEntriesResponse>> rpcClient,
            BiFunction<String, InstallSnapshotRequest, CompletableFuture<InstallSnapshotResponse>> snapshotRpcClient,
            RaftSnapshotStore snapshotStore) {
        this.nodeId = nodeId;
        this.config = config;
        this.state = state;
        this.persistentStore = Objects.requireNonNull(persistentStore, "persistentStore cannot be null");
        this.rpcClient = rpcClient;
        this.snapshotRpcClient = snapshotRpcClient;
        this.snapshotStore = snapshotStore;
    }

    /**
     * Replicates log entries to all followers.
     * Called by leader when new entries are appended to the log.
     *
     * @return CompletableFuture that completes when majority has replicated
     */
    public CompletableFuture<Void> replicateToAll() {
        synchronized (state) {
            return replicateToAll(new Leadership(state.getCurrentTerm(), generation));
        }
    }

    private CompletableFuture<Void> replicateToAll(Leadership leadership) {
        if (!state.isLeader()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Not a leader"));
        }

        int quorumSize = config.getQuorumSize();
        if (quorumSize == 1) {
            updateCommitIndex(leadership);
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<Void> quorumReached = new CompletableFuture<>();
        AtomicInteger successfulServers = new AtomicInteger(1); // The leader stores the entry locally.
        AtomicInteger failedServers = new AtomicInteger();

        for (String peerId : config.getPeers().keySet()) {
            replicateToPeer(peerId, leadership).whenComplete((ignored, error) -> {
                synchronized (state) {
                    if (!isCurrent(leadership)) {
                        quorumReached.completeExceptionally(
                                new IllegalStateException("Replication leadership was superseded"));
                        return;
                    }
                    if (error == null) {
                        updateCommitIndex(leadership);
                        if (successfulServers.incrementAndGet() >= quorumSize) {
                            quorumReached.complete(null);
                        }
                    } else if (config.getClusterSize() - failedServers.incrementAndGet() < quorumSize) {
                        quorumReached.completeExceptionally(
                                new IllegalStateException("Unable to replicate command to a majority", error));
                    }
                }
            });
        }

        return quorumReached;
    }

    /**
     * Replicates log entries to a specific peer.
     * If already replicating to this peer, returns the existing future.
     *
     * @param peerId the peer to replicate to
     * @return CompletableFuture that completes when replication succeeds
     */
    public CompletableFuture<Void> replicateToPeer(String peerId) {
        synchronized (state) {
            return replicateToPeer(peerId, new Leadership(state.getCurrentTerm(), generation));
        }
    }

    private CompletableFuture<Void> replicateToPeer(String peerId, Leadership leadership) {
        if (!isCurrent(leadership)) {
            return supersededLeadership();
        }
        // Check if already replicating to this peer
        CompletableFuture<Void> existing = activeReplications.get(peerId);
        if (existing != null && !existing.isDone()) {
            log.trace("[{}] Already replicating to {}, returning existing future", nodeId, peerId);
            return existing;
        }

        CompletableFuture<Void> future = doReplication(peerId, leadership);
        activeReplications.put(peerId, future);

        future.whenComplete((result, error) -> {
            activeReplications.remove(peerId, future);
            if (error != null) {
                log.warn("[{}] Replication to {} failed: {}", nodeId, peerId, error.getMessage());
            }
        });

        return future;
    }

    /**
     * Performs the actual replication to a peer.
     */
    private CompletableFuture<Void> doReplication(String peerId, Leadership leadership) {
        if (!isCurrent(leadership)) {
            return supersededLeadership();
        }

        try {
            Long nextIndex = state.getNextIndex(peerId);
            if (nextIndex == null) {
                log.warn("[{}] No nextIndex for peer {}, cannot replicate", nodeId, peerId);
                return CompletableFuture.failedFuture(
                        new IllegalStateException("No replication state for peer " + peerId));
            }

            RaftLog log = state.getLog();
            if (nextIndex <= log.compactedIndex()) {
                return sendSnapshot(peerId, 0, leadership);
            }
            long currentTerm = leadership.term();
            long commitIndex = state.getCommitIndex();

            // Get previous log entry for consistency check
            long prevLogIndex = nextIndex - 1;
            long prevLogTerm = 0;
            if (prevLogIndex > 0) {
                prevLogTerm = log.getTerm(prevLogIndex).orElse(0L);
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

            this.log.debug(
                    "[{}] Replicating {} entries to {} (nextIndex={}, prevLogIndex={}, prevLogTerm={})",
                    nodeId,
                    entriesToSend.size(),
                    peerId,
                    nextIndex,
                    prevLogIndex,
                    prevLogTerm);

            return rpcClient.apply(peerId, request).thenCompose(response -> {
                synchronized (state) {
                    return handleReplicationResponse(peerId, nextIndex, leadership, response);
                }
            });

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
        long lastIndex = log.lastIndex();

        for (long i = fromIndex; i <= lastIndex && entries.size() < maxEntries; i++) {
            log.getEntry(i).ifPresent(entries::add);
        }

        return entries;
    }

    /**
     * Handles the response from an AppendEntries RPC.
     */
    private CompletableFuture<Void> handleReplicationResponse(
            String peerId, long nextIndex, Leadership leadership, AppendEntriesResponse response) {

        // Check for higher term
        if (response.getTerm() > state.getCurrentTerm()) {
            log.warn("[{}] Discovered higher term {} from {}, stepping down", nodeId, response.getTerm(), peerId);
            return stepDownForHigherTerm(
                    response.getTerm(), "Stepped down after discovering higher term " + response.getTerm());
        }

        if (!isCurrent(leadership) || response.getTerm() != leadership.term()) {
            return supersededLeadership();
        }

        if (response.getSuccess()) {
            // Success - update matchIndex and nextIndex
            long newMatchIndex = response.getMatchIndex();
            state.setMatchIndex(peerId, newMatchIndex);

            log.debug("[{}] Successfully replicated to {} up to index {}", nodeId, peerId, newMatchIndex);

            // Check if there are more entries to replicate
            if (newMatchIndex < state.getLog().lastIndex()) {
                log.trace("[{}] More entries to replicate to {}, continuing", nodeId, peerId);
                return doReplication(peerId, leadership);
            }

            return CompletableFuture.completedFuture(null);

        } else {
            // Failure - decrement nextIndex and retry
            handleReplicationFailure(peerId, nextIndex, response);
            return doReplication(peerId, leadership); // Retry with updated nextIndex
        }
    }

    /**
     * Durably adopts a higher term learned from a response, then steps down. The returned future always fails: with
     * the step-down message, or with the persistence error when the term could not be made durable (in which case the
     * node's in-memory term and role are unchanged).
     */
    private <T> CompletableFuture<T> stepDownForHigherTerm(long term, String message) {
        try {
            RaftTermAdoption.adoptHigherTerm(state, persistentStore, term);
        } catch (IOException e) {
            log.error("[{}] Failed to persist higher term {}, not adopting it", nodeId, term, e);
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Unable to persist higher term " + term, e));
        }
        return CompletableFuture.failedFuture(new IllegalStateException(message));
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
            log.debug(
                    "[{}] Log conflict with {}, adjusting nextIndex from {} to {} (conflictIndex={}, conflictTerm={})",
                    nodeId,
                    peerId,
                    currentNextIndex,
                    newNextIndex,
                    response.getConflictIndex(),
                    response.getConflictTerm());
        } else {
            // Slow backtracking: decrement by 1
            newNextIndex = Math.max(1, currentNextIndex - 1);
            log.debug(
                    "[{}] AppendEntries to {} failed, decrementing nextIndex from {} to {}",
                    nodeId,
                    peerId,
                    currentNextIndex,
                    newNextIndex);
        }

        state.setNextIndex(peerId, newNextIndex);
    }

    private CompletableFuture<Void> sendSnapshot(String peerId, long offset, Leadership leadership) {
        if (!isCurrent(leadership)) {
            return supersededLeadership();
        }
        if (snapshotStore == null) {
            return CompletableFuture.failedFuture(new IllegalStateException("Snapshot store is not configured"));
        }
        try {
            RaftSnapshotStore.Snapshot snapshot = snapshotStore
                    .load()
                    .orElseThrow(() -> new IllegalStateException("Compacted log has no durable snapshot"));
            byte[] data = snapshot.data();
            if (offset < 0 || offset > data.length) {
                return CompletableFuture.failedFuture(
                        new IOException("Follower requested invalid snapshot offset " + offset));
            }
            int length = Math.min(RaftSnapshotStore.MAX_CHUNK_BYTES, data.length - Math.toIntExact(offset));
            byte[] chunk =
                    java.util.Arrays.copyOfRange(data, Math.toIntExact(offset), Math.toIntExact(offset) + length);
            long nextOffset = offset + length;
            InstallSnapshotRequest request = InstallSnapshotRequest.newBuilder()
                    .setTerm(leadership.term())
                    .setLeaderId(nodeId)
                    .setLastIncludedIndex(snapshot.lastIncludedIndex())
                    .setLastIncludedTerm(snapshot.lastIncludedTerm())
                    .setOffset(offset)
                    .setData(ByteString.copyFrom(chunk))
                    .setDone(nextOffset == data.length)
                    .setChecksum(snapshot.checksum())
                    .setTotalSize(data.length)
                    .build();
            return snapshotRpcClient.apply(peerId, request).thenCompose(response -> {
                synchronized (state) {
                    if (response.getTerm() > state.getCurrentTerm()) {
                        return stepDownForHigherTerm(response.getTerm(), "Stepped down during snapshot transfer");
                    }
                    if (!isCurrent(leadership) || response.getTerm() != leadership.term()) {
                        return supersededLeadership();
                    }
                    long resumeOffset = response.getNextOffset();
                    if (resumeOffset < 0 || resumeOffset > data.length) {
                        return CompletableFuture.failedFuture(
                                new IOException("Follower returned invalid snapshot resume offset " + resumeOffset));
                    }
                    if (!response.getSuccess()) {
                        return sendSnapshot(peerId, resumeOffset, leadership);
                    }
                    if (resumeOffset < data.length) {
                        return sendSnapshot(peerId, resumeOffset, leadership);
                    }
                    state.setMatchIndex(peerId, snapshot.lastIncludedIndex());
                    return doReplication(peerId, leadership);
                }
            });
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * Updates the commit index based on matchIndex values.
     * Raft paper §5.3, §5.4: Leader commits entry when majority has replicated it
     * AND it's from the current term.
     */
    private void updateCommitIndex(Leadership leadership) {
        if (!isCurrent(leadership)) {
            return;
        }

        long currentTerm = leadership.term();
        long currentCommitIndex = state.getCommitIndex();

        // Find the highest index replicated on a majority
        int clusterSize = config.getClusterSize();
        long majorityMatchIndex = state.computeMajorityMatchIndex(clusterSize);

        // Can only commit entries from current term (§5.4.2)
        // This prevents committing entries from previous terms directly
        if (majorityMatchIndex > currentCommitIndex) {
            try {
                // Verify the entry is from current term
                Long entryTerm = state.getLog().getTerm(majorityMatchIndex).orElse(null);
                if (entryTerm != null && entryTerm == currentTerm) {
                    state.advanceCommitIndex(majorityMatchIndex);
                    log.info("[{}] Advanced commitIndex to {} (majority replicated)", nodeId, majorityMatchIndex);
                } else {
                    log.trace(
                            "[{}] Entry at {} is not from current term (entry term={}), not committing",
                            nodeId,
                            majorityMatchIndex,
                            entryTerm);
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
        synchronized (state) {
            generation++;
            activeReplications.clear();
            log.debug("[{}] Cleared replication manager", nodeId);
        }
    }
}
