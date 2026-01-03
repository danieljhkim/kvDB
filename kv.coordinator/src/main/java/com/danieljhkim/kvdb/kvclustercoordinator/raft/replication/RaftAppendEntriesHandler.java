package com.danieljhkim.kvdb.kvclustercoordinator.raft.replication;

import com.danieljhkim.kvdb.kvclustercoordinator.raft.election.RaftElectionTimer;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftLog;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftLogEntry;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftPersistentStateStore;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.state.RaftNodeState;
import com.danieljhkim.kvdb.proto.raft.AppendEntriesRequest;
import com.danieljhkim.kvdb.proto.raft.AppendEntriesResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/**
 * Handles AppendEntries RPC requests from the leader.
 *
 * <p>This implements the receiver implementation from Raft paper §5.3:
 * <ol>
 *   <li>Reply false if term < currentTerm (§5.1)</li>
 *   <li>Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)</li>
 *   <li>If an existing entry conflicts with a new one (same index but different terms),
 *       delete the existing entry and all that follow it (§5.3)</li>
 *   <li>Append any new entries not already in the log</li>
 *   <li>If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)</li>
 * </ol>
 */
@Slf4j
public class RaftAppendEntriesHandler {

    private final String nodeId;
    private final RaftNodeState state;
    private final RaftPersistentStateStore persistentStore;
    private final RaftElectionTimer electionTimer;

    public RaftAppendEntriesHandler(
            String nodeId,
            RaftNodeState state,
            RaftPersistentStateStore persistentStore,
            RaftElectionTimer electionTimer) {
        this.nodeId = nodeId;
        this.state = state;
        this.persistentStore = persistentStore;
        this.electionTimer = electionTimer;
    }

    /**
     * Handles an AppendEntries RPC request.
     *
     * @param request the AppendEntries request
     * @return the response to send back to the leader
     */
    public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
        long requestTerm = request.getTerm();
        long currentTerm = state.getCurrentTerm();
        String leaderId = request.getLeaderId();

        log.debug(
                "[{}] Received AppendEntries from {} (term={}, prevLogIndex={}, prevLogTerm={}, entries={}, leaderCommit={})",
                nodeId,
                leaderId,
                requestTerm,
                request.getPrevLogIndex(),
                request.getPrevLogTerm(),
                request.getEntriesCount(),
                request.getLeaderCommit());

        // 1. Reply false if term < currentTerm (§5.1)
        if (requestTerm < currentTerm) {
            log.debug(
                    "[{}] Rejecting AppendEntries from {} due to stale term (request={}, current={})",
                    nodeId,
                    leaderId,
                    requestTerm,
                    currentTerm);
            return AppendEntriesResponse.newBuilder()
                    .setTerm(currentTerm)
                    .setSuccess(false)
                    .setFollowerId(nodeId)
                    .build();
        }

        // Update term if necessary and step down to follower
        if (requestTerm > currentTerm) {
            log.info("[{}] Discovered higher term {} from leader {}, updating term", nodeId, requestTerm, leaderId);

            // Persist FIRST, then update memory for crash safety
            try {
                persistentStore.save(requestTerm, state.getVotedFor());
            } catch (IOException e) {
                log.error("[{}] Failed to persist term update, rejecting AppendEntries", nodeId, e);
                return AppendEntriesResponse.newBuilder()
                        .setTerm(currentTerm)
                        .setSuccess(false)
                        .setFollowerId(nodeId)
                        .build();
            }

            state.updateTerm(requestTerm);
        }

        // Recognize leader and reset election timer
        state.transitionToFollower(leaderId);
        electionTimer.reset();

        RaftLog raftLog = state.getLog();

        try {
            // 2. Reply false if log doesn't contain entry at prevLogIndex with matching term
            long prevLogIndex = request.getPrevLogIndex();
            long prevLogTerm = request.getPrevLogTerm();

            if (!checkLogConsistency(raftLog, prevLogIndex, prevLogTerm)) {
                long conflictIndex = findConflictIndex(raftLog, prevLogIndex, prevLogTerm);
                long conflictTerm = getConflictTerm(raftLog, conflictIndex);

                log.debug(
                        "[{}] Log inconsistency at prevLogIndex={}, prevLogTerm={}. Conflict at index={}, term={}",
                        nodeId,
                        prevLogIndex,
                        prevLogTerm,
                        conflictIndex,
                        conflictTerm);

                return AppendEntriesResponse.newBuilder()
                        .setTerm(currentTerm)
                        .setSuccess(false)
                        .setConflictIndex(conflictIndex)
                        .setConflictTerm(conflictTerm)
                        .setFollowerId(nodeId)
                        .build();
            }

            // 3 & 4. Append new entries (handling conflicts)
            if (request.getEntriesCount() > 0) {
                appendEntries(raftLog, request.getEntriesList(), prevLogIndex);
            }

            // 5. Update commit index if necessary
            if (request.getLeaderCommit() > state.getCommitIndex()) {
                long newCommitIndex = Math.min(request.getLeaderCommit(), raftLog.size());
                state.advanceCommitIndex(newCommitIndex);
                log.debug("[{}] Advanced commitIndex to {}", nodeId, newCommitIndex);
            }

            long matchIndex = prevLogIndex + request.getEntriesCount();

            return AppendEntriesResponse.newBuilder()
                    .setTerm(currentTerm)
                    .setSuccess(true)
                    .setMatchIndex(matchIndex)
                    .setFollowerId(nodeId)
                    .build();

        } catch (IOException e) {
            log.error("[{}] Error handling AppendEntries: {}", nodeId, e.getMessage(), e);
            return AppendEntriesResponse.newBuilder()
                    .setTerm(currentTerm)
                    .setSuccess(false)
                    .setFollowerId(nodeId)
                    .build();
        }
    }

    /**
     * Checks if the log contains an entry at prevLogIndex with the given term.
     */
    private boolean checkLogConsistency(RaftLog log, long prevLogIndex, long prevLogTerm) throws IOException {
        // If prevLogIndex is 0, there's nothing to check (beginning of log)
        if (prevLogIndex == 0) {
            return true;
        }

        // Check if we have an entry at prevLogIndex
        Optional<RaftLogEntry> prevEntry = log.getEntry(prevLogIndex);
        if (prevEntry.isEmpty()) {
            return false; // We don't have this entry
        }

        // Check if the term matches
        return prevEntry.get().term() == prevLogTerm;
    }

    /**
     * Finds the index where the conflict occurs for faster log backtracking.
     */
    private long findConflictIndex(RaftLog log, long prevLogIndex, long prevLogTerm) throws IOException {
        // If we don't have the entry at all, return our last log index + 1
        if (prevLogIndex > log.size()) {
            return log.size() + 1;
        }

        // If we have the entry but term doesn't match, find the first entry of the conflicting term
        Optional<RaftLogEntry> conflictEntry = log.getEntry(prevLogIndex);
        if (conflictEntry.isEmpty()) {
            return prevLogIndex;
        }

        long conflictTerm = conflictEntry.get().term();

        // Find the first index with this term
        for (long i = prevLogIndex; i >= 1; i--) {
            Optional<RaftLogEntry> entry = log.getEntry(i);
            if (entry.isEmpty() || entry.get().term() != conflictTerm) {
                return i + 1;
            }
        }

        return 1; // Conflict at the beginning
    }

    /**
     * Gets the term of the entry at conflictIndex.
     */
    private long getConflictTerm(RaftLog log, long conflictIndex) throws IOException {
        if (conflictIndex <= 0 || conflictIndex > log.size()) {
            return 0;
        }
        return log.getEntry(conflictIndex).map(RaftLogEntry::term).orElse(0L);
    }

    /**
     * Appends new entries to the log, handling conflicts.
     * Raft paper §5.3: If an existing entry conflicts with a new one,
     * delete the existing entry and all that follow it.
     */
    private void appendEntries(
            RaftLog raftLog, List<com.danieljhkim.kvdb.proto.raft.RaftLogEntry> protoEntries, long prevLogIndex)
            throws IOException {

        List<RaftLogEntry> entriesToAppend = new ArrayList<>();
        long nextIndex = prevLogIndex + 1;

        for (com.danieljhkim.kvdb.proto.raft.RaftLogEntry protoEntry : protoEntries) {
            long entryIndex = protoEntry.getIndex();

            // Sanity check: entries should be consecutive
            if (entryIndex != nextIndex) {
                log.warn("[{}] Non-consecutive entry index: expected {}, got {}", nodeId, nextIndex, entryIndex);
            }

            // Check if we already have an entry at this index
            Optional<RaftLogEntry> existing = raftLog.getEntry(entryIndex);

            if (existing.isPresent()) {
                // If terms match, skip this entry (already have it)
                if (existing.get().term() == protoEntry.getTerm()) {
                    log.trace("[{}] Entry at index {} already exists with matching term", nodeId, entryIndex);
                    nextIndex++;
                    continue;
                }

                // Terms don't match - delete this entry and all following entries
                log.info(
                        "[{}] Conflict at index {}: existing term {}, new term {}. Truncating.",
                        nodeId,
                        entryIndex,
                        existing.get().term(),
                        protoEntry.getTerm());
                raftLog.truncateAfter(entryIndex - 1);
            }

            // Convert proto entry to our internal format
            RaftLogEntry entry = RaftLogEntry.fromBytes(protoEntry.toByteArray());
            entriesToAppend.add(entry);
            nextIndex++;
        }

        // Append all new entries
        for (RaftLogEntry entry : entriesToAppend) {
            raftLog.append(entry);
            RaftAppendEntriesHandler.log.debug(
                    "[{}] Appended entry at index {} with term {}", nodeId, entry.index(), entry.term());
        }
    }

    /**
     * Persists current term and votedFor to stable storage.
     */
    private void persistState() {
        try {
            persistentStore.save(state.getCurrentTerm(), state.getVotedFor());
        } catch (IOException e) {
            log.error("[{}] Failed to persist state: {}", nodeId, e.getMessage(), e);
        }
    }
}
