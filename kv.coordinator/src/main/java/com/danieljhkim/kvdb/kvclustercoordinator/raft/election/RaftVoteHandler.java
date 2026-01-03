package com.danieljhkim.kvdb.kvclustercoordinator.raft.election;

import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftPersistentStateStore;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.state.RaftNodeState;
import com.danieljhkim.kvdb.proto.raft.RequestVoteRequest;
import com.danieljhkim.kvdb.proto.raft.RequestVoteResponse;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Objects;

/**
 * Handles RequestVote RPC requests during leader election.
 *
 * <p>This handler implements the voting logic as specified in the Raft paper:
 * 1. Reply false if request term < currentTerm
 * 2. If request term > currentTerm, update term and step down to follower
 * 3. Grant vote if:
 *    - Haven't voted in this term (or already voted for this candidate)
 *    - Candidate's log is at least as up-to-date as ours
 *
 * <p>Thread-safety: This class is thread-safe.
 */
@Slf4j
public class RaftVoteHandler {

    private final String nodeId;
    private final RaftNodeState state;
    private final RaftPersistentStateStore persistentStore;
    private final RaftElectionTimer electionTimer;

    /**
     * Creates a new vote handler.
     *
     * @param nodeId the ID of this Raft node
     * @param state the Raft node state
     * @param persistentStore storage for persistent state (term, votedFor)
     * @param electionTimer the election timer to reset on vote grant
     */
    public RaftVoteHandler(
            String nodeId,
            RaftNodeState state,
            RaftPersistentStateStore persistentStore,
            RaftElectionTimer electionTimer) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId cannot be null");
        this.state = Objects.requireNonNull(state, "state cannot be null");
        this.persistentStore = Objects.requireNonNull(persistentStore, "persistentStore cannot be null");
        this.electionTimer = Objects.requireNonNull(electionTimer, "electionTimer cannot be null");
    }

    /**
     * Handles a RequestVote RPC.
     *
     * @param request the vote request
     * @return the vote response
     */
    public RequestVoteResponse handleRequestVote(RequestVoteRequest request) {
        log.debug("[{}] Received RequestVote from {} for term {} (lastLogIndex={}, lastLogTerm={})",
                nodeId, request.getCandidateId(), request.getTerm(),
                request.getLastLogIndex(), request.getLastLogTerm());

        synchronized (state) {
            long currentTerm = state.getCurrentTerm();
            String votedFor = state.getVotedFor();

            // 1. Reply false if request.term < currentTerm (§5.1)
            if (request.getTerm() < currentTerm) {
                log.debug("[{}] Rejecting vote for {} - stale term {} < {}",
                        nodeId, request.getCandidateId(), request.getTerm(), currentTerm);
                return buildResponse(currentTerm, false);
            }

            // 2. If request.term > currentTerm, update term and step down (§5.1)
            if (request.getTerm() > currentTerm) {
                log.info("[{}] Discovered higher term {} from {}, stepping down from term {}",
                        nodeId, request.getTerm(), request.getCandidateId(), currentTerm);
                state.becomeFollower(request.getTerm(), null);
                try {
                    persistentStore.save(request.getTerm(), null);
                } catch (IOException e) {
                    log.error("[{}] Failed to persist state after term update", nodeId, e);
                    // Continue despite persistence failure - in-memory state is updated
                }
                currentTerm = request.getTerm();
                votedFor = null;
            }

            // 3. Check if we can vote for this candidate
            boolean canVote = votedFor == null || votedFor.equals(request.getCandidateId());
            if (!canVote) {
                log.debug("[{}] Rejecting vote for {} - already voted for {} in term {}",
                        nodeId, request.getCandidateId(), votedFor, currentTerm);
                return buildResponse(currentTerm, false);
            }

            // 4. Check if candidate's log is at least as up-to-date as ours (§5.4.1)
            boolean logIsUpToDate = isLogUpToDate(request.getLastLogTerm(), request.getLastLogIndex());
            if (!logIsUpToDate) {
                log.debug("[{}] Rejecting vote for {} - log is not up-to-date (candidate: term={}, index={}; ours: term={}, index={})",
                        nodeId, request.getCandidateId(),
                        request.getLastLogTerm(), request.getLastLogIndex(),
                        getLastLogTerm(), getLastLogIndex());
                return buildResponse(currentTerm, false);
            }

            // 5. Grant vote and reset election timer (§5.2)
            log.info("[{}] Granting vote to {} in term {}", nodeId, request.getCandidateId(), currentTerm);
            state.setVotedFor(request.getCandidateId());
            try {
                persistentStore.save(currentTerm, request.getCandidateId());
            } catch (IOException e) {
                log.error("[{}] Failed to persist vote for {}", nodeId, request.getCandidateId(), e);
                // Continue despite persistence failure - in-memory state is updated
            }
            electionTimer.reset();

            return buildResponse(currentTerm, true);
        }
    }

    /**
     * Checks if the candidate's log is at least as up-to-date as ours.
     *
     * <p>Raft determines which of two logs is more up-to-date by comparing the index and term
     * of the last entries in the logs. If the logs have last entries with different terms,
     * then the log with the later term is more up-to-date. If the logs end with the same term,
     * then whichever log is longer is more up-to-date. (§5.4.1)
     *
     * @param candidateLastLogTerm term of candidate's last log entry
     * @param candidateLastLogIndex index of candidate's last log entry
     * @return true if candidate's log is at least as up-to-date
     */
    private boolean isLogUpToDate(long candidateLastLogTerm, long candidateLastLogIndex) {
        long ourLastLogTerm = getLastLogTerm();
        long ourLastLogIndex = getLastLogIndex();

        // Candidate's log is more up-to-date if:
        // 1. Last log term is greater, OR
        // 2. Last log term is equal AND last log index is >= ours
        if (candidateLastLogTerm > ourLastLogTerm) {
            return true;
        } else if (candidateLastLogTerm == ourLastLogTerm) {
            return candidateLastLogIndex >= ourLastLogIndex;
        } else {
            return false;
        }
    }

    /**
     * Returns the term of our last log entry (0 if log is empty).
     */
    private long getLastLogTerm() {
        long lastIndex = state.getLog().size();
        if (lastIndex == 0) {
            return 0;
        }
        try {
            return state.getLog().getEntry(lastIndex)
                    .map(com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftLogEntry::term)
                    .orElse(0L);
        } catch (IOException e) {
            log.error("[{}] Failed to read last log entry term", nodeId, e);
            return 0;
        }
    }

    /**
     * Returns the index of our last log entry (0 if log is empty).
     */
    private long getLastLogIndex() {
        return state.getLog().size();
    }

    /**
     * Builds a RequestVoteResponse.
     */
    private RequestVoteResponse buildResponse(long term, boolean voteGranted) {
        return RequestVoteResponse.newBuilder()
                .setTerm(term)
                .setVoteGranted(voteGranted)
                .setVoterId(nodeId)
                .build();
    }
}

