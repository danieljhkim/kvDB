package com.danieljhkim.kvdb.kvclustercoordinator.raft.state;

import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftLog;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

/**
 * Maintains the Raft state as specified in the Raft paper.
 *
 * <p>
 * This class manages both persistent state (currentTerm, votedFor, log) and volatile state (commitIndex, lastApplied,
 * role, and leader-specific indices).
 *
 * <p>
 * Thread-safety: This class is thread-safe. State transitions and term updates are atomic.
 */
@Slf4j
public class RaftNodeState {


    // Persistent state on all servers (updated on stable storage before responding to RPCs)
    private final AtomicLong currentTerm;
    private final AtomicReference<String> votedFor;
    private final RaftLog raftLog;

    // Volatile state on all servers
    private final AtomicLong commitIndex;
    private final AtomicLong lastApplied;
    private final AtomicReference<RaftRole> currentRole;
    private final AtomicReference<String> currentLeader;

    // Volatile state on leaders (reinitialized after election)
    private final RaftLeaderState leaderState;

    private final String nodeId;

    public RaftNodeState(String nodeId, RaftLog log) {
        this(nodeId, log, 0, null);
    }

    public RaftNodeState(String nodeId, RaftLog log, long initialTerm, String initialVotedFor) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId cannot be null");
        this.raftLog = Objects.requireNonNull(log, "log cannot be null");
        this.currentTerm = new AtomicLong(initialTerm);
        this.votedFor = new AtomicReference<>(initialVotedFor);
        this.commitIndex = new AtomicLong(0);
        this.lastApplied = new AtomicLong(0);
        this.currentRole = new AtomicReference<>(RaftRole.FOLLOWER);
        this.currentLeader = new AtomicReference<>(null);
        this.leaderState = new RaftLeaderState(nodeId);
    }

    // === Persistent State Accessors ===

    public long getCurrentTerm() {
        return currentTerm.get();
    }

    public String getVotedFor() {
        return votedFor.get();
    }

    public RaftLog getLog() {
        return raftLog;
    }

    // === Volatile State Accessors ===

    public long getCommitIndex() {
        return commitIndex.get();
    }

    public long getLastApplied() {
        return lastApplied.get();
    }

    public RaftRole getCurrentRole() {
        return currentRole.get();
    }

    public String getCurrentLeader() {
        return currentLeader.get();
    }

    public boolean isLeader() {
        return currentRole.get() == RaftRole.LEADER;
    }

    public boolean isCandidate() {
        return currentRole.get() == RaftRole.CANDIDATE;
    }

    public boolean isFollower() {
        return currentRole.get() == RaftRole.FOLLOWER;
    }

    // === Leader State Accessors ===

    public Long getNextIndex(String peerId) {
        return leaderState.getNextIndex(peerId);
    }

    public Long getMatchIndex(String peerId) {
        return leaderState.getMatchIndex(peerId);
    }

    public Map<String, Long> getNextIndexMap() {
        return leaderState.getNextIndexMap();
    }

    public Map<String, Long> getMatchIndexMap() {
        return leaderState.getMatchIndexMap();
    }

    // === State Mutations ===

    /**
     * Increments the current term and clears votedFor.
     *
     * @return the new term value
     */
    public long incrementTerm() {
        long newTerm = currentTerm.incrementAndGet();
        votedFor.set(null);
        log.info("[{}] Incremented term to {}", nodeId, newTerm);
        return newTerm;
    }

    /**
     * Updates the current term if the new term is greater. Clears votedFor if term changed.
     *
     * @param newTerm the new term to potentially adopt
     * @return true if the term was updated
     */
    public boolean updateTerm(long newTerm) {
        long oldTerm = currentTerm.get();
        if (newTerm > oldTerm) {
            currentTerm.set(newTerm);
            votedFor.set(null);
            log.info("[{}] Updated term from {} to {}", nodeId, oldTerm, newTerm);
            return true;
        }
        return false;
    }

    /**
     * Sets votedFor for the current term.
     *
     * @param candidateId the candidate to vote for
     */
    public void setVotedFor(String candidateId) {
        votedFor.set(candidateId);
        log.debug("[{}] Voted for {} in term {}", nodeId, candidateId, currentTerm.get());
    }

    /**
     * Updates the commit index.
     *
     * @param newCommitIndex the new commit index
     */
    public void updateCommitIndex(long newCommitIndex) {
        long oldCommitIndex = commitIndex.getAndUpdate(current -> Math.max(current, newCommitIndex));
        if (newCommitIndex > oldCommitIndex) {
            log.debug("[{}] Updated commit index from {} to {}", nodeId, oldCommitIndex, newCommitIndex);
        }
    }

    /**
     * Updates the last applied index.
     *
     * @param newLastApplied the new last applied index
     */
    public void updateLastApplied(long newLastApplied) {
        lastApplied.set(newLastApplied);
    }

    /**
     * Sets the current leader.
     *
     * @param leaderId the leader node ID
     */
    public void setCurrentLeader(String leaderId) {
        currentLeader.set(leaderId);
    }

    // === Role Transitions ===

    /**
     * Transitions to FOLLOWER state. This can happen from any state.
     *
     * @param term the term to adopt
     * @param leaderId the current leader (may be null)
     */
    public void becomeFollower(long term, String leaderId) {
        updateTerm(term);
        RaftRole oldRole = currentRole.getAndSet(RaftRole.FOLLOWER);
        currentLeader.set(leaderId);
        if (oldRole != RaftRole.FOLLOWER) {
            log.info("[{}] Transitioned from {} to FOLLOWER in term {} (leader: {})", nodeId, oldRole, term, leaderId);
            clearLeaderState();
        }
    }

    /**
     * Transitions to CANDIDATE state. Should only be called from FOLLOWER state.
     */
    public void becomeCandidate() {
        long newTerm = incrementTerm();
        RaftRole oldRole = currentRole.getAndSet(RaftRole.CANDIDATE);
        currentLeader.set(null);
        votedFor.set(nodeId); // Vote for self
        clearLeaderState();
        log.info("[{}] Transitioned from {} to CANDIDATE in term {}", nodeId, oldRole, newTerm);
    }

    /**
     * Transitions to LEADER state. Should only be called from CANDIDATE state after winning election.
     *
     * @param peerIds the IDs of all peer nodes in the cluster
     */
    public void becomeLeader(Iterable<String> peerIds) {
        RaftRole oldRole = currentRole.getAndSet(RaftRole.LEADER);
        currentLeader.set(nodeId);
        leaderState.initialize(peerIds, raftLog.size());
        log.info("[{}] Transitioned from {} to LEADER in term {}", nodeId, oldRole, currentTerm.get());
    }

    /**
     * Clears leader-specific state (used when transitioning out of LEADER role).
     */
    private void clearLeaderState() {
        leaderState.clear();
    }

    /**
     * Updates nextIndex for a follower (typically after failed AppendEntries).
     *
     * @param followerId the follower ID
     * @param newNextIndex the new nextIndex value
     */
    public void setNextIndex(String followerId, long newNextIndex) {
        leaderState.setNextIndex(followerId, newNextIndex);
    }

    /**
     * Updates matchIndex for a follower (after successful AppendEntries).
     *
     * @param followerId the follower ID
     * @param newMatchIndex the new matchIndex value
     */
    public void setMatchIndex(String followerId, long newMatchIndex) {
        leaderState.setMatchIndex(followerId, newMatchIndex);
    }

    @Override
    public String toString() {
        return "RaftNodeState{" + "nodeId='" + nodeId + '\'' + ", currentTerm=" + currentTerm.get() + ", votedFor='"
                + votedFor.get() + '\'' + ", currentRole=" + currentRole.get() + ", currentLeader='"
                + currentLeader.get()
                + '\'' + ", commitIndex=" + commitIndex.get() + ", lastApplied=" + lastApplied.get() + ", logSize="
                + raftLog.size() + '}';
    }
}
