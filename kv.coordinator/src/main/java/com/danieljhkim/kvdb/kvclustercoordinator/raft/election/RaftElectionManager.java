package com.danieljhkim.kvdb.kvclustercoordinator.raft.election;

import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftConfiguration;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftPersistentStateStore;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.state.RaftNodeState;
import com.danieljhkim.kvdb.proto.raft.RequestVoteRequest;
import com.danieljhkim.kvdb.proto.raft.RequestVoteResponse;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Manages the election process for Raft consensus.
 *
 * <p>When a follower's election timeout expires, this manager:
 * 1. Increments the term and transitions to CANDIDATE
 * 2. Votes for itself
 * 3. Sends RequestVote RPCs to all peers in parallel
 * 4. Collects votes and determines election outcome:
 *    - If majority votes received → become LEADER
 *    - If AppendEntries from valid leader → become FOLLOWER
 *    - If election timeout → start new election
 *
 * <p>Thread-safety: This class is thread-safe.
 */
@Slf4j
public class RaftElectionManager {

    private final String nodeId;
    private final RaftConfiguration config;
    private final RaftNodeState state;
    private final RaftPersistentStateStore persistentStore;
    private final RaftElectionTimer electionTimer;
    private final BiFunction<String, RequestVoteRequest, CompletableFuture<RequestVoteResponse>> rpcClient;

    // Track current election state
    private final AtomicInteger votesReceived = new AtomicInteger(0);
    private final Set<String> votedInCurrentElection = ConcurrentHashMap.newKeySet();

    @Getter
    private volatile boolean electionWon = false;

    /**
     * Creates a new election manager.
     *
     * @param nodeId the ID of this Raft node
     * @param config the Raft configuration
     * @param state the Raft node state
     * @param persistentStore storage for persistent state
     * @param electionTimer the election timer
     * @param rpcClient function to send RequestVote RPCs (peerId, request) → response future
     */
    public RaftElectionManager(
            String nodeId,
            RaftConfiguration config,
            RaftNodeState state,
            RaftPersistentStateStore persistentStore,
            RaftElectionTimer electionTimer,
            BiFunction<String, RequestVoteRequest, CompletableFuture<RequestVoteResponse>> rpcClient) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId cannot be null");
        this.config = Objects.requireNonNull(config, "config cannot be null");
        this.state = Objects.requireNonNull(state, "state cannot be null");
        this.persistentStore = Objects.requireNonNull(persistentStore, "persistentStore cannot be null");
        this.electionTimer = Objects.requireNonNull(electionTimer, "electionTimer cannot be null");
        this.rpcClient = Objects.requireNonNull(rpcClient, "rpcClient cannot be null");
    }

    /**
     * Starts a new election.
     * This is typically called when the election timeout expires.
     */
    public void startElection() {
        synchronized (state) {
            // Only start election if we're not already a leader
            if (state.isLeader()) {
                log.debug("[{}] Already a leader, ignoring election trigger", nodeId);
                return;
            }

            // 1. FIRST reset election timer to prevent race condition
            // where another timeout fires during state transition
            electionTimer.reset();

            // 2. Reset election state before transitioning
            resetElectionState();

            // 3. Increment term and transition to CANDIDATE (§5.2)
            state.becomeCandidate();
            long currentTerm = state.getCurrentTerm();

            // 4. Persist the term and self-vote BEFORE proceeding
            // This ensures crash safety - if we crash, we won't lose our vote
            try {
                persistentStore.save(currentTerm, nodeId);
            } catch (IOException e) {
                log.error(
                        "[{}] Failed to persist election state for term {}, aborting election", nodeId, currentTerm, e);
                // Revert to follower if we can't persist
                state.transitionToFollower(null);
                return;
            }

            // 5. Count our own vote
            votesReceived.set(1);
            votedInCurrentElection.add(nodeId);

            log.info(
                    "[{}] Starting election for term {} (cluster size: {}, quorum: {})",
                    nodeId,
                    currentTerm,
                    config.getClusterSize(),
                    config.getQuorumSize());

            // 6. Send RequestVote RPCs to all peers
            sendVoteRequests(currentTerm);
        }
    }

    /**
     * Sends RequestVote RPCs to all peers in parallel.
     */
    private void sendVoteRequests(long electionTerm) {
        Map<String, String> peers = config.getPeers();
        if (peers.isEmpty()) {
            // Single-node cluster, automatically win
            log.info("[{}] Single-node cluster, automatically winning election", nodeId);
            handleElectionWon(electionTerm);
            return;
        }

        // Build the RequestVote request
        RequestVoteRequest request = buildVoteRequest(electionTerm);

        // Send to all peers in parallel
        for (Map.Entry<String, String> peer : peers.entrySet()) {
            String peerId = peer.getKey();
            sendVoteRequest(peerId, request, electionTerm);
        }
    }

    /**
     * Sends a RequestVote RPC to a single peer.
     */
    private void sendVoteRequest(String peerId, RequestVoteRequest request, long electionTerm) {
        rpcClient.apply(peerId, request).whenComplete((response, error) -> {
            if (error != null) {
                log.warn("[{}] Failed to get vote from {}: {}", nodeId, peerId, error.getMessage());
                return;
            }

            handleVoteResponse(peerId, response, electionTerm);
        });
    }

    /**
     * Handles a vote response from a peer.
     */
    private void handleVoteResponse(String peerId, RequestVoteResponse response, long electionTerm) {
        synchronized (state) {
            // Ignore response if we're no longer a candidate or the term has changed
            if (!state.isCandidate() || state.getCurrentTerm() != electionTerm) {
                log.trace("[{}] Ignoring vote response from {} (no longer candidate or term changed)", nodeId, peerId);
                return;
            }

            // Check if we discovered a higher term (§5.1)
            if (response.getTerm() > electionTerm) {
                log.info("[{}] Discovered higher term {} from {}, stepping down", nodeId, response.getTerm(), peerId);

                // Persist FIRST, then update memory
                try {
                    persistentStore.save(response.getTerm(), null);
                } catch (IOException e) {
                    log.error("[{}] Failed to persist state after discovering higher term", nodeId, e);
                    // Still step down even if persistence fails - safety violation is worse
                }

                state.becomeFollower(response.getTerm(), null);
                electionTimer.reset();
                return;
            }

            // Count the vote if granted
            if (response.getVoteGranted() && votedInCurrentElection.add(peerId)) {
                int votes = votesReceived.incrementAndGet();
                log.debug("[{}] Received vote from {} ({}/{})", nodeId, peerId, votes, config.getClusterSize());

                // Check if we've won the election (§5.2)
                if (!electionWon && votes >= config.getQuorumSize()) {
                    handleElectionWon(electionTerm);
                }
            } else if (!response.getVoteGranted()) {
                log.debug("[{}] Vote denied by {}", nodeId, peerId);
            }
        }
    }

    /**
     * Handles winning the election by transitioning to LEADER.
     */
    private void handleElectionWon(long electionTerm) {
        synchronized (state) {
            // Double-check we're still a candidate in the same term
            if (!state.isCandidate() || state.getCurrentTerm() != electionTerm) {
                return;
            }

            electionWon = true;
            log.info("[{}] Won election for term {} with {} votes", nodeId, electionTerm, votesReceived.get());

            // Transition to LEADER
            state.becomeLeader(config.getPeers().keySet());

            // Stop election timer (leaders don't need it)
            electionTimer.stop();

            // Note: The heartbeat manager should be started by the RaftNode orchestrator
        }
    }

    /**
     * Builds a RequestVote request for the current state.
     */
    private RequestVoteRequest buildVoteRequest(long term) {
        long lastLogIndex = state.getLog().size();
        long lastLogTerm = 0;
        if (lastLogIndex > 0) {
            try {
                lastLogTerm = state.getLog()
                        .getEntry(lastLogIndex)
                        .map(com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftLogEntry::term)
                        .orElse(0L);
            } catch (IOException e) {
                log.error("[{}] Failed to read last log entry term", nodeId, e);
                // Continue with lastLogTerm = 0
            }
        }

        return RequestVoteRequest.newBuilder()
                .setTerm(term)
                .setCandidateId(nodeId)
                .setLastLogIndex(lastLogIndex)
                .setLastLogTerm(lastLogTerm)
                .build();
    }

    /**
     * Resets election state for a new election.
     */
    private void resetElectionState() {
        votesReceived.set(0);
        votedInCurrentElection.clear();
        electionWon = false;
    }

    /**
     * Returns the number of votes received in the current election.
     */
    public int getVotesReceived() {
        return votesReceived.get();
    }
}
