package com.danieljhkim.kvdb.kvclustercoordinator.raft;

import com.danieljhkim.kvdb.kvclustercoordinator.raft.election.RaftElectionManager;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.election.RaftElectionTimer;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.election.RaftVoteHandler;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftLog;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftPersistentStateStore;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.replication.RaftAppendEntriesHandler;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.replication.RaftHeartbeatManager;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.replication.RaftReplicationManager;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.replication.RaftStateMachineApplier;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.state.RaftNodeState;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.state.RaftRole;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.statemachine.RaftStateMachine;
import com.danieljhkim.kvdb.proto.raft.AppendEntriesRequest;
import com.danieljhkim.kvdb.proto.raft.AppendEntriesResponse;
import com.danieljhkim.kvdb.proto.raft.RequestVoteRequest;
import com.danieljhkim.kvdb.proto.raft.RequestVoteResponse;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiFunction;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Main Raft node that orchestrates all Raft components.
 *
 * <p>This class integrates:
 * <ul>
 *   <li>State management (RaftNodeState)</li>
 *   <li>Leader election (RaftElectionManager, RaftElectionTimer)</li>
 *   <li>Log replication (RaftHeartbeatManager, RaftReplicationManager)</li>
 *   <li>State machine application (RaftStateMachineApplier)</li>
 * </ul>
 *
 * <p>It manages component lifecycle and ensures proper coordination between components.
 */
@Slf4j
public class RaftNode {

    private final String nodeId;
    private final RaftConfiguration config;

    @Getter
    private final RaftNodeState state;

    private final RaftPersistentStateStore persistentStore;
    private final RaftStateMachine stateMachine;

    // Election components
    private final RaftElectionTimer electionTimer;
    private final RaftElectionManager electionManager;

    @Getter
    private final RaftVoteHandler voteHandler;

    // Replication components
    private final RaftHeartbeatManager heartbeatManager;
    private final RaftReplicationManager replicationManager;

    @Getter
    private final RaftAppendEntriesHandler appendEntriesHandler;

    private final RaftStateMachineApplier stateMachineApplier;

    // RPC clients (to be provided by gRPC layer)
    private final BiFunction<String, RequestVoteRequest, CompletableFuture<RequestVoteResponse>> voteRpcClient;
    private final BiFunction<String, AppendEntriesRequest, CompletableFuture<AppendEntriesResponse>>
            appendEntriesRpcClient;

    // Executor for scheduled tasks
    private final ScheduledExecutorService scheduler;

    private volatile boolean started = false;

    /**
     * Creates a new Raft node.
     *
     * @param nodeId the ID of this node
     * @param config the Raft configuration
     * @param raftLog the Raft log implementation
     * @param persistentStore the persistent state store
     * @param stateMachine the state machine to apply committed entries to
     * @param voteRpcClient function to send RequestVote RPCs
     * @param appendEntriesRpcClient function to send AppendEntries RPCs
     */
    public RaftNode(
            String nodeId,
            RaftConfiguration config,
            RaftLog raftLog,
            RaftPersistentStateStore persistentStore,
            RaftStateMachine stateMachine,
            BiFunction<String, RequestVoteRequest, CompletableFuture<RequestVoteResponse>> voteRpcClient,
            BiFunction<String, AppendEntriesRequest, CompletableFuture<AppendEntriesResponse>> appendEntriesRpcClient) {

        this.nodeId = nodeId;
        this.config = config;
        this.persistentStore = persistentStore;
        this.stateMachine = stateMachine;
        this.voteRpcClient = voteRpcClient;
        this.appendEntriesRpcClient = appendEntriesRpcClient;

        // Initialize state
        this.state = loadOrCreateState(raftLog, persistentStore);

        // Create executor
        this.scheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "raft-" + nodeId);
            t.setDaemon(true);
            return t;
        });

        // Initialize election components
        this.electionTimer = new RaftElectionTimer(nodeId, config, scheduler, this::onElectionTimeout);

        this.electionManager =
                new RaftElectionManager(nodeId, config, state, persistentStore, electionTimer, voteRpcClient);

        this.voteHandler = new RaftVoteHandler(nodeId, state, persistentStore, electionTimer);

        // Initialize replication components
        this.heartbeatManager = new RaftHeartbeatManager(nodeId, config, state, scheduler, appendEntriesRpcClient);

        this.replicationManager = new RaftReplicationManager(nodeId, config, state, appendEntriesRpcClient);

        this.appendEntriesHandler = new RaftAppendEntriesHandler(nodeId, state, persistentStore, electionTimer);

        this.stateMachineApplier = new RaftStateMachineApplier(nodeId, state, stateMachine);

        // Register for immediate role change notifications (event-driven, not polling)
        this.state.addRoleChangeListener(this::handleRoleTransition);

        log.info("[{}] RaftNode created in term {}", nodeId, state.getCurrentTerm());
    }

    /**
     * Starts the Raft node.
     */
    public synchronized void start() {
        if (started) {
            log.warn("[{}] RaftNode already started", nodeId);
            return;
        }

        log.info("[{}] Starting RaftNode", nodeId);

        // Start state machine applier
        stateMachineApplier.start();

        // Start election timer (will trigger elections when needed)
        electionTimer.start();

        started = true;
        log.info("[{}] RaftNode started successfully", nodeId);
    }

    /**
     * Stops the Raft node.
     */
    public synchronized void stop() {
        if (!started) {
            log.warn("[{}] RaftNode not started", nodeId);
            return;
        }

        log.info("[{}] Stopping RaftNode", nodeId);

        // Stop all components
        electionTimer.stop();
        heartbeatManager.stop();
        stateMachineApplier.stop();

        // Shutdown scheduler
        scheduler.shutdown();

        started = false;
        log.info("[{}] RaftNode stopped", nodeId);
    }

    /**
     * Handles incoming RequestVote RPC.
     */
    public RequestVoteResponse handleRequestVote(RequestVoteRequest request) {
        return voteHandler.handleRequestVote(request);
    }

    /**
     * Handles incoming AppendEntries RPC.
     */
    public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
        AppendEntriesResponse response = appendEntriesHandler.handleAppendEntries(request);

        // Trigger state machine applier if commit index advanced
        if (state.getCommitIndex() > state.getLastApplied()) {
            stateMachineApplier.applyCommittedEntries();
        }

        return response;
    }

    /**
     * Submits a command to be replicated (only works if this node is the leader).
     *
     * @param command the command to replicate
     * @return CompletableFuture that completes when command is committed
     */
    public CompletableFuture<Void> submitCommand(RaftCommand command) {
        if (!state.isLeader()) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Not the leader. Current leader: " + state.getCurrentLeader()));
        }

        try {
            // Append to local log
            long index = state.getLog().size() + 1;
            long term = state.getCurrentTerm();
            var entry = com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftLogEntry.create(
                    index, term, command);
            state.getLog().append(entry);

            log.debug("[{}] Appended command to log at index {} term {}", nodeId, index, term);

            // Replicate to followers
            return replicationManager.replicateToAll().thenRun(() -> {
                // After replication, apply to state machine if committed
                if (state.getCommitIndex() >= index) {
                    stateMachineApplier.applyCommittedEntries();
                }
            });

        } catch (IOException e) {
            log.error("[{}] Failed to append command to log", nodeId, e);
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * Gets the current role of this node.
     */
    public RaftRole getCurrentRole() {
        return state.getCurrentRole();
    }

    /**
     * Gets the current term.
     */
    public long getCurrentTerm() {
        return state.getCurrentTerm();
    }

    /**
     * Gets the current leader ID (may be null).
     */
    public String getCurrentLeader() {
        return state.getCurrentLeader();
    }

    /**
     * Returns true if this node is the leader.
     */
    public boolean isLeader() {
        return state.isLeader();
    }

    /**
     * Gets the gRPC address of the current leader.
     * Returns null if leader is unknown.
     */
    public String getLeaderAddress() {
        String leaderId = state.getCurrentLeader();
        if (leaderId == null) {
            return null;
        }

        // Look up leader address from config
        return config.getClusterMembers().get(leaderId);
    }

    /**
     * Called when election timeout expires.
     */
    private void onElectionTimeout() {
        log.debug("[{}] Election timeout, starting election", nodeId);
        electionManager.startElection();
    }

    /**
     * Handles transitions between Raft roles.
     * Called immediately when role changes via the listener mechanism.
     */
    private void handleRoleTransition(RaftRole from, RaftRole to) {
        log.info("[{}] Role transition: {} → {}", nodeId, from, to);

        switch (to) {
            case LEADER -> onBecomeLeader();
            case FOLLOWER -> onBecomeFollower();
            case CANDIDATE -> onBecomeCandidate();
        }
    }

    /**
     * Called when this node becomes leader.
     */
    private void onBecomeLeader() {
        log.info("[{}] Became LEADER in term {}", nodeId, state.getCurrentTerm());

        // Start sending heartbeats
        heartbeatManager.start();

        // Initial replication to establish authority
        replicationManager.replicateToAll();

        log.info("[{}] Started heartbeat manager and initial replication", nodeId);
    }

    /**
     * Called when this node becomes follower.
     */
    private void onBecomeFollower() {
        log.info("[{}] Became FOLLOWER in term {}", nodeId, state.getCurrentTerm());

        // Stop sending heartbeats (no longer leader)
        heartbeatManager.stop();

        // Clear replication state
        replicationManager.clear();

        // Start/restart election timer
        if (!electionTimer.isRunning()) {
            electionTimer.start();
        } else {
            electionTimer.reset();
        }
    }

    /**
     * Called when this node becomes candidate.
     */
    private void onBecomeCandidate() {
        log.info("[{}] Became CANDIDATE in term {}", nodeId, state.getCurrentTerm());

        // Stop heartbeats (candidates don't send heartbeats)
        heartbeatManager.stop();

        // Election timer is already running (triggered the election)
    }

    /**
     * Loads state from persistent storage or creates new state.
     */
    private RaftNodeState loadOrCreateState(RaftLog raftLog, RaftPersistentStateStore store) {
        try {
            var persistedState = store.load();
            return new RaftNodeState(nodeId, raftLog, persistedState.getCurrentTerm(), persistedState.getVotedFor());
        } catch (IOException e) {
            log.warn("[{}] Failed to load persistent state, starting fresh: {}", nodeId, e.getMessage());
            return new RaftNodeState(nodeId, raftLog);
        }
    }
}
