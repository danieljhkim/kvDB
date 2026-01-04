package com.danieljhkim.kvdb.kvclustercoordinator.raft.state;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

/**
 * Manages leader-specific volatile state.
 *
 * <p>This state is reinitialized after each election and only maintained by the leader.
 */
@Slf4j
public class RaftLeaderState {

    private final String nodeId;
    private final Map<String, Long> nextIndex; // Next log entry to send to each server
    private final Map<String, Long> matchIndex; // Highest log entry known to be replicated

    public RaftLeaderState(String nodeId) {
        this.nodeId = nodeId;
        this.nextIndex = new ConcurrentHashMap<>();
        this.matchIndex = new ConcurrentHashMap<>();
    }

    /**
     * Initializes leader state for all peers after winning election.
     *
     * @param peerIds the IDs of all peer nodes
     * @param lastLogIndex the index of the last entry in the leader's log
     */
    public void initialize(Iterable<String> peerIds, long lastLogIndex) {
        nextIndex.clear();
        matchIndex.clear();
        for (String peerId : peerIds) {
            nextIndex.put(peerId, lastLogIndex + 1);
            matchIndex.put(peerId, 0L);
        }
        log.debug("[{}] Initialized leader state with lastLogIndex={}", nodeId, lastLogIndex);
    }

    /**
     * Clears all leader state (called when stepping down).
     */
    public void clear() {
        nextIndex.clear();
        matchIndex.clear();
    }

    public Long getNextIndex(String peerId) {
        return nextIndex.get(peerId);
    }

    public Long getMatchIndex(String peerId) {
        return matchIndex.get(peerId);
    }

    public Map<String, Long> getNextIndexMap() {
        return Map.copyOf(nextIndex);
    }

    public Map<String, Long> getMatchIndexMap() {
        return Map.copyOf(matchIndex);
    }

    /**
     * Updates nextIndex for a follower after a failed AppendEntries.
     *
     * @param followerId the follower ID
     * @param newNextIndex the new nextIndex value
     */
    public void setNextIndex(String followerId, long newNextIndex) {
        nextIndex.put(followerId, newNextIndex);
        log.trace("[{}] Updated nextIndex for {} to {}", nodeId, followerId, newNextIndex);
    }

    /**
     * Updates matchIndex for a follower after successful replication.
     * Also advances nextIndex to matchIndex + 1.
     *
     * @param followerId the follower ID
     * @param newMatchIndex the new matchIndex value
     */
    public void setMatchIndex(String followerId, long newMatchIndex) {
        matchIndex.put(followerId, newMatchIndex);
        nextIndex.put(followerId, newMatchIndex + 1);
        log.trace("[{}] Updated matchIndex for {} to {}", nodeId, followerId, newMatchIndex);
    }

    /**
     * Computes the highest log index replicated on a majority of servers.
     * This is used to advance commitIndex.
     *
     * <p>The leader's own log is implicitly at the highest index, so we include
     * the leaderLogIndex in the calculation.
     *
     * @param clusterSize the total number of nodes in the cluster
     * @param leaderLogIndex the index of the last entry in the leader's log
     * @return the highest index replicated on majority, or 0 if none
     */
    public long computeMajorityMatchIndex(int clusterSize, long leaderLogIndex) {
        // Include leader's own log index (leader always has its own entries)
        java.util.List<Long> allIndices = new java.util.ArrayList<>();
        allIndices.add(leaderLogIndex);
        allIndices.addAll(matchIndex.values());

        // Sort descending to find the N/2+1 highest index
        allIndices.sort((a, b) -> Long.compare(b, a));

        // Majority requires (clusterSize / 2) + 1 nodes
        // In a sorted descending list, the index at position (clusterSize / 2)
        // is the highest index that at least majority nodes have
        int majorityPosition = clusterSize / 2;
        // For cluster of 3: position 1; for 5: position 2

        if (majorityPosition < allIndices.size()) {
            return allIndices.get(majorityPosition);
        }
        return 0;
    }
}
