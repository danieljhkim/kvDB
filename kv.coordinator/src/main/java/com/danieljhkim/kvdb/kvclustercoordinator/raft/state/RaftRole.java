package com.danieljhkim.kvdb.kvclustercoordinator.raft.state;

/**
 * Represents the three possible roles a Raft node can have.
 */
public enum RaftRole {
    /**
     * Follower state: passively receives AppendEntries and votes in elections.
     */
    FOLLOWER,

    /**
     * Candidate state: actively seeking votes to become leader.
     */
    CANDIDATE,

    /**
     * Leader state: handles client requests and replicates log to followers.
     */
    LEADER
}
