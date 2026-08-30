package com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public interface RaftLog extends AutoCloseable {

    /**
     * Append a new entry to the log
     */
    void append(RaftLogEntry entry) throws IOException;

    /**
     * Get all entries starting from the given index (inclusive)
     */
    List<RaftLogEntry> getEntriesSince(long fromIndex) throws IOException;

    /**
     * Get a specific log entry by index
     */
    Optional<RaftLogEntry> getEntry(long index) throws IOException;

    /**
     * Get the last log entry
     */
    Optional<RaftLogEntry> getLastEntry() throws IOException;

    /**
     * Get the current size of the log
     */
    long size();

    /** First index that may still be present after prefix compaction. */
    default long firstIndex() {
        return 1;
    }

    /** Highest authoritative log index, including a compacted snapshot boundary. */
    default long lastIndex() {
        return size();
    }

    /** Highest index covered by the durable snapshot that compacted this log. */
    default long compactedIndex() {
        return 0;
    }

    /** Term at {@link #compactedIndex()}. */
    default long compactedTerm() {
        return 0;
    }

    /** Returns the term at an entry or at the compacted snapshot boundary. */
    default Optional<Long> getTerm(long index) throws IOException {
        if (index == compactedIndex()) {
            return Optional.of(compactedTerm());
        }
        return getEntry(index).map(RaftLogEntry::term);
    }

    /**
     * Truncate log after the given index (exclusive)
     */
    void truncateAfter(long index) throws IOException;

    /**
     * Removes entries covered by an already durable snapshot.
     *
     * <p>Implementations that cannot compact must fail explicitly.
     */
    default void compactThrough(long index, long term) throws IOException {
        throw new UnsupportedOperationException("Log compaction is not supported");
    }
}
