package com.danieljhkim.kvdb.kvclustercoordinator.raft;

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

    /**
     * Truncate log after the given index (exclusive)
     */
    void truncateAfter(long index) throws IOException;
}
