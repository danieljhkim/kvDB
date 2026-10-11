package com.danieljhkim.kvdb.kvnode.storage;

/**
 * Bounds the request identities a shard keeps after they stop describing current state.
 *
 * <p>The newest committed mutation of every key (including delete tombstones), every pending prepare, and every aborted
 * version owner above the committed version are always kept. The remaining history identities, superseded committed
 * mutations and aborted mutations, form a deduplication window. An identity is evicted only while the window exceeds
 * {@code maxEntries} or {@code maxBytes} and only after it has been kept for {@code minRetentionMs}, measured from the
 * mutation's {@code update_time_ms}. When a new mutation cannot fit without evicting a younger identity, it is refused
 * before it is staged.
 *
 * <p>The replication journal is rewritten to the retained state whenever its record count exceeds both
 * {@code journalCompactionMinRecords} and twice the bound on a rewritten journal: one checkpoint plus at most two
 * records per retained identity.
 */
public record WriteHistoryPolicy(long minRetentionMs, int maxEntries, long maxBytes, int journalCompactionMinRecords) {

    public static final long DEFAULT_MIN_RETENTION_MS = 30_000;
    public static final int DEFAULT_MAX_ENTRIES = 200_000;
    public static final long DEFAULT_MAX_BYTES = 128L * 1024 * 1024;
    public static final int DEFAULT_JOURNAL_COMPACTION_MIN_RECORDS = 1024;

    public static final WriteHistoryPolicy DEFAULT = new WriteHistoryPolicy(
            DEFAULT_MIN_RETENTION_MS, DEFAULT_MAX_ENTRIES, DEFAULT_MAX_BYTES, DEFAULT_JOURNAL_COMPACTION_MIN_RECORDS);

    public WriteHistoryPolicy {
        if (minRetentionMs < 0) {
            throw new IllegalArgumentException("write history minRetentionMs must be non-negative");
        }
        if (maxEntries <= 0 || maxBytes <= 0) {
            throw new IllegalArgumentException("write history maxEntries and maxBytes must be positive");
        }
        if (journalCompactionMinRecords <= 0) {
            throw new IllegalArgumentException("journalCompactionMinRecords must be positive");
        }
    }

    public WriteHistoryPolicy(long minRetentionMs, int maxEntries, long maxBytes) {
        this(minRetentionMs, maxEntries, maxBytes, DEFAULT_JOURNAL_COMPACTION_MIN_RECORDS);
    }
}
