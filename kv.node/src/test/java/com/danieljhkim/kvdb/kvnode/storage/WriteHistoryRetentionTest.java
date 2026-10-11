package com.danieljhkim.kvdb.kvnode.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.danieljhkim.kvdb.kvcommon.exception.NodeUnavailableException;
import com.danieljhkim.kvdb.kvcommon.exception.RequestIdConflictException;
import com.danieljhkim.kvdb.kvcommon.persistence.WALManager;
import com.danieljhkim.kvdb.kvnode.persistence.FilePersistenceManager;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.protobuf.ByteString;
import com.kvdb.proto.kvstore.MutationKind;
import com.kvdb.proto.kvstore.ReplicatedMutation;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Deterministic coverage of the bounded write-history window and replication journal compaction. */
class WriteHistoryRetentionTest {

    private static final TypeReference<Map<String, String>> MAP_TYPE = new TypeReference<>() {};
    private static final long RETENTION_MS = 1_000;

    @TempDir
    Path tempDir;

    private final AtomicLong now = new AtomicLong(1_000_000);

    @Test
    void boundedKeyspaceOverwritesStayWithinTheHistoryAndJournalBound() throws IOException {
        WriteHistoryPolicy policy = new WriteHistoryPolicy(RETENTION_MS, 4, 1024 * 1024, 8);
        ShardKVStore store = newStore("bounded", policy);
        Path journal = journalPath("bounded");
        List<String> keys = List.of("a", "b");
        int writes = 120;
        // Two current keys, at most four history identities, and the 2x compaction trigger bound the journal.
        int maxIdentities = keys.size() + policy.maxEntries();
        int maxJournalRecords = 2 * (1 + maxIdentities + policy.maxEntries());

        for (int i = 1; i <= writes; i++) {
            now.addAndGet(2 * RETENTION_MS);
            commit(store, "write-" + i, MutationKind.SET, keys.get(i % keys.size()), "value-" + i);
            assertTrue(store.retainedIdentityCount() <= maxIdentities, "identities after write " + i);
            assertTrue(journalRecords(journal) <= maxJournalRecords, "journal records after write " + i);
        }
        // Every record is under 200 bytes, so the journal stays far below the ~40 KiB an uncompacted one reaches.
        assertTrue(Files.size(journal) < 200L * maxJournalRecords, "journal bytes " + Files.size(journal));
        assertEquals(writes, store.committedVersion());
        store.shutdown();

        ShardKVStore reopened = newStore("bounded", policy);
        assertEquals("value-120", reopened.get("a"));
        assertEquals("value-119", reopened.get("b"));
        assertEquals(writes, reopened.committedVersion());
        assertTrue(reopened.retainedIdentityCount() <= maxIdentities);
        // A retained superseded identity still replays identically and still detects a conflicting reuse.
        ReplicatedMutation replay = prepare(reopened, "write-117", MutationKind.SET, "b", "value-117");
        assertEquals(117, replay.getVersion());
        assertThrows(
                RequestIdConflictException.class,
                () -> prepare(reopened, "write-117", MutationKind.SET, "b", "different"));
        // An identity past both the retention window and the entry budget is the documented expiry: it is new again.
        ReplicatedMutation expired = prepare(reopened, "write-1", MutationKind.SET, "b", "value-1");
        assertEquals(writes + 1, expired.getVersion());
        reopened.shutdown();
    }

    @Test
    void fullHistoryWindowRejectsNewWritesBeforeStagingAcrossRestart() throws IOException {
        WriteHistoryPolicy policy = new WriteHistoryPolicy(RETENTION_MS, 2, 1024 * 1024, 1024);
        ShardKVStore store = newStore("admission", policy);
        commit(store, "w1", MutationKind.SET, "k", "one");
        commit(store, "w2", MutationKind.SET, "k", "two");
        commit(store, "w3", MutationKind.SET, "k", "three");
        Path journal = journalPath("admission");
        byte[] journalBefore = Files.readAllBytes(journal);

        NodeUnavailableException rejected =
                assertThrows(NodeUnavailableException.class, () -> prepare(store, "w4", MutationKind.SET, "k", "four"));
        assertTrue(rejected.isRejectedBeforeMutation());
        assertArrayEquals(journalBefore, Files.readAllBytes(journal));
        assertEquals("three", store.get("k"));
        assertEquals(3, store.committedVersion());
        assertFalse(store.prepareMutation(replicaMutation("replica-w4", 4, "k", "four"))
                .success());

        // Retries inside the window are never refused, and conflicting reuse is still detected.
        assertEquals(2, prepare(store, "w2", MutationKind.SET, "k", "two").getVersion());
        assertThrows(RequestIdConflictException.class, () -> prepare(store, "w1", MutationKind.SET, "k", "other"));
        store.shutdown();

        ShardKVStore restarted = newStore("admission", policy);
        assertThrows(NodeUnavailableException.class, () -> prepare(restarted, "w4", MutationKind.SET, "k", "four"));
        assertEquals(1, prepare(restarted, "w1", MutationKind.SET, "k", "one").getVersion());

        now.addAndGet(RETENTION_MS);
        ReplicatedMutation admitted = prepare(restarted, "w4", MutationKind.SET, "k", "four");
        assertEquals(4, admitted.getVersion());
        assertTrue(restarted.commitMutation(admitted).success());
        assertEquals("four", restarted.get("k"));
        restarted.shutdown();
    }

    @Test
    void compactionPreservesPendingAbortedTombstoneAndReplayStateAcrossRestart() throws IOException {
        WriteHistoryPolicy policy = new WriteHistoryPolicy(RETENTION_MS, 16, 1024 * 1024, 1024);
        ShardKVStore store = newStore("compaction", policy);
        commit(store, "set-a", MutationKind.SET, "a", "alpha");
        commit(store, "del-a", MutationKind.DELETE, "a", "");
        commit(store, "set-b-1", MutationKind.SET, "b", "first");
        commit(store, "set-b-2", MutationKind.SET, "b", "second");
        ReplicatedMutation pending = prepare(store, "pending-c", MutationKind.SET, "c", "hidden");
        ReplicatedMutation aborted = replicaMutation("aborted-d", 6, "d", "fenced");
        assertTrue(store.prepareMutation(aborted).success());
        assertTrue(store.abortMutation(aborted).success());
        Path journal = journalPath("compaction");
        long uncompacted = Files.size(journal);

        store.persistNow();
        assertTrue(Files.size(journal) < uncompacted, "compaction must shrink the journal");
        store.shutdown();

        ShardKVStore restarted = newStore("compaction", policy);
        assertEquals(4, restarted.committedVersion());
        assertEquals("(nil)", restarted.get("a"));
        assertEquals("second", restarted.get("b"));
        assertEquals("(nil)", restarted.get("c"));
        // Anti-entropy still serves the delete tombstone and the superseded identities.
        ReplicatedMutation tombstone = restarted.committedMutations().stream()
                .filter(mutation -> mutation.getKey().equals(ByteString.copyFromUtf8("a")))
                .findFirst()
                .orElseThrow();
        assertEquals(MutationKind.DELETE, tombstone.getKind());
        assertEquals(
                List.of("set-a", "del-a", "set-b-1", "set-b-2"),
                restarted.committedHistoryAfter(0, 16).stream()
                        .map(entry -> entry.mutation().getRequestId())
                        .toList());
        // Identical replay and conflict detection survive for superseded identities.
        assertEquals(
                3, prepare(restarted, "set-b-1", MutationKind.SET, "b", "first").getVersion());
        assertThrows(
                RequestIdConflictException.class,
                () -> prepare(restarted, "set-b-1", MutationKind.SET, "b", "changed"));
        // The aborted owner above the committed version still fences a same-epoch replacement.
        assertFalse(restarted
                .prepareMutation(replicaMutation("intruder-d", 6, "d", "x"))
                .success());
        // The pending prepare is still pending and commits after restart.
        assertTrue(restarted.commitMutation(pending).success());
        assertEquals("hidden", restarted.get("c"));
        assertEquals(5, restarted.committedVersion());
        restarted.shutdown();

        ShardKVStore reopened = newStore("compaction", policy);
        assertEquals("hidden", reopened.get("c"));
        assertEquals("already committed", reopened.commitMutation(pending).message());
        reopened.shutdown();
    }

    @Test
    void interruptedCompactionKeepsTheCompleteJournalForRestart() throws IOException {
        WriteHistoryPolicy policy = new WriteHistoryPolicy(RETENTION_MS, 1, 1024 * 1024, 4);
        String name = "interrupted";
        AtomicBoolean failRewrite = new AtomicBoolean(true);
        WALManager replicationWal = new WALManager(journalPath(name).toString()) {
            @Override
            public synchronized void rewrite(List<WalRecord> records) {
                if (failRewrite.get()) {
                    throw new UncheckedIOException("crash before rename", new IOException("interrupted"));
                }
                super.rewrite(records);
            }
        };
        WALManager wal = new WALManager(tempDir.resolve(name + ".wal").toString());
        ShardKVStore store = new ShardKVStore(
                "shard",
                100,
                false,
                new FilePersistenceManager<>(tempDir.resolve(name + ".json").toString(), MAP_TYPE),
                wal,
                replicationWal,
                policy,
                now::get);

        // Automatic compaction is attempted once the journal outgrows the retained state. Its failures leave every
        // acknowledged record in place and do not fail the write.
        for (int i = 1; i <= 6; i++) {
            now.addAndGet(2 * RETENTION_MS);
            commit(store, "write-" + i, MutationKind.SET, "k", "value-" + i);
        }
        assertEquals(12, journalRecords(journalPath(name)));
        assertThrows(UncheckedIOException.class, store::persistNow);
        wal.close();
        replicationWal.close();

        ShardKVStore restarted = newStore(name, policy);
        assertEquals("value-6", restarted.get("k"));
        assertEquals(6, restarted.committedVersion());
        assertEquals(
                5,
                prepare(restarted, "write-5", MutationKind.SET, "k", "value-5").getVersion());
        restarted.shutdown();
    }

    @Test
    void standaloneOverwritesDoNotRetainSupersededIdentities() {
        ShardKVStore store = new ShardKVStore(
                "shard",
                100,
                false,
                new FilePersistenceManager<>(tempDir.resolve("standalone.json").toString(), MAP_TYPE),
                new WALManager(tempDir.resolve("standalone.wal").toString()));
        for (int i = 0; i < 20; i++) {
            store.set("k", "value-" + i);
        }
        store.del("gone");

        assertEquals(2, store.retainedIdentityCount());
        assertEquals("value-19", store.get("k"));
        store.shutdown();
    }

    private ShardKVStore newStore(String name, WriteHistoryPolicy policy) {
        return new ShardKVStore(
                "shard",
                100,
                false,
                new FilePersistenceManager<>(tempDir.resolve(name + ".json").toString(), MAP_TYPE),
                new WALManager(tempDir.resolve(name + ".wal").toString()),
                new WALManager(journalPath(name).toString()),
                policy,
                now::get);
    }

    private Path journalPath(String name) {
        return tempDir.resolve(name + ".wal.replication");
    }

    private static int journalRecords(Path journal) {
        WALManager reader = new WALManager(journal.toString());
        try {
            return reader.replayRecords().size();
        } finally {
            reader.close();
        }
    }

    private ReplicatedMutation prepare(
            ShardKVStore store, String requestId, MutationKind kind, String key, String value) {
        return store.prepareNewMutation(
                requestId,
                1,
                kind,
                ByteString.copyFromUtf8(key),
                ByteString.copyFromUtf8(value),
                "leader",
                0,
                OptionalLong.empty(),
                false,
                now.get());
    }

    private void commit(ShardKVStore store, String requestId, MutationKind kind, String key, String value) {
        ReplicatedMutation mutation = prepare(store, requestId, kind, key, value);
        assertTrue(store.commitMutation(mutation).success());
    }

    private ReplicatedMutation replicaMutation(String requestId, long version, String key, String value) {
        return ReplicatedMutation.newBuilder()
                .setRequestId(requestId)
                .setShardId("shard")
                .setEpoch(1)
                .setVersion(version)
                .setKind(MutationKind.SET)
                .setKey(ByteString.copyFromUtf8(key))
                .setValue(ByteString.copyFromUtf8(value))
                .setOriginNodeId("leader")
                .setUpdateTimeMs(now.get())
                .build();
    }
}
