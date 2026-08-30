package com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftCommand;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RaftPersistenceTest {

    @TempDir
    Path tempDir;

    @Test
    void compactionSurvivesRestartWithAbsoluteIndexes() throws Exception {
        Path path = tempDir.resolve("raft.log");
        try (FileBasedRaftLog log = new FileBasedRaftLog(path)) {
            log.append(entry(1, 1));
            log.append(entry(2, 1));
            log.append(entry(3, 2));
            log.compactThrough(2, 1);
            assertEquals(2, log.compactedIndex());
            assertEquals(3, log.lastIndex());
            assertEquals(1, log.size());
        }

        try (FileBasedRaftLog restarted = new FileBasedRaftLog(path)) {
            assertEquals(2, restarted.compactedIndex());
            assertEquals(1, restarted.compactedTerm());
            assertTrue(restarted.getEntry(2).isEmpty());
            assertEquals(2, restarted.getEntry(3).orElseThrow().term());
            restarted.append(entry(4, 2));
            assertEquals(4, restarted.lastIndex());
        }
    }

    @Test
    void readsLegacyLogThenUpgradesOnMutation() throws Exception {
        Path path = tempDir.resolve("legacy.log");
        try (DataOutputStream output = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(path)))) {
            byte[] first = entry(1, 1).toBytes();
            output.writeInt(first.length);
            output.write(first);
        }
        try (FileBasedRaftLog log = new FileBasedRaftLog(path)) {
            assertEquals(1, log.lastIndex());
            log.append(entry(2, 2));
        }
        assertEquals(
                FileBasedRaftLog.MAGIC,
                ByteBuffer.wrap(Files.readAllBytes(path)).getInt());
    }

    @Test
    void truncatedOversizedAndChecksumInvalidLogsFailDeterministically() throws Exception {
        Path truncated = tempDir.resolve("truncated.log");
        try (FileBasedRaftLog ignored = new FileBasedRaftLog(truncated)) {}
        byte[] bytes = Files.readAllBytes(truncated);
        Files.write(truncated, java.util.Arrays.copyOf(bytes, bytes.length - 1));
        assertTrue(assertThrows(IOException.class, () -> new FileBasedRaftLog(truncated))
                .getMessage()
                .contains("truncated"));

        Path oversized = tempDir.resolve("oversized.log");
        Files.write(
                oversized,
                ByteBuffer.allocate(4)
                        .putInt(FileBasedRaftLog.MAX_ENTRY_BYTES + 1)
                        .array());
        assertTrue(assertThrows(IOException.class, () -> new FileBasedRaftLog(oversized))
                .getMessage()
                .contains("outside"));

        Path checksum = tempDir.resolve("checksum.log");
        try (FileBasedRaftLog log = new FileBasedRaftLog(checksum)) {
            log.append(entry(1, 1));
        }
        byte[] corrupt = Files.readAllBytes(checksum);
        corrupt[corrupt.length - 1] ^= 1;
        Files.write(checksum, corrupt);
        assertTrue(assertThrows(IOException.class, () -> new FileBasedRaftLog(checksum))
                .getMessage()
                .contains("checksum"));
    }

    @Test
    void stateStoreReadsLegacyAndFailsClosedOnCorruption() throws Exception {
        Path stateDir = tempDir.resolve("state");
        Files.createDirectories(stateDir);
        Properties properties = new Properties();
        properties.setProperty("currentTerm", "7");
        properties.setProperty("votedFor", "node-2");
        try (var output = Files.newOutputStream(stateDir.resolve("raft_state.properties"))) {
            properties.store(output, "legacy");
        }
        RaftPersistentStateStore store = new RaftPersistentStateStore(stateDir.toString());
        assertEquals(7, store.load().getCurrentTerm());
        store.save(8, "node-3");
        assertEquals(
                RaftPersistentStateStore.MAGIC,
                ByteBuffer.wrap(Files.readAllBytes(stateDir.resolve("raft_state.properties")))
                        .getInt());

        byte[] corrupt = Files.readAllBytes(stateDir.resolve("raft_state.properties"));
        corrupt[corrupt.length - 1] ^= 1;
        Files.write(stateDir.resolve("raft_state.properties"), corrupt);
        assertTrue(assertThrows(IOException.class, store::load).getMessage().contains("checksum"));

        Files.write(
                stateDir.resolve("raft_state.properties"),
                ByteBuffer.allocate(12)
                        .putInt(RaftPersistentStateStore.MAGIC)
                        .putInt(RaftPersistentStateStore.FORMAT_VERSION)
                        .putInt(RaftPersistentStateStore.MAX_PAYLOAD_BYTES + 1)
                        .array());
        assertTrue(assertThrows(IOException.class, store::load).getMessage().contains("outside"));

        Files.write(stateDir.resolve("raft_state.properties"), new byte[] {1});
        assertTrue(assertThrows(IOException.class, store::load).getMessage().contains("truncated"));
    }

    @Test
    void fileAndDirectoryFsyncFailuresAreReported() throws Exception {
        Path stateDir = tempDir.resolve("fault-state");
        RaftPersistentStateStore initial = new RaftPersistentStateStore(stateDir.toString());
        initial.save(1, null);

        AtomicBoolean fileForceReached = new AtomicBoolean();
        DurableFileOps fileFailure = new DurableFileOps() {
            @Override
            public void forceFile(Path path) throws IOException {
                fileForceReached.set(true);
                throw new IOException("injected file fsync failure");
            }
        };
        RaftPersistentStateStore failingFileStore = new RaftPersistentStateStore(stateDir, fileFailure);
        assertThrows(IOException.class, () -> failingFileStore.save(2, null));
        assertTrue(fileForceReached.get());
        assertEquals(1, initial.load().getCurrentTerm());

        AtomicBoolean directoryForceReached = new AtomicBoolean();
        DurableFileOps directoryFailure = new DurableFileOps() {
            @Override
            public void forceDirectory(Path path) throws IOException {
                directoryForceReached.set(true);
                throw new IOException("injected directory fsync failure");
            }
        };
        RaftPersistentStateStore failingDirectoryStore = new RaftPersistentStateStore(stateDir, directoryFailure);
        assertThrows(IOException.class, () -> failingDirectoryStore.save(3, null));
        assertTrue(directoryForceReached.get());
    }

    private static RaftLogEntry entry(long index, long term) {
        return new RaftLogEntry(
                index,
                term,
                index,
                new RaftCommand.RegisterNode("node-" + index, "127.0.0.1:" + (9000 + index), "zone-a"));
    }
}
