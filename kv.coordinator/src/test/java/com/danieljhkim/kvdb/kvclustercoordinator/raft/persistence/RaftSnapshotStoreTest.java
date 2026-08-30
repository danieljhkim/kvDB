package com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RaftSnapshotStoreTest {

    @TempDir
    Path tempDir;

    @Test
    void interruptedInstallationResumesAtDurableOffsetAndInstallsAtomically() throws Exception {
        byte[] data = "complete state-machine snapshot".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        int checksum = RaftSnapshotStore.checksum(data);
        RaftSnapshotStore first = new RaftSnapshotStore(tempDir);
        var partial =
                first.installChunk(25, 4, 0, java.util.Arrays.copyOfRange(data, 0, 8), false, data.length, checksum);
        assertEquals(8, partial.nextOffset());
        assertFalse(first.load().isPresent());

        RaftSnapshotStore restarted = new RaftSnapshotStore(tempDir);
        var wrongOffset = restarted.installChunk(
                25, 4, 3, java.util.Arrays.copyOfRange(data, 8, data.length), true, data.length, checksum);
        assertFalse(wrongOffset.accepted());
        assertEquals(8, wrongOffset.nextOffset());

        var complete = restarted.installChunk(
                25, 4, 8, java.util.Arrays.copyOfRange(data, 8, data.length), true, data.length, checksum);
        assertTrue(complete.complete());
        assertArrayEquals(data, restarted.load().orElseThrow().data());
    }

    @Test
    void crashDuringTemporaryInstallHeaderRestartsTransferFromZero() throws Exception {
        Files.write(tempDir.resolve("raft_snapshot.installing"), new byte[] {1, 2, 3});
        RaftSnapshotStore restarted = new RaftSnapshotStore(tempDir);
        byte[] data = new byte[] {4, 5};
        var complete = restarted.installChunk(3, 2, 0, data, true, data.length, RaftSnapshotStore.checksum(data));
        assertTrue(complete.complete());
        assertArrayEquals(data, restarted.load().orElseThrow().data());
    }

    @Test
    void checksumInvalidAndTruncatedSnapshotsFailClosed() throws Exception {
        RaftSnapshotStore store = new RaftSnapshotStore(tempDir);
        store.save(2, 1, new byte[] {1, 2, 3, 4});
        Path snapshot = tempDir.resolve("raft_snapshot.bin");
        byte[] corrupt = Files.readAllBytes(snapshot);
        corrupt[corrupt.length - 1] ^= 1;
        Files.write(snapshot, corrupt);
        assertTrue(assertThrows(IOException.class, () -> new RaftSnapshotStore(tempDir))
                .getMessage()
                .contains("checksum"));

        Files.write(snapshot, java.util.Arrays.copyOf(corrupt, 3));
        assertTrue(assertThrows(IOException.class, () -> new RaftSnapshotStore(tempDir))
                .getMessage()
                .contains("truncated"));
    }

    @Test
    void failedDurabilityBoundaryDoesNotReplacePriorSnapshot() throws Exception {
        RaftSnapshotStore original = new RaftSnapshotStore(tempDir);
        original.save(1, 1, new byte[] {1});
        DurableFileOps failing = new DurableFileOps() {
            @Override
            public void forceFile(Path path) throws IOException {
                throw new IOException("injected fsync failure");
            }
        };
        RaftSnapshotStore store = new RaftSnapshotStore(tempDir, failing);
        assertThrows(IOException.class, () -> store.save(2, 2, new byte[] {2}));
        assertEquals(1, original.load().orElseThrow().lastIncludedIndex());
    }
}
