package com.danieljhkim.kvdb.kvcommon.persistence;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WALManagerTest {

    private static final int MAGIC = 0x4b565741;

    @TempDir
    Path tempDir;

    @Test
    void roundTripsLengthDelimitedTextAndArbitraryBinaryValues() {
        Path wal = tempDir.resolve("data.wal");
        WALManager manager = new WALManager(wal.toString());

        manager.log("SET", "key with\nnewline\0", "value with spaces\nand unicode \u2603");
        byte[] binaryKey = new byte[] {0, -1, 10, 13, 42};
        byte[] binaryValue = new byte[] {-128, 0, 127, 1};
        manager.log("SET", binaryKey, binaryValue);

        List<WALManager.WalRecord> records = manager.replayRecords();
        assertEquals(2, records.size());
        assertArrayEquals(binaryKey, records.get(1).key());
        assertArrayEquals(binaryValue, records.get(1).value());
        assertEquals(WALManager.Durability.FSYNC, manager.durability());
    }

    @Test
    void appendAndSyncFailuresPropagateAndPoisonFurtherWrites() {
        for (WALManager.FaultPoint point :
                List.of(WALManager.FaultPoint.BEFORE_APPEND, WALManager.FaultPoint.BEFORE_SYNC)) {
            Path wal = tempDir.resolve(point.name() + ".wal");
            WALManager manager = new WALManager(wal.toString(), current -> {
                if (current == point) {
                    throw new IOException(
                            point == WALManager.FaultPoint.BEFORE_APPEND ? "No space left on device" : "fsync failed");
                }
            });

            assertThrows(UncheckedIOException.class, () -> manager.log("SET", "key", "value"));
            assertThrows(UncheckedIOException.class, () -> manager.log("SET", "later", "value"));
        }
    }

    @Test
    void ignoresOnlyATornFinalRecord() throws IOException {
        Path wal = tempDir.resolve("torn.wal");
        WALManager manager = new WALManager(wal.toString());
        manager.log("SET", "first", "one");
        manager.log("SET", "second", "two");
        manager.close();

        try (var channel = FileChannel.open(wal, StandardOpenOption.WRITE)) {
            channel.truncate(Files.size(wal) - 2);
        }

        List<String[]> recovered = new WALManager(wal.toString()).replay();
        assertEquals(1, recovered.size());
        assertEquals("first", recovered.getFirst()[1]);
    }

    @Test
    void failsClosedOnNonTailCorruption() throws IOException {
        Path wal = tempDir.resolve("corrupt.wal");
        WALManager manager = new WALManager(wal.toString());
        manager.log("SET", "first", "one");
        manager.log("SET", "second", "two");
        manager.log("SET", "third", "three");
        manager.close();

        byte[] bytes = Files.readAllBytes(wal);
        List<Integer> records = findRecordOffsets(bytes);
        bytes[records.get(1) + 9 + 12] ^= 1;
        Files.write(wal, bytes);

        assertThrows(WALManager.WALCorruptionException.class, () -> new WALManager(wal.toString()).replay());
    }

    @Test
    void interruptedRotationLeavesTheRequiredWalIntact() {
        Path wal = tempDir.resolve("rotate.wal");
        WALManager manager = new WALManager(wal.toString(), point -> {
            if (point == WALManager.FaultPoint.BEFORE_ROTATE_MOVE) {
                throw new IOException("rename interrupted");
            }
        });
        manager.log("SET", "key", "value");

        assertThrows(UncheckedIOException.class, manager::clear);
        assertEquals("key", new WALManager(wal.toString()).replay().getFirst()[1]);
    }

    @Test
    void rotationDirectorySyncFailurePropagates() {
        Path wal = tempDir.resolve("rotate-sync.wal");
        WALManager manager = new WALManager(wal.toString(), point -> {
            if (point == WALManager.FaultPoint.BEFORE_ROTATE_DIRECTORY_SYNC) {
                throw new IOException("directory fsync failed");
            }
        });
        manager.log("SET", "key", "value");

        assertThrows(UncheckedIOException.class, manager::clear);
    }

    private static List<Integer> findRecordOffsets(byte[] contents) {
        List<Integer> offsets = new ArrayList<>();
        for (int i = 0; i <= contents.length - Integer.BYTES; i++) {
            if (ByteBuffer.wrap(contents, i, Integer.BYTES).getInt() == MAGIC) {
                offsets.add(i);
            }
        }
        return offsets;
    }
}
