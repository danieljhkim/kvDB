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
import java.util.Arrays;
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
    void repairsEveryPartialTailBeforeRecoverAppendAndReopen() throws IOException {
        Path wal = tempDir.resolve("tail-fixture.wal");
        WALManager manager = new WALManager(wal.toString());
        manager.log("SET", "first", "one");
        manager.log("SET", "second", "two");
        long prefixSize = Files.size(wal);
        manager.log("SET", "torn", "discarded");
        manager.close();

        byte[] complete = Files.readAllBytes(wal);
        // Every partial header, payload and checksum, plus a complete tail with an invalid checksum.
        for (int tailBytes = 1; tailBytes <= complete.length - prefixSize; tailBytes++) {
            Path damagedWal = tempDir.resolve("tail-" + tailBytes + ".wal");
            byte[] damaged = Arrays.copyOf(complete, (int) prefixSize + tailBytes);
            if (damaged.length == complete.length) {
                damaged[damaged.length - 1] ^= 1;
            }
            Files.write(damagedWal, damaged);

            WALManager recovered = new WALManager(damagedWal.toString());
            List<String[]> prefix = recovered.replay();
            assertEquals(2, prefix.size());
            assertArrayEquals(new String[] {"SET", "first", "one"}, prefix.get(0));
            assertArrayEquals(new String[] {"SET", "second", "two"}, prefix.get(1));
            assertArrayEquals(Arrays.copyOf(complete, (int) prefixSize), Files.readAllBytes(damagedWal));
            recovered.log("SET", "acknowledged", "survives");
            recovered.close();

            WALManager reopened = new WALManager(damagedWal.toString());
            List<String[]> replayed = reopened.replay();
            assertEquals(3, replayed.size());
            assertArrayEquals(prefix.get(0), replayed.get(0));
            assertArrayEquals(prefix.get(1), replayed.get(1));
            assertArrayEquals(new String[] {"SET", "acknowledged", "survives"}, replayed.get(2));
            reopened.close();
        }
    }

    @Test
    void appendWithoutExplicitRecoveryRepairsATornFirstRecord() throws IOException {
        Path wal = tempDir.resolve("torn-first.wal");
        WALManager manager = new WALManager(wal.toString());
        manager.log("SET", "torn", "discarded");
        manager.close();
        try (var channel = FileChannel.open(wal, StandardOpenOption.WRITE)) {
            channel.truncate(3);
        }

        WALManager recovered = new WALManager(wal.toString());
        recovered.log("SET", "acknowledged", "survives");
        recovered.close();
        WALManager reopened = new WALManager(wal.toString());
        List<String[]> replayed = reopened.replay();
        assertEquals(1, replayed.size());
        assertArrayEquals(new String[] {"SET", "acknowledged", "survives"}, replayed.getFirst());
        reopened.close();
    }

    @Test
    void repairSyncFailurePreventsRecoveryAndPoisonsFurtherWrites() throws IOException {
        Path wal = tempDir.resolve("repair-sync.wal");
        WALManager failedRecovery = new WALManager(wal.toString(), point -> {
            if (point == WALManager.FaultPoint.BEFORE_REPAIR_SYNC) {
                throw new IOException("repair fsync failed");
            }
        });
        failedRecovery.log("SET", "first", "one");
        failedRecovery.log("SET", "torn", "discarded");
        // Leave the append channel open to exercise its closure during repair.
        try (var channel = FileChannel.open(wal, StandardOpenOption.WRITE)) {
            channel.truncate(Files.size(wal) - 2);
        }
        assertThrows(UncheckedIOException.class, failedRecovery::replayRecords);
        byte[] afterFailedSync = Files.readAllBytes(wal);
        assertThrows(UncheckedIOException.class, () -> failedRecovery.log("SET", "later", "unacknowledged"));
        assertArrayEquals(afterFailedSync, Files.readAllBytes(wal));
        failedRecovery.close();
    }

    @Test
    void failsClosedOnNonTailCorruption() throws IOException {
        Path wal = tempDir.resolve("corrupt.wal");
        WALManager manager = new WALManager(wal.toString());
        manager.log("SET", "first", "one");
        manager.log("SET", "second", "two");
        manager.log("SET", "third", "three");
        manager.close();

        byte[] complete = Files.readAllBytes(wal);
        int middle = findRecordOffsets(complete).get(1);
        for (String form : List.of("checksum", "truncated", "swallowed-record", "magic", "version", "length")) {
            Path corruptWal = tempDir.resolve("corrupt-" + form + ".wal");
            byte[] damaged = complete.clone();
            switch (form) {
                case "checksum" -> damaged[middle + 9 + 12] ^= 1;
                case "truncated" -> ByteBuffer.wrap(damaged).putInt(middle + 5, complete.length);
                case "swallowed-record" ->
                    ByteBuffer.wrap(damaged).putInt(middle + 5, complete.length - middle - 9 - 4);
                case "magic" -> damaged[middle] ^= 1;
                case "version" -> damaged[middle + 4] ^= 1;
                case "length" -> ByteBuffer.wrap(damaged).putInt(middle + 5, -1);
                default -> throw new AssertionError(form);
            }
            Files.write(corruptWal, damaged);

            WALManager recovered = new WALManager(corruptWal.toString());
            assertThrows(WALManager.WALCorruptionException.class, recovered::replay);
            assertArrayEquals(damaged, Files.readAllBytes(corruptWal));
            assertThrows(WALManager.WALCorruptionException.class, () -> recovered.log("SET", "later", "rejected"));
            assertArrayEquals(damaged, Files.readAllBytes(corruptWal));
            recovered.close();
        }
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
