package com.danieljhkim.kvdb.kvcommon.persistence;

import com.danieljhkim.kvdb.kvcommon.observability.Metrics;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32C;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Synchronous write-ahead log.
 *
 * <p>An append returns only after the complete record has been forced to stable storage. Records are versioned,
 * length-delimited, and protected by CRC32C. A torn final record is ignored during recovery, while corruption before
 * the tail fails recovery closed.
 */
public class WALManager {

    public enum Durability {
        FSYNC
    }

    enum FaultPoint {
        BEFORE_APPEND,
        BEFORE_SYNC,
        BEFORE_ROTATE_MOVE,
        BEFORE_ROTATE_DIRECTORY_SYNC
    }

    @FunctionalInterface
    interface FaultInjector {
        FaultInjector NONE = point -> {};

        void trigger(FaultPoint point) throws IOException;
    }

    public static final class WALCorruptionException extends IllegalStateException {
        public WALCorruptionException(String message) {
            super(message);
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(WALManager.class);
    private static final int RECORD_MAGIC = 0x4b565741; // KVWA
    private static final byte RECORD_VERSION = 1;
    private static final int HEADER_SIZE = Integer.BYTES + Byte.BYTES + Integer.BYTES;
    private static final int LENGTH_FIELDS_SIZE = 3 * Integer.BYTES;
    private static final int CHECKSUM_SIZE = Integer.BYTES;
    private static final int MAX_PAYLOAD_SIZE = 64 * 1024 * 1024;

    private volatile boolean loggingEnabled = true;
    private Path walFile;
    private FileChannel channel;
    private IOException appendFailure;
    private final FaultInjector faultInjector;

    public WALManager(String fileName) {
        this(fileName, FaultInjector.NONE);
    }

    WALManager(String fileName, FaultInjector faultInjector) {
        this.faultInjector = faultInjector;
        setWalFileInternal(fileName);
        logger.info("WALManager initialized with file: {} (acknowledged durability={})", fileName, durability());
    }

    public Durability durability() {
        return Durability.FSYNC;
    }

    /** Append and fsync one UTF-8 operation. Any append or sync failure is propagated to the caller. */
    public synchronized void log(String operation, String key, String value) {
        log(
                operation,
                key.getBytes(StandardCharsets.UTF_8),
                value == null ? null : value.getBytes(StandardCharsets.UTF_8));
    }

    /** Append and fsync one operation with arbitrary binary key and value bytes. */
    public synchronized void log(String operation, byte[] key, byte[] value) {
        if (!loggingEnabled) {
            throw new IllegalStateException("WAL logging is disabled");
        }
        if (appendFailure != null) {
            throw new UncheckedIOException("WAL is unavailable after a previous append failure", appendFailure);
        }

        try {
            byte[] record = encodeRecord(operation, key, value);
            ensureChannelOpen();
            faultInjector.trigger(FaultPoint.BEFORE_APPEND);
            ByteBuffer buffer = ByteBuffer.wrap(record);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            faultInjector.trigger(FaultPoint.BEFORE_SYNC);
            channel.force(true);
            logger.debug("WAL operation logged and synced: {}", operation);
        } catch (IOException e) {
            appendFailure = e;
            Metrics.increment("kvdb_wal_failures_total", "node", "append", "error");
            closeChannelQuietly();
            throw new UncheckedIOException("Failed to append and sync WAL operation " + operation, e);
        }
    }

    public void setLoggingEnabled(boolean enabled) {
        this.loggingEnabled = enabled;
        logger.info("WAL logging {}", enabled ? "enabled" : "disabled");
    }

    /** Replay UTF-8 records in order. A corrupt non-tail record aborts recovery. */
    public synchronized List<String[]> replay() {
        List<String[]> operations = new ArrayList<>();
        for (WalRecord record : replayRecords()) {
            operations.add(new String[] {
                record.operation(),
                new String(record.key(), StandardCharsets.UTF_8),
                record.value() == null ? null : new String(record.value(), StandardCharsets.UTF_8)
            });
        }
        return operations;
    }

    /** Replay the binary-safe representation used on disk. */
    public synchronized List<WalRecord> replayRecords() {
        List<WalRecord> records = new ArrayList<>();
        if (!Files.exists(walFile)) {
            return records;
        }

        try (FileChannel readChannel = FileChannel.open(walFile, StandardOpenOption.READ)) {
            long fileSize = readChannel.size();
            long position = 0;
            while (position < fileSize) {
                long recordStart = position;
                ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
                int headerBytes = readFully(readChannel, header, position);
                if (headerBytes < HEADER_SIZE) {
                    logger.warn("Ignoring torn final WAL header at offset {}", recordStart);
                    break;
                }
                header.flip();
                int magic = header.getInt();
                byte version = header.get();
                int payloadLength = header.getInt();
                position += HEADER_SIZE;

                if (magic != RECORD_MAGIC || version != RECORD_VERSION) {
                    throw corruption(recordStart, "invalid magic or version");
                }
                if (payloadLength < LENGTH_FIELDS_SIZE || payloadLength > MAX_PAYLOAD_SIZE) {
                    throw corruption(recordStart, "invalid payload length " + payloadLength);
                }

                long recordEnd = position + payloadLength + CHECKSUM_SIZE;
                if (recordEnd > fileSize) {
                    if (hasValidRecordAfter(readChannel, recordStart + 1, fileSize)) {
                        throw corruption(recordStart, "truncated non-tail record");
                    }
                    logger.warn("Ignoring torn final WAL record at offset {}", recordStart);
                    break;
                }

                ByteBuffer payload = ByteBuffer.allocate(payloadLength);
                readFully(readChannel, payload, position);
                payload.flip();
                position += payloadLength;
                ByteBuffer checksum = ByteBuffer.allocate(CHECKSUM_SIZE);
                readFully(readChannel, checksum, position);
                checksum.flip();
                int storedChecksum = checksum.getInt();
                position += CHECKSUM_SIZE;

                if (storedChecksum != checksum(payload.array())) {
                    if (position == fileSize) {
                        logger.warn("Ignoring torn final WAL record with invalid checksum at offset {}", recordStart);
                        break;
                    }
                    throw corruption(recordStart, "checksum mismatch");
                }
                records.add(decodePayload(payload, recordStart));
            }
            logger.info("Replayed {} operations from WAL file: {}", records.size(), walFile);
            return records;
        } catch (IOException e) {
            Metrics.increment("kvdb_wal_failures_total", "node", "replay", "error");
            throw new UncheckedIOException("Failed to read WAL file " + walFile, e);
        }
    }

    public synchronized Map<String, String[]> replayAsMap() {
        Map<String, String[]> latestOps = new HashMap<>();
        for (String[] operation : replay()) {
            latestOps.put(operation[1], operation);
        }
        return latestOps;
    }

    /** Atomically replace the current WAL with an empty, synced WAL. */
    public synchronized void clear() {
        closeChannelForRotation();
        Path parent = parentDirectory();
        Path tempFile = null;
        try {
            Files.createDirectories(parent);
            tempFile = Files.createTempFile(parent, walFile.getFileName().toString(), ".next");
            try (FileChannel empty = FileChannel.open(tempFile, StandardOpenOption.WRITE)) {
                empty.force(true);
            }
            faultInjector.trigger(FaultPoint.BEFORE_ROTATE_MOVE);
            Files.move(tempFile, walFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            tempFile = null;
            faultInjector.trigger(FaultPoint.BEFORE_ROTATE_DIRECTORY_SYNC);
            forceDirectory(parent);
            appendFailure = null;
            logger.info("WAL atomically rotated: {}", walFile);
        } catch (AtomicMoveNotSupportedException e) {
            throw recordRotationFailure(new IOException("Atomic WAL rotation is not supported", e));
        } catch (IOException e) {
            throw recordRotationFailure(e);
        } finally {
            if (tempFile != null) {
                try {
                    Files.deleteIfExists(tempFile);
                } catch (IOException cleanupError) {
                    logger.warn("Failed to clean temporary WAL {}", tempFile, cleanupError);
                }
            }
        }
    }

    public synchronized void setWalFile(String fileName) {
        closeChannelForRotation();
        setWalFileInternal(fileName);
        appendFailure = null;
        logger.info("WALManager file set to: {}", fileName);
    }

    public synchronized void close() {
        closeChannelForRotation();
    }

    public record WalRecord(String operation, byte[] key, byte[] value) {}

    private byte[] encodeRecord(String operation, byte[] key, byte[] value) throws IOException {
        byte[] operationBytes = operation.getBytes(StandardCharsets.UTF_8);
        int valueLength = value == null ? -1 : value.length;
        long payloadLength = (long) LENGTH_FIELDS_SIZE + operationBytes.length + key.length + Math.max(valueLength, 0);
        if (payloadLength > MAX_PAYLOAD_SIZE) {
            throw new IOException("WAL record exceeds maximum payload size");
        }

        ByteArrayOutputStream bytes = new ByteArrayOutputStream(HEADER_SIZE + (int) payloadLength + CHECKSUM_SIZE);
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(RECORD_MAGIC);
            out.writeByte(RECORD_VERSION);
            out.writeInt((int) payloadLength);
            ByteArrayOutputStream payloadBytes = new ByteArrayOutputStream((int) payloadLength);
            try (DataOutputStream payload = new DataOutputStream(payloadBytes)) {
                payload.writeInt(operationBytes.length);
                payload.writeInt(key.length);
                payload.writeInt(valueLength);
                payload.write(operationBytes);
                payload.write(key);
                if (value != null) {
                    payload.write(value);
                }
            }
            byte[] encodedPayload = payloadBytes.toByteArray();
            out.write(encodedPayload);
            out.writeInt(checksum(encodedPayload));
        }
        return bytes.toByteArray();
    }

    private WalRecord decodePayload(ByteBuffer payload, long offset) {
        int operationLength = payload.getInt();
        int keyLength = payload.getInt();
        int valueLength = payload.getInt();
        long expected = (long) LENGTH_FIELDS_SIZE + operationLength + keyLength + Math.max(valueLength, 0);
        if (operationLength <= 0 || keyLength < 0 || valueLength < -1 || expected != payload.limit()) {
            throw corruption(offset, "invalid field lengths");
        }
        byte[] operation = new byte[operationLength];
        byte[] key = new byte[keyLength];
        byte[] value = valueLength < 0 ? null : new byte[valueLength];
        payload.get(operation);
        payload.get(key);
        if (value != null) {
            payload.get(value);
        }
        return new WalRecord(new String(operation, StandardCharsets.UTF_8), key, value);
    }

    private static int checksum(byte[] payload) {
        CRC32C crc = new CRC32C();
        crc.update(payload, 0, payload.length);
        return (int) crc.getValue();
    }

    private static int readFully(FileChannel channel, ByteBuffer buffer, long position) throws IOException {
        int total = 0;
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer, position + total);
            if (read <= 0) {
                break;
            }
            total += read;
        }
        return total;
    }

    private static boolean hasValidRecordAfter(FileChannel channel, long start, long fileSize) throws IOException {
        for (long offset = start; offset + HEADER_SIZE + LENGTH_FIELDS_SIZE + CHECKSUM_SIZE <= fileSize; offset++) {
            ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
            if (readFully(channel, header, offset) != HEADER_SIZE) {
                return false;
            }
            header.flip();
            if (header.getInt() != RECORD_MAGIC || header.get() != RECORD_VERSION) {
                continue;
            }
            int payloadLength = header.getInt();
            long end = offset + HEADER_SIZE + payloadLength + CHECKSUM_SIZE;
            if (payloadLength < LENGTH_FIELDS_SIZE || payloadLength > MAX_PAYLOAD_SIZE || end > fileSize) {
                continue;
            }
            ByteBuffer payload = ByteBuffer.allocate(payloadLength);
            if (readFully(channel, payload, offset + HEADER_SIZE) != payloadLength) {
                continue;
            }
            ByteBuffer storedChecksum = ByteBuffer.allocate(CHECKSUM_SIZE);
            if (readFully(channel, storedChecksum, offset + HEADER_SIZE + payloadLength) != CHECKSUM_SIZE) {
                continue;
            }
            storedChecksum.flip();
            if (storedChecksum.getInt() == checksum(payload.array())) {
                return true;
            }
        }
        return false;
    }

    private void ensureChannelOpen() throws IOException {
        ensureParentDirectoryExists();
        if (channel == null) {
            channel = FileChannel.open(
                    walFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        }
    }

    private void setWalFileInternal(String fileName) {
        walFile = Paths.get(fileName);
        try {
            ensureParentDirectoryExists();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create WAL directory for " + fileName, e);
        }
    }

    private void ensureParentDirectoryExists() throws IOException {
        Files.createDirectories(parentDirectory());
    }

    private Path parentDirectory() {
        return walFile.toAbsolutePath().getParent();
    }

    private static void forceDirectory(Path directory) throws IOException {
        try (FileChannel directoryChannel = FileChannel.open(directory, StandardOpenOption.READ)) {
            directoryChannel.force(true);
        }
    }

    private void closeChannelForRotation() {
        if (channel == null) {
            return;
        }
        try {
            channel.close();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to close WAL file " + walFile, e);
        } finally {
            channel = null;
        }
    }

    private void closeChannelQuietly() {
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException closeError) {
                logger.warn("Failed to close WAL after append failure", closeError);
            } finally {
                channel = null;
            }
        }
    }

    private UncheckedIOException recordRotationFailure(IOException error) {
        Metrics.increment("kvdb_wal_failures_total", "node", "rotate", "error");
        return new UncheckedIOException("Failed to rotate WAL " + walFile, error);
    }

    private WALCorruptionException corruption(long offset, String reason) {
        Metrics.increment("kvdb_wal_failures_total", "node", "corruption", "error");
        return new WALCorruptionException("Corrupt WAL record at offset " + offset + ": " + reason);
    }
}
