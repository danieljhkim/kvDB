package com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.zip.CRC32C;
import lombok.extern.slf4j.Slf4j;

/**
 * Durable, prefix-compacting Raft log.
 *
 * <p>Version 2 format: {@code [magic][version][base index][base term][header crc]} followed by records
 * {@code [length][protobuf][crc32c]}. Version 1 was the historical length-prefixed protobuf stream. It is read
 * strictly for rolling upgrades and rewritten as version 2 at the first mutation. Truncated, oversized, malformed,
 * checksum-invalid, or non-consecutive records fail construction.
 */
@Slf4j
public class FileBasedRaftLog implements RaftLog {

    static final int MAGIC = 0x4b564c47; // KVLG
    static final int FORMAT_VERSION = 2;
    static final int MAX_ENTRY_BYTES = 16 * 1024 * 1024;
    private static final int HEADER_BYTES = Integer.BYTES * 3 + Long.BYTES * 2;

    private final Path logFile;
    private final Path tempFile;
    private final DurableFileOps durableFiles;
    private final List<Long> indexOffsets = new ArrayList<>();

    private long compactedIndex;
    private long compactedTerm;
    private boolean legacyFormat;
    private IOException failure;

    public FileBasedRaftLog(Path logFile) throws IOException {
        this(logFile, new DurableFileOps());
    }

    FileBasedRaftLog(Path logFile, DurableFileOps durableFiles) throws IOException {
        this.logFile = logFile;
        this.tempFile = logFile.resolveSibling(logFile.getFileName() + ".tmp");
        this.durableFiles = durableFiles;
        Files.createDirectories(logFile.toAbsolutePath().getParent());

        if (!Files.exists(logFile) || Files.size(logFile) == 0) {
            rewrite(List.of(), 0, 0);
            log.info("Created new versioned Raft log file: {}", logFile);
        } else {
            buildIndex();
        }
    }

    private void buildIndex() throws IOException {
        indexOffsets.clear();
        try (RandomAccessFile file = new RandomAccessFile(logFile.toFile(), "r")) {
            if (file.length() < Integer.BYTES) {
                throw corruption("truncated log header");
            }
            int marker = file.readInt();
            if (marker == MAGIC) {
                readVersionedHeader(file);
                legacyFormat = false;
                readVersionedRecords(file);
            } else {
                compactedIndex = 0;
                compactedTerm = 0;
                legacyFormat = true;
                file.seek(0);
                readLegacyRecords(file);
                log.warn("Loaded legacy Raft log {}; it will be upgraded on the next write", logFile);
            }
        }
        log.info(
                "Built Raft log index with {} live entries after compacted index {} from {}",
                indexOffsets.size(),
                compactedIndex,
                logFile);
    }

    private void readVersionedHeader(RandomAccessFile file) throws IOException {
        if (file.length() < HEADER_BYTES) {
            throw corruption("truncated versioned log header");
        }
        int version = file.readInt();
        if (version != FORMAT_VERSION) {
            throw corruption("unsupported log format version " + version);
        }
        compactedIndex = file.readLong();
        compactedTerm = file.readLong();
        int expected = file.readInt();
        int actual = checksum(headerPayload(compactedIndex, compactedTerm));
        if (expected != actual) {
            throw corruption("log header checksum mismatch");
        }
        if (compactedIndex < 0 || compactedTerm < 0) {
            throw corruption("negative compacted index or term");
        }
    }

    private void readVersionedRecords(RandomAccessFile file) throws IOException {
        long expectedIndex = compactedIndex + 1;
        while (file.getFilePointer() < file.length()) {
            long offset = file.getFilePointer();
            int length = readLength(file, "log record");
            ensureRemaining(file, (long) length + Integer.BYTES, "log record");
            byte[] data = new byte[length];
            file.readFully(data);
            int expectedChecksum = file.readInt();
            if (checksum(data) != expectedChecksum) {
                throw corruption("checksum mismatch at byte offset " + offset);
            }
            RaftLogEntry entry = decode(data, offset);
            if (entry.index() != expectedIndex) {
                throw corruption("non-consecutive entry at byte offset " + offset + ": expected index " + expectedIndex
                        + " but found " + entry.index());
            }
            indexOffsets.add(offset);
            expectedIndex++;
        }
    }

    private void readLegacyRecords(RandomAccessFile file) throws IOException {
        long expectedIndex = 1;
        while (file.getFilePointer() < file.length()) {
            long offset = file.getFilePointer();
            int length = readLength(file, "legacy log record");
            ensureRemaining(file, length, "legacy log record");
            byte[] data = new byte[length];
            file.readFully(data);
            RaftLogEntry entry = decode(data, offset);
            if (entry.index() != expectedIndex) {
                throw corruption("non-consecutive legacy entry at byte offset " + offset + ": expected index "
                        + expectedIndex + " but found " + entry.index());
            }
            indexOffsets.add(offset);
            expectedIndex++;
        }
    }

    private int readLength(RandomAccessFile file, String recordType) throws IOException {
        try {
            int length = file.readInt();
            if (length <= 0 || length > MAX_ENTRY_BYTES) {
                throw corruption(recordType + " length " + length + " is outside 1.." + MAX_ENTRY_BYTES);
            }
            return length;
        } catch (EOFException e) {
            throw corruption("truncated " + recordType + " length", e);
        }
    }

    private void ensureRemaining(RandomAccessFile file, long required, String recordType) throws IOException {
        long remaining = file.length() - file.getFilePointer();
        if (remaining < required) {
            throw corruption("truncated " + recordType + ": requires " + required + " bytes, has " + remaining);
        }
    }

    private RaftLogEntry decode(byte[] data, long offset) throws IOException {
        try {
            return RaftLogEntry.fromBytes(data);
        } catch (Exception e) {
            throw corruption("invalid protobuf record at byte offset " + offset, e);
        }
    }

    @Override
    public synchronized void append(RaftLogEntry entry) throws IOException {
        ensureHealthy();
        if (entry.index() != lastIndex() + 1) {
            throw new IOException(
                    "Refusing non-consecutive Raft entry " + entry.index() + "; expected " + (lastIndex() + 1));
        }
        if (legacyFormat) {
            rewrite(readAllEntries(), compactedIndex, compactedTerm);
        }

        byte[] data = entry.toBytes();
        validateEntryLength(data.length);
        long offset = Files.size(logFile);
        ByteBuffer record = ByteBuffer.allocate(Integer.BYTES + data.length + Integer.BYTES)
                .putInt(data.length)
                .put(data)
                .putInt(checksum(data));
        record.flip();
        try (FileChannel channel = FileChannel.open(logFile, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            while (record.hasRemaining()) {
                channel.write(record);
            }
            channel.force(true);
        } catch (IOException e) {
            throw poison(e);
        }
        indexOffsets.add(offset);
        log.debug("Appended durable Raft entry at index {} (offset={})", entry.index(), offset);
    }

    @Override
    public synchronized Optional<RaftLogEntry> getEntry(long index) throws IOException {
        ensureHealthy();
        if (index <= compactedIndex || index > lastIndex()) {
            return Optional.empty();
        }
        int arrayIndex = Math.toIntExact(index - compactedIndex - 1);
        long offset = indexOffsets.get(arrayIndex);
        try (RandomAccessFile file = new RandomAccessFile(logFile.toFile(), "r")) {
            file.seek(offset);
            int length = readLength(file, legacyFormat ? "legacy log record" : "log record");
            ensureRemaining(file, (long) length + (legacyFormat ? 0 : Integer.BYTES), "log record");
            byte[] data = new byte[length];
            file.readFully(data);
            if (!legacyFormat && file.readInt() != checksum(data)) {
                throw corruption("checksum mismatch at index " + index);
            }
            return Optional.of(decode(data, offset));
        } catch (IOException e) {
            throw poison(e);
        }
    }

    @Override
    public synchronized List<RaftLogEntry> getEntriesSince(long fromIndex) throws IOException {
        long start = Math.max(fromIndex, firstIndex());
        List<RaftLogEntry> entries = new ArrayList<>();
        for (long index = start; index <= lastIndex(); index++) {
            Optional<RaftLogEntry> entry = getEntry(index);
            if (entry.isEmpty()) {
                throw corruption("missing indexed entry " + index);
            }
            entries.add(entry.get());
        }
        return entries;
    }

    @Override
    public synchronized Optional<RaftLogEntry> getLastEntry() throws IOException {
        return indexOffsets.isEmpty() ? Optional.empty() : getEntry(lastIndex());
    }

    @Override
    public synchronized long size() {
        ensureHealthyUnchecked();
        return indexOffsets.size();
    }

    @Override
    public synchronized long firstIndex() {
        ensureHealthyUnchecked();
        return compactedIndex + 1;
    }

    @Override
    public synchronized long lastIndex() {
        ensureHealthyUnchecked();
        return compactedIndex + indexOffsets.size();
    }

    @Override
    public synchronized long compactedIndex() {
        ensureHealthyUnchecked();
        return compactedIndex;
    }

    @Override
    public synchronized long compactedTerm() {
        ensureHealthyUnchecked();
        return compactedTerm;
    }

    @Override
    public synchronized void truncateAfter(long index) throws IOException {
        ensureHealthy();
        if (index < compactedIndex) {
            throw new IOException("Cannot truncate before durable snapshot index " + compactedIndex);
        }
        if (index >= lastIndex()) {
            return;
        }
        int keep = Math.toIntExact(index - compactedIndex);
        rewrite(new ArrayList<>(readAllEntries().subList(0, keep)), compactedIndex, compactedTerm);
        log.info("Truncated Raft log after index {}; last index is now {}", index, lastIndex());
    }

    @Override
    public synchronized void compactThrough(long index, long term) throws IOException {
        ensureHealthy();
        if (index < compactedIndex) {
            throw new IOException("Compaction index " + index + " precedes durable snapshot index " + compactedIndex);
        }
        if (index == compactedIndex) {
            if (term != compactedTerm) {
                throw new IOException("Compaction term mismatch at existing snapshot boundary");
            }
            return;
        }
        if (index > lastIndex()) {
            rewrite(List.of(), index, term);
            log.info("Installed Raft snapshot boundary at index {} term {} beyond the prior log", index, term);
            return;
        }
        List<RaftLogEntry> entries = readAllEntries();
        int remove = Math.toIntExact(index - compactedIndex);
        long actualTerm = getTerm(index).orElseThrow(() -> corruption("missing compaction boundary " + index));
        List<RaftLogEntry> suffix =
                actualTerm == term ? new ArrayList<>(entries.subList(remove, entries.size())) : List.of();
        rewrite(suffix, index, term);
        log.info("Compacted durable Raft log through index {} term {}", index, term);
    }

    private List<RaftLogEntry> readAllEntries() throws IOException {
        return getEntriesSince(firstIndex());
    }

    private void rewrite(List<RaftLogEntry> entries, long newCompactedIndex, long newCompactedTerm) throws IOException {
        ensureHealthy();
        try {
            try (DataOutputStream output = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(
                    tempFile,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE)))) {
                writeHeader(output, newCompactedIndex, newCompactedTerm);
                long expectedIndex = newCompactedIndex + 1;
                for (RaftLogEntry entry : entries) {
                    if (entry.index() != expectedIndex++) {
                        throw new IOException("Cannot persist non-consecutive Raft entry " + entry.index());
                    }
                    byte[] data = entry.toBytes();
                    validateEntryLength(data.length);
                    output.writeInt(data.length);
                    output.write(data);
                    output.writeInt(checksum(data));
                }
            }
            durableFiles.atomicReplace(tempFile, logFile);
            compactedIndex = newCompactedIndex;
            compactedTerm = newCompactedTerm;
            legacyFormat = false;
            buildIndex();
        } catch (IOException e) {
            throw poison(e);
        }
    }

    private void writeHeader(DataOutputStream output, long baseIndex, long baseTerm) throws IOException {
        output.writeInt(MAGIC);
        output.writeInt(FORMAT_VERSION);
        output.writeLong(baseIndex);
        output.writeLong(baseTerm);
        output.writeInt(checksum(headerPayload(baseIndex, baseTerm)));
    }

    private byte[] headerPayload(long baseIndex, long baseTerm) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeInt(MAGIC);
            output.writeInt(FORMAT_VERSION);
            output.writeLong(baseIndex);
            output.writeLong(baseTerm);
        }
        return bytes.toByteArray();
    }

    private void validateEntryLength(int length) throws IOException {
        if (length <= 0 || length > MAX_ENTRY_BYTES) {
            throw new IOException("Raft log entry length " + length + " is outside 1.." + MAX_ENTRY_BYTES);
        }
    }

    static int checksum(byte[] data) {
        CRC32C checksum = new CRC32C();
        checksum.update(data, 0, data.length);
        return (int) checksum.getValue();
    }

    private IOException corruption(String detail) {
        return new IOException("Corrupt Raft log " + logFile + ": " + detail);
    }

    private IOException corruption(String detail, Throwable cause) {
        return new IOException("Corrupt Raft log " + logFile + ": " + detail, cause);
    }

    private void ensureHealthy() throws IOException {
        if (failure != null) {
            throw new IOException("Raft log is unavailable after a prior persistence failure", failure);
        }
    }

    private void ensureHealthyUnchecked() {
        if (failure != null) {
            throw new IllegalStateException("Raft log is unavailable after a prior persistence failure", failure);
        }
    }

    private IOException poison(IOException error) {
        failure = error;
        return error;
    }

    @Override
    public void close() {
        log.info("Closing Raft log at {} with {} live entries", logFile, indexOffsets.size());
    }
}
