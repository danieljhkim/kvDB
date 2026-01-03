package com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence;

import com.google.protobuf.InvalidProtocolBufferException;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * File-based implementation of RaftLog that stores log entries in a binary format.
 * Each entry is prefixed with its size (4 bytes) followed by the Protocol Buffer serialized data.
 *
 * <p>Format: [size (4 bytes)][protobuf data (size bytes)]...
 *
 * <p>This implementation maintains an in-memory index of byte offsets for fast random access.
 */
@Slf4j
public class FileBasedRaftLog implements RaftLog {

    private final Path logFile;
    private final List<Long> indexOffsets; // Byte offset for each entry

    public FileBasedRaftLog(Path logFile) throws IOException {
        this.logFile = logFile;
        this.indexOffsets = new ArrayList<>();

        if (!Files.exists(logFile)) {
            Files.createDirectories(logFile.getParent());
            Files.createFile(logFile);
            log.info("Created new Raft log file: {}", logFile);
        } else {
            buildIndex();
        }
    }

    /**
     * Builds the in-memory index by reading the log file and recording byte offsets.
     */
    private void buildIndex() throws IOException {
        indexOffsets.clear();
        try (DataInputStream dis = new DataInputStream(new BufferedInputStream(Files.newInputStream(logFile)))) {

            long offset = 0;
            while (dis.available() > 0) {
                indexOffsets.add(offset);
                int entrySize = dis.readInt();
                offset += 4 + entrySize;
                dis.skipBytes(entrySize);
            }
        }
        log.info("Built index with {} entries from {}", indexOffsets.size(), logFile);
    }

    @Override
    public synchronized void append(RaftLogEntry entry) throws IOException {
        byte[] serialized = entry.toBytes();

        try (DataOutputStream dos = new DataOutputStream(
                new BufferedOutputStream(Files.newOutputStream(logFile, StandardOpenOption.APPEND)))) {

            dos.writeInt(serialized.length);
            dos.write(serialized);
            dos.flush();

            long offset = indexOffsets.isEmpty()
                    ? 0
                    : indexOffsets.get(indexOffsets.size() - 1) + getEntrySize(indexOffsets.size() - 1);
            indexOffsets.add(offset);

            log.debug("Appended entry at index {} (offset={})", entry.index(), offset);
        }
    }

    /**
     * Calculates the size of an entry including the 4-byte size prefix.
     */
    private int getEntrySize(int index) throws IOException {
        if (index < 0 || index >= indexOffsets.size()) {
            throw new IllegalArgumentException("Invalid index: " + index);
        }

        long offset = indexOffsets.get(index);

        try (RandomAccessFile raf = new RandomAccessFile(logFile.toFile(), "r")) {
            raf.seek(offset);
            int entrySize = raf.readInt();
            return 4 + entrySize; // size prefix + data
        }
    }

    @Override
    public synchronized Optional<RaftLogEntry> getEntry(long index) throws IOException {
        if (index < 0 || index >= indexOffsets.size()) {
            return Optional.empty();
        }

        long offset = indexOffsets.get((int) index);

        try (RandomAccessFile raf = new RandomAccessFile(logFile.toFile(), "r")) {
            raf.seek(offset);
            int entrySize = raf.readInt();
            byte[] data = new byte[entrySize];
            raf.readFully(data);
            return Optional.of(RaftLogEntry.fromBytes(data));
        } catch (InvalidProtocolBufferException e) {
            log.error("Failed to deserialize entry at index {}", index, e);
            throw new IOException("Failed to deserialize log entry", e);
        }
    }

    @Override
    public synchronized List<RaftLogEntry> getEntriesSince(long fromIndex) throws IOException {
        List<RaftLogEntry> entries = new ArrayList<>();
        for (long i = fromIndex; i < indexOffsets.size(); i++) {
            getEntry(i).ifPresent(entries::add);
        }
        return entries;
    }

    @Override
    public synchronized Optional<RaftLogEntry> getLastEntry() throws IOException {
        if (indexOffsets.isEmpty()) {
            return Optional.empty();
        }
        return getEntry(indexOffsets.size() - 1);
    }

    @Override
    public synchronized long size() {
        return indexOffsets.size();
    }

    @Override
    public synchronized void truncateAfter(long index) throws IOException {
        if (index >= indexOffsets.size()) {
            return;
        }

        long offset = indexOffsets.get((int) index);
        try (RandomAccessFile raf = new RandomAccessFile(logFile.toFile(), "rw")) {
            raf.setLength(offset);
        }

        indexOffsets.subList((int) index, indexOffsets.size()).clear();
        log.info("Truncated log after index {}, new size: {}", index, indexOffsets.size());
    }

    @Override
    public void close() throws IOException {
        log.info("Closing Raft log with {} entries", indexOffsets.size());
        // No resources to clean up in current implementation
    }
}
