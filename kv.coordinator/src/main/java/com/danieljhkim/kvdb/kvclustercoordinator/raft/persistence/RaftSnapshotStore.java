package com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.zip.CRC32C;

/** Durable storage and resumable chunk assembly for Raft state-machine snapshots. */
public class RaftSnapshotStore {

    static final int MAGIC = 0x4b56534e; // KVSN
    static final int INSTALL_MAGIC = 0x4b565349; // KVSI
    static final int FORMAT_VERSION = 1;
    public static final int MAX_SNAPSHOT_BYTES = 64 * 1024 * 1024;
    public static final int MAX_CHUNK_BYTES = 512 * 1024;
    private static final int HEADER_BYTES = Integer.BYTES * 4 + Long.BYTES * 3;
    private static final int INSTALL_HEADER_BYTES = Integer.BYTES * 3 + Long.BYTES * 3;

    private final Path snapshotFile;
    private final Path temporaryFile;
    private final Path installingFile;
    private final DurableFileOps durableFiles;

    public RaftSnapshotStore(Path directory) throws IOException {
        this(directory, new DurableFileOps());
    }

    RaftSnapshotStore(Path directory, DurableFileOps durableFiles) throws IOException {
        Files.createDirectories(directory);
        this.snapshotFile = directory.resolve("raft_snapshot.bin");
        this.temporaryFile = directory.resolve("raft_snapshot.bin.tmp");
        this.installingFile = directory.resolve("raft_snapshot.installing");
        this.durableFiles = durableFiles;
        if (Files.exists(snapshotFile)) {
            load(); // Fail construction immediately on corrupt safety-critical state.
        }
        if (Files.exists(installingFile)) {
            try {
                inspectInstalling(); // A complete partial transfer can resume after restart.
            } catch (IOException invalidTemporaryInstall) {
                // This file is staging only and has never replaced the durable snapshot. A crash while creating its
                // header safely restarts transfer at offset zero instead of making the node unavailable.
                Files.deleteIfExists(installingFile);
                durableFiles.forceDirectory(directory.toAbsolutePath());
            }
        }
    }

    public synchronized Snapshot save(long lastIncludedIndex, long lastIncludedTerm, byte[] data) throws IOException {
        validateMetadata(lastIncludedIndex, lastIncludedTerm, data.length);
        int checksum = checksum(data);
        try (DataOutputStream output = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(
                temporaryFile,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE)))) {
            writeSnapshotHeader(output, lastIncludedIndex, lastIncludedTerm, data.length, checksum);
            output.write(data);
        }
        durableFiles.atomicReplace(temporaryFile, snapshotFile);
        return new Snapshot(lastIncludedIndex, lastIncludedTerm, data.clone(), checksum);
    }

    public synchronized Optional<Snapshot> load() throws IOException {
        if (!Files.exists(snapshotFile)) {
            return Optional.empty();
        }
        try (RandomAccessFile file = new RandomAccessFile(snapshotFile.toFile(), "r")) {
            if (file.length() < HEADER_BYTES) {
                throw corruption("truncated snapshot header");
            }
            int magic = file.readInt();
            int version = file.readInt();
            long index = file.readLong();
            long term = file.readLong();
            long length = file.readLong();
            int dataChecksum = file.readInt();
            int headerChecksum = file.readInt();
            if (magic != MAGIC || version != FORMAT_VERSION) {
                throw corruption("unsupported snapshot magic or version");
            }
            validateMetadata(index, term, length);
            if (headerChecksum != checksum(snapshotHeaderPayload(index, term, length, dataChecksum))) {
                throw corruption("snapshot header checksum mismatch");
            }
            if (file.length() != HEADER_BYTES + length) {
                throw corruption("snapshot length mismatch: header=" + length + ", file=" + file.length());
            }
            byte[] data = new byte[Math.toIntExact(length)];
            file.readFully(data);
            if (checksum(data) != dataChecksum) {
                throw corruption("snapshot data checksum mismatch");
            }
            return Optional.of(new Snapshot(index, term, data, dataChecksum));
        }
    }

    /**
     * Appends one leader chunk. A nonmatching offset is rejected with the exact durable resume offset; offset zero
     * starts a new transfer. Completion first installs the durable snapshot atomically, leaving log compaction to the
     * caller.
     */
    public synchronized ChunkResult installChunk(
            long index, long term, long offset, byte[] chunk, boolean done, long totalSize, int expectedChecksum)
            throws IOException {
        validateMetadata(index, term, totalSize);
        if (chunk.length > MAX_CHUNK_BYTES) {
            throw new IOException("Snapshot chunk exceeds " + MAX_CHUNK_BYTES + " bytes");
        }
        if (offset < 0 || offset + chunk.length > totalSize) {
            throw new IOException("Snapshot chunk range is outside declared total size");
        }

        InstallMetadata metadata;
        if (offset == 0) {
            writeInstallHeader(index, term, totalSize, expectedChecksum);
            metadata = new InstallMetadata(index, term, totalSize, expectedChecksum, 0);
        } else if (!Files.exists(installingFile)) {
            return new ChunkResult(false, false, 0, null);
        } else {
            metadata = inspectInstalling();
            if (metadata.index != index
                    || metadata.term != term
                    || metadata.totalSize != totalSize
                    || metadata.checksum != expectedChecksum) {
                return new ChunkResult(false, false, 0, null);
            }
        }

        if (offset != metadata.receivedBytes) {
            return new ChunkResult(false, false, metadata.receivedBytes, null);
        }
        try (FileChannel channel =
                FileChannel.open(installingFile, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            java.nio.ByteBuffer bytes = java.nio.ByteBuffer.wrap(chunk);
            while (bytes.hasRemaining()) {
                channel.write(bytes);
            }
            channel.force(true);
        }
        long nextOffset = offset + chunk.length;
        if (!done) {
            return new ChunkResult(true, false, nextOffset, null);
        }
        if (nextOffset != totalSize) {
            throw new IOException("Final snapshot chunk ended at " + nextOffset + " but total size is " + totalSize);
        }

        byte[] data = readInstallingData(totalSize);
        if (checksum(data) != expectedChecksum) {
            throw corruption("installed snapshot checksum mismatch");
        }
        Snapshot installed = save(index, term, data);
        Files.delete(installingFile);
        durableFiles.forceDirectory(snapshotFile.toAbsolutePath().getParent());
        return new ChunkResult(true, true, nextOffset, installed);
    }

    private void writeInstallHeader(long index, long term, long totalSize, int dataChecksum) throws IOException {
        try (DataOutputStream output = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(
                installingFile,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE)))) {
            output.writeInt(INSTALL_MAGIC);
            output.writeInt(FORMAT_VERSION);
            output.writeLong(index);
            output.writeLong(term);
            output.writeLong(totalSize);
            output.writeInt(dataChecksum);
        }
        durableFiles.forceFile(installingFile);
    }

    private InstallMetadata inspectInstalling() throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(installingFile.toFile(), "r")) {
            if (file.length() < INSTALL_HEADER_BYTES) {
                throw corruption("truncated installing snapshot header");
            }
            if (file.readInt() != INSTALL_MAGIC || file.readInt() != FORMAT_VERSION) {
                throw corruption("invalid installing snapshot magic or version");
            }
            long index = file.readLong();
            long term = file.readLong();
            long total = file.readLong();
            int dataChecksum = file.readInt();
            validateMetadata(index, term, total);
            long received = file.length() - INSTALL_HEADER_BYTES;
            if (received < 0 || received > total) {
                throw corruption("installing snapshot length exceeds declared total");
            }
            return new InstallMetadata(index, term, total, dataChecksum, received);
        }
    }

    private byte[] readInstallingData(long totalSize) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(installingFile.toFile(), "r")) {
            file.seek(INSTALL_HEADER_BYTES);
            byte[] data = new byte[Math.toIntExact(totalSize)];
            file.readFully(data);
            return data;
        }
    }

    private void writeSnapshotHeader(DataOutputStream output, long index, long term, long length, int dataChecksum)
            throws IOException {
        output.writeInt(MAGIC);
        output.writeInt(FORMAT_VERSION);
        output.writeLong(index);
        output.writeLong(term);
        output.writeLong(length);
        output.writeInt(dataChecksum);
        output.writeInt(checksum(snapshotHeaderPayload(index, term, length, dataChecksum)));
    }

    private byte[] snapshotHeaderPayload(long index, long term, long length, int dataChecksum) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeInt(MAGIC);
            output.writeInt(FORMAT_VERSION);
            output.writeLong(index);
            output.writeLong(term);
            output.writeLong(length);
            output.writeInt(dataChecksum);
        }
        return bytes.toByteArray();
    }

    private void validateMetadata(long index, long term, long length) throws IOException {
        if (index < 0 || term < 0 || length < 0 || length > MAX_SNAPSHOT_BYTES) {
            throw new IOException(
                    "Invalid snapshot metadata: index=" + index + ", term=" + term + ", length=" + length);
        }
    }

    public static int checksum(byte[] data) {
        CRC32C checksum = new CRC32C();
        checksum.update(data, 0, data.length);
        return (int) checksum.getValue();
    }

    private IOException corruption(String detail) {
        return new IOException("Corrupt Raft snapshot " + snapshotFile + ": " + detail);
    }

    public record Snapshot(long lastIncludedIndex, long lastIncludedTerm, byte[] data, int checksum) {
        public Snapshot {
            data = data.clone();
        }

        @Override
        public byte[] data() {
            return data.clone();
        }
    }

    public record ChunkResult(boolean accepted, boolean complete, long nextOffset, Snapshot snapshot) {}

    private record InstallMetadata(long index, long term, long totalSize, int checksum, long receivedBytes) {}
}
