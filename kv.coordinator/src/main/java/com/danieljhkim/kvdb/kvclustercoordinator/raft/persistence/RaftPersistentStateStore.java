package com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Properties;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Persists Raft's safety-critical term and vote with an atomic, checksummed durable replacement.
 *
 * <p>The historical properties format remains readable for rolling upgrades. New writes use version 2 and never
 * treat a malformed existing state file as a fresh node.
 */
@Slf4j
public class RaftPersistentStateStore {

    static final int MAGIC = 0x4b565253; // KVRS
    static final int FORMAT_VERSION = 2;
    static final int MAX_PAYLOAD_BYTES = 64 * 1024;
    private static final String STATE_FILE_NAME = "raft_state.properties";
    private static final String STATE_FILE_TEMP = "raft_state.properties.tmp";
    private static final String KEY_CURRENT_TERM = "currentTerm";
    private static final String KEY_VOTED_FOR = "votedFor";

    private final Path stateFilePath;
    private final Path tempFilePath;
    private final DurableFileOps durableFiles;

    public RaftPersistentStateStore(String dataDirectory) throws IOException {
        this(Path.of(dataDirectory), new DurableFileOps());
    }

    RaftPersistentStateStore(Path dataDirectory, DurableFileOps durableFiles) throws IOException {
        Files.createDirectories(dataDirectory);
        this.stateFilePath = dataDirectory.resolve(STATE_FILE_NAME);
        this.tempFilePath = dataDirectory.resolve(STATE_FILE_TEMP);
        this.durableFiles = durableFiles;
    }

    /** Persists term/vote before the caller exposes the corresponding Raft state transition. */
    public synchronized void save(long currentTerm, String votedFor) throws IOException {
        if (currentTerm < 0) {
            throw new IllegalArgumentException("currentTerm cannot be negative");
        }
        byte[] payload = encode(currentTerm, votedFor);
        if (payload.length > MAX_PAYLOAD_BYTES) {
            throw new IOException("Raft state payload exceeds " + MAX_PAYLOAD_BYTES + " bytes");
        }
        try (DataOutputStream output = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(
                tempFilePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE)))) {
            output.writeInt(MAGIC);
            output.writeInt(FORMAT_VERSION);
            output.writeInt(payload.length);
            output.write(payload);
            output.writeInt(FileBasedRaftLog.checksum(payload));
        }
        durableFiles.atomicReplace(tempFilePath, stateFilePath);
        log.debug("Persisted durable Raft state: term={}, votedFor={}", currentTerm, votedFor);
    }

    /**
     * Loads durable state. A missing file is the only condition that yields a fresh term-zero node; every malformed
     * existing file fails closed with an actionable {@link IOException}.
     */
    public synchronized PersistentState load() throws IOException {
        if (!Files.exists(stateFilePath)) {
            log.info("No persistent Raft state file found, starting with term=0");
            return new PersistentState(0, null);
        }
        if (Files.size(stateFilePath) < Integer.BYTES) {
            throw corruption("truncated state header");
        }

        try (DataInputStream input =
                new DataInputStream(new BufferedInputStream(Files.newInputStream(stateFilePath)))) {
            int marker = input.readInt();
            if (marker != MAGIC) {
                return loadLegacy();
            }
            int version = input.readInt();
            if (version != FORMAT_VERSION) {
                throw corruption("unsupported state format version " + version);
            }
            int length = input.readInt();
            if (length <= 0 || length > MAX_PAYLOAD_BYTES) {
                throw corruption("state payload length " + length + " is outside 1.." + MAX_PAYLOAD_BYTES);
            }
            byte[] payload = input.readNBytes(length);
            if (payload.length != length) {
                throw corruption("truncated state payload");
            }
            int expectedChecksum = input.readInt();
            if (input.read() != -1) {
                throw corruption("trailing bytes after state record");
            }
            if (FileBasedRaftLog.checksum(payload) != expectedChecksum) {
                throw corruption("state checksum mismatch");
            }
            PersistentState state = decode(payload);
            log.info("Loaded persistent Raft state: term={}, votedFor={}", state.currentTerm, state.votedFor);
            return state;
        } catch (java.io.EOFException e) {
            throw corruption("truncated state record", e);
        }
    }

    private PersistentState loadLegacy() throws IOException {
        Properties properties = new Properties();
        try (var input = Files.newInputStream(stateFilePath)) {
            properties.load(input);
        }
        String termValue = properties.getProperty(KEY_CURRENT_TERM);
        if (termValue == null || termValue.isBlank()) {
            throw corruption("legacy state is missing currentTerm");
        }
        try {
            long term = Long.parseLong(termValue);
            if (term < 0) {
                throw corruption("legacy state has negative currentTerm");
            }
            String votedFor = properties.getProperty(KEY_VOTED_FOR);
            log.warn("Loaded legacy Raft state {}; it will be upgraded on the next write", stateFilePath);
            return new PersistentState(term, votedFor);
        } catch (NumberFormatException e) {
            throw corruption("legacy currentTerm is not a number", e);
        }
    }

    private byte[] encode(long currentTerm, String votedFor) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeLong(currentTerm);
            output.writeBoolean(votedFor != null);
            if (votedFor != null) {
                output.writeUTF(votedFor);
            }
        }
        return bytes.toByteArray();
    }

    private PersistentState decode(byte[] payload) throws IOException {
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(payload))) {
            long term = input.readLong();
            boolean hasVote = input.readBoolean();
            String votedFor = hasVote ? input.readUTF() : null;
            if (term < 0 || input.read() != -1) {
                throw corruption("invalid state payload");
            }
            return new PersistentState(term, votedFor);
        }
    }

    public synchronized void clear() throws IOException {
        boolean changed = Files.deleteIfExists(stateFilePath) | Files.deleteIfExists(tempFilePath);
        if (changed) {
            durableFiles.forceDirectory(stateFilePath.toAbsolutePath().getParent());
        }
        log.info("Cleared persistent Raft state");
    }

    private IOException corruption(String detail) {
        return new IOException("Corrupt Raft state " + stateFilePath + ": " + detail);
    }

    private IOException corruption(String detail, Throwable cause) {
        return new IOException("Corrupt Raft state " + stateFilePath + ": " + detail, cause);
    }

    @Getter
    public static class PersistentState {
        private final long currentTerm;
        private final String votedFor;

        public PersistentState(long currentTerm, String votedFor) {
            this.currentTerm = currentTerm;
            this.votedFor = votedFor;
        }

        @Override
        public String toString() {
            return "PersistentState{" + "currentTerm=" + currentTerm + ", votedFor='" + votedFor + '\'' + '}';
        }
    }
}
