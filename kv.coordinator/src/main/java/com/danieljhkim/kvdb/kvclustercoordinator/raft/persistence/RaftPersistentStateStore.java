package com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Properties;

/**
 * Persists Raft's durable state (currentTerm and votedFor) to disk.
 *
 * <p>
 * This class ensures that critical Raft state survives crashes and restarts, which is essential for maintaining safety
 * guarantees.
 *
 * <p>
 * The state is stored in a simple properties file with atomic writes using a temporary file and rename strategy.
 */
@Slf4j
public class RaftPersistentStateStore {


    private static final String STATE_FILE_NAME = "raft_state.properties";
    private static final String STATE_FILE_TEMP = "raft_state.properties.tmp";
    private static final String KEY_CURRENT_TERM = "currentTerm";
    private static final String KEY_VOTED_FOR = "votedFor";

    private final Path stateFilePath;
    private final Path tempFilePath;

    public RaftPersistentStateStore(String dataDirectory) throws IOException {
        Path dataDir = Path.of(dataDirectory);
        Files.createDirectories(dataDir);
        this.stateFilePath = dataDir.resolve(STATE_FILE_NAME);
        this.tempFilePath = dataDir.resolve(STATE_FILE_TEMP);
    }

    /**
     * Persists the current term and votedFor to disk.
     *
     * @param currentTerm the current Raft term
     * @param votedFor the candidate this node voted for in the current term (null if not voted)
     * @throws IOException if the state cannot be persisted
     */
    public void save(long currentTerm, String votedFor) throws IOException {
        Properties props = new Properties();
        props.setProperty(KEY_CURRENT_TERM, String.valueOf(currentTerm));
        if (votedFor != null) {
            props.setProperty(KEY_VOTED_FOR, votedFor);
        }

        // Write to temp file first, then atomically rename
        try (var out = Files.newOutputStream(tempFilePath)) {
            props.store(out, "Raft Persistent State");
        }

        // Atomic move
        Files.move(tempFilePath, stateFilePath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

        log.debug("Persisted state: term={}, votedFor={}", currentTerm, votedFor);
    }

    /**
     * Loads the persistent state from disk.
     *
     * @return the persistent state, or a default state (term=0, votedFor=null) if no state file exists
     * @throws IOException if the state file exists but cannot be read
     */
    public PersistentState load() throws IOException {
        if (!Files.exists(stateFilePath)) {
            log.info("No persistent state file found, starting with term=0");
            return new PersistentState(0, null);
        }

        Properties props = new Properties();
        try (var in = Files.newInputStream(stateFilePath)) {
            props.load(in);
        }

        long currentTerm = Long.parseLong(props.getProperty(KEY_CURRENT_TERM, "0"));
        String votedFor = props.getProperty(KEY_VOTED_FOR);

        log.info("Loaded persistent state: term={}, votedFor={}", currentTerm, votedFor);
        return new PersistentState(currentTerm, votedFor);
    }

    /**
     * Clears the persistent state file. Useful for testing or resetting a node.
     *
     * @throws IOException if the file cannot be deleted
     */
    public void clear() throws IOException {
        Files.deleteIfExists(stateFilePath);
        Files.deleteIfExists(tempFilePath);
        log.info("Cleared persistent state");
    }

    /**
     * Represents the persistent Raft state loaded from disk.
     */
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
