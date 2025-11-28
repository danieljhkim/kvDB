package com.kvdb.kvcommon.persistence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Write-Ahead Log (WAL)
public class WALManager {

    private static final Logger logger = LoggerFactory.getLogger(WALManager.class);

    /** Whether new log entries should be appended to the WAL. */
    private volatile boolean loggingEnabled = true;

    /** Path to the WAL file. */
    private Path walFile;

    public WALManager(String fileName) {
        setWalFileInternal(fileName);
        logger.info("WALManager initialized with file: {}", fileName);
    }

    /**
     * Append a single operation to the WAL. Format: "OP KEY VALUE\n", where VALUE may be empty but
     * not contain newlines.
     */
    public synchronized void log(String operation, String key, String value) {
        if (!loggingEnabled) {
            // In case logging is disabled during recovery / maintenance
            logger.debug("Logging operation rejected: logging is disabled");
            return;
        }
        try {
            ensureParentDirectoryExists();
            try (BufferedWriter writer =
                    Files.newBufferedWriter(
                            walFile, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
                writer.write(operation);
                writer.write(' ');
                writer.write(key);
                writer.write(' ');
                writer.write(value != null ? value : "");
                writer.write('\n');

                logger.debug("Operation logged: {} {} {}", operation, key, value);
            }
        } catch (IOException e) {
            logger.error("Failed to log operation: {} {} {}", operation, key, value, e);
        }
    }

    /** Enable or disable logging. When disabled, {@link #log} is a no-op. */
    public void setLoggingEnabled(boolean enabled) {
        this.loggingEnabled = enabled;
        logger.info("WAL logging {}", enabled ? "enabled" : "disabled");
    }

    /**
     * Replay WAL as a full list, in order. Each entry is String[3] = { op, key, value } where value
     * may be empty.
     */
    public synchronized List<String[]> replay() {
        List<String[]> ops = new ArrayList<>();

        if (!Files.exists(walFile)) {
            logger.info("WAL file does not exist, nothing to replay");
            return ops;
        }

        try (BufferedReader reader = Files.newBufferedReader(walFile)) {
            String line;
            int count = 0;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.trim().split(" ", 3);
                ops.add(parts);
                count++;
            }
            logger.info("Replayed {} operations from WAL file: {}", count, walFile);
        } catch (IOException e) {
            logger.error("Failed to read WAL file: {}", walFile, e);
        }

        return ops;
    }

    /**
     * Replay WAL and return only the last operation for each key. Map: key -> latest { op, key,
     * value }
     */
    public synchronized Map<String, String[]> replayAsMap() {
        Map<String, String[]> latestOps = new HashMap<>();

        if (!Files.exists(walFile)) {
            logger.info("WAL file does not exist, nothing to replay");
            return latestOps;
        }

        try (BufferedReader reader = Files.newBufferedReader(walFile)) {
            String line;
            int count = 0;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.trim().split(" ", 3);
                if (parts.length < 2) {
                    continue;
                }

                String key = parts[1];
                latestOps.put(key, parts);
                count++;
            }
            logger.info("Processed {} operations from WAL file: {}", count, walFile);
            logger.info("Returning {} unique key operations", latestOps.size());
        } catch (IOException e) {
            logger.error("Failed to read WAL file: {}", walFile, e);
        }

        return latestOps;
    }

    /** Delete the WAL file from disk. */
    public synchronized void clear() {
        try {
            if (Files.deleteIfExists(walFile)) {
                logger.info("WAL file cleared: {}", walFile);
            } else {
                logger.debug("WAL file didn't exist when attempting to clear: {}", walFile);
            }
        } catch (IOException e) {
            logger.error("Failed to clear WAL file: {}", walFile, e);
        }
    }

    /**
     * Change the WAL file path in a thread-safe way. Existing WAL contents are not migrated; this
     * simply points to a new file.
     */
    public synchronized void setWalFile(String fileName) {
        setWalFileInternal(fileName);
        logger.info("WALManager file set to: {}", fileName);
    }

    private void setWalFileInternal(String fileName) {
        this.walFile = Paths.get(fileName);
        try {
            ensureParentDirectoryExists();
        } catch (IOException e) {
            logger.error("Failed to create directories for WAL file: {}", fileName, e);
        }
    }

    private void ensureParentDirectoryExists() throws IOException {
        Path parent = walFile.getParent();
        if (parent != null && !Files.exists(parent)) {
            Files.createDirectories(parent);
        }
    }

    /**
     * For symmetry with other persistence components; currently a no-op. Callers may invoke this
     * during shutdown.
     */
    public void close() {
        // No long-lived resources to close; kept for future extensibility.
    }
}
