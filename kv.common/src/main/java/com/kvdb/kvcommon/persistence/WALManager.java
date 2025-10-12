package com.kvdb.kvcommon.persistence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

// Write-Ahead Log (WAL)
public class WALManager {

    private static final Logger logger = LoggerFactory.getLogger(WALManager.class);
    private volatile boolean loggingEnabled = true;
    private final ReentrantLock accessLock = new ReentrantLock();
    private Path walFile;

    public WALManager(String fileName) {
        this.walFile = Paths.get(fileName);
        logger.info("WALManager initialized with file: {}", fileName);
    }

    public synchronized void log(String operation, String key, String value) {
        if (!loggingEnabled) {
            // in-case of concurrent access while logs are being replayed
            logger.debug("Logging operation rejected: logging is disabled");
            return;
        }
        try (BufferedWriter writer = Files.newBufferedWriter(walFile, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            writer.write(operation + " " + key + " " + (value != null ? value : "") + "\n");
            logger.debug("Operation logged: {} {} {}", operation, key, value);
        } catch (IOException e) {
            logger.error("Failed to log operation: {} {} {}", operation, key, value, e);
        }
    }

    public void setLoggingEnabled(boolean enabled) {
        accessLock.lock();
        try {
            this.loggingEnabled = enabled;
            logger.info("WAL logging {}", enabled ? "enabled" : "disabled");
        } finally {
            accessLock.unlock();
        }
    }

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
        } catch(IOException e) {
            logger.error("Failed to read WAL file: {}", walFile, e);
        }
        return ops;
    }

    public synchronized Map<String, String[]> replayAsMap() {
        // will return only the final state of each key
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
                if (parts.length < 2) continue;

                String operation = parts[0];
                String key = parts[1];
                latestOps.put(key, parts);
                count++;
            }
            logger.info("Processed {} operations from WAL file: {}", count, walFile);
            logger.info("Returning {} unique key operations", latestOps.size());
        } catch(IOException e) {
            logger.error("Failed to read WAL file: {}", walFile, e);
        }
        return latestOps;
    }

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

    public void setWalFile(String fileName) {
        this.walFile = Paths.get(fileName);
        logger.info("WALManager file set to: {}", fileName);
    }
}
