package com.kvdb.kvdbserver.persistence;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FilePersistenceManager<T> implements PersistenceManager<T> {

    private static final Logger LOGGER = Logger.getLogger(FilePersistenceManager.class.getName());

    private final Path filePath;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final TypeReference<T> typeReference; // Retains type info for deserialization
    private final ObjectMapper objectMapper = new ObjectMapper();

    public FilePersistenceManager(String fileName, TypeReference<T> typeReference) {
        this.filePath = Paths.get(fileName);
        this.typeReference = typeReference;

        try {
            Path parent = filePath.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to create directory for persistence: " + filePath, e);
        }
    }

    @Override
    public void save(T data) throws IOException {
        lock.writeLock().lock();
        try {
            if (data == null) {
                LOGGER.log(Level.WARNING, "Data to save is null; skipping persistence");
                return;
            }

            Path parent = filePath.getParent();
            if (parent != null && !Files.exists(parent)) {
                Files.createDirectories(parent);
            }

            // Write to a temp file then move atomically to avoid partial writes
            Path tmpDir = (parent != null ? parent : Paths.get("."));
            Path tempFile = Files.createTempFile(tmpDir, "kvdb-", ".tmp");

            objectMapper.writeValue(tempFile.toFile(), data);

            Files.move(
                    tempFile,
                    filePath,
                    StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public T load() throws IOException {
        lock.readLock().lock();
        try {
            if (!Files.exists(filePath)) {
                LOGGER.log(Level.INFO, "Persistence file does not exist: {0}", filePath);
                return null;
            }

            T out = objectMapper.readValue(filePath.toFile(), typeReference);
            if (out == null) {
                LOGGER.log(Level.WARNING, "Loaded data is null from file: {0}", filePath);
            }
            return out;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void close() {
        // No long-lived resources to close; kept for interface symmetry and future extensibility.
        LOGGER.fine("FilePersistenceManager closed for file: " + filePath);
    }
}
