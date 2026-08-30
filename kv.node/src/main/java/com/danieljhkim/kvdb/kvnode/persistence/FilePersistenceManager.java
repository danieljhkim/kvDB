package com.danieljhkim.kvdb.kvnode.persistence;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilePersistenceManager<T> implements PersistenceManager<T> {

    enum FaultPoint {
        BEFORE_WRITE,
        BEFORE_FILE_SYNC,
        BEFORE_ATOMIC_MOVE,
        BEFORE_DIRECTORY_SYNC
    }

    @FunctionalInterface
    interface FaultInjector {
        FaultInjector NONE = point -> {};

        void trigger(FaultPoint point) throws IOException;
    }

    private static final Logger logger = LoggerFactory.getLogger(FilePersistenceManager.class);

    private final Path filePath;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final TypeReference<T> typeReference; // Retains type info for deserialization
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final FaultInjector faultInjector;

    public FilePersistenceManager(String fileName, TypeReference<T> typeReference) {
        this(fileName, typeReference, FaultInjector.NONE);
    }

    FilePersistenceManager(String fileName, TypeReference<T> typeReference, FaultInjector faultInjector) {
        this.filePath = Paths.get(fileName);
        this.typeReference = typeReference;
        this.faultInjector = faultInjector;

        try {
            Path parent = filePath.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
        } catch (IOException e) {
            logger.warn("Failed to create directory for persistence: {}", filePath, e);
        }
    }

    @Override
    public void save(T data) throws IOException {
        lock.writeLock().lock();
        Path tempFile = null;
        Path backupFile = null;
        Path snapshotDirectory = null;
        boolean replacementMade = false;
        boolean committed = false;
        try {
            if (data == null) {
                throw new IOException("Refusing to persist a null snapshot");
            }

            Path parent = filePath.getParent();
            if (parent != null && !Files.exists(parent)) {
                Files.createDirectories(parent);
            }

            Path tmpDir = (parent != null ? parent : Paths.get("."));
            snapshotDirectory = tmpDir.toAbsolutePath();
            tempFile = Files.createTempFile(tmpDir, "kvdb-", ".tmp");
            byte[] snapshot = objectMapper.writeValueAsBytes(data);
            faultInjector.trigger(FaultPoint.BEFORE_WRITE);
            try (FileChannel tempChannel = FileChannel.open(tempFile, StandardOpenOption.WRITE)) {
                ByteBuffer buffer = ByteBuffer.wrap(snapshot);
                while (buffer.hasRemaining()) {
                    tempChannel.write(buffer);
                }
                faultInjector.trigger(FaultPoint.BEFORE_FILE_SYNC);
                tempChannel.force(true);
            }

            faultInjector.trigger(FaultPoint.BEFORE_ATOMIC_MOVE);
            if (Files.exists(filePath)) {
                backupFile = Files.createTempFile(tmpDir, "kvdb-", ".previous");
                Files.copy(filePath, backupFile, StandardCopyOption.REPLACE_EXISTING);
                try (FileChannel backupChannel = FileChannel.open(backupFile, StandardOpenOption.WRITE)) {
                    backupChannel.force(true);
                }
            }
            Files.move(tempFile, filePath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            tempFile = null;
            replacementMade = true;
            faultInjector.trigger(FaultPoint.BEFORE_DIRECTORY_SYNC);
            forceDirectory(snapshotDirectory);
            committed = true;
        } catch (AtomicMoveNotSupportedException e) {
            throw new IOException("Atomic snapshot replacement is not supported for " + filePath, e);
        } catch (IOException e) {
            if (replacementMade && !committed) {
                try {
                    if (backupFile == null) {
                        Files.deleteIfExists(filePath);
                    } else {
                        Files.move(
                                backupFile,
                                filePath,
                                StandardCopyOption.REPLACE_EXISTING,
                                StandardCopyOption.ATOMIC_MOVE);
                        backupFile = null;
                    }
                    forceDirectory(snapshotDirectory);
                } catch (IOException rollbackError) {
                    e.addSuppressed(rollbackError);
                }
            }
            throw e;
        } finally {
            if (tempFile != null) {
                try {
                    Files.deleteIfExists(tempFile);
                } catch (IOException cleanupError) {
                    logger.warn("Failed to clean temporary snapshot {}", tempFile, cleanupError);
                }
            }
            if (backupFile != null) {
                try {
                    Files.deleteIfExists(backupFile);
                } catch (IOException cleanupError) {
                    logger.warn("Failed to clean previous snapshot backup {}", backupFile, cleanupError);
                }
            }
            lock.writeLock().unlock();
        }
    }

    @Override
    public T load() throws IOException {
        lock.readLock().lock();
        try {
            if (!Files.exists(filePath)) {
                logger.info("Persistence file does not exist: {}", filePath);
                return null;
            }

            T out = objectMapper.readValue(filePath.toFile(), typeReference);
            if (out == null) {
                logger.warn("Loaded data is null from file: {}", filePath);
            }
            return out;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void close() {
        // No long-lived resources to close; kept for interface symmetry and future
        // extensibility.
        logger.debug("FilePersistenceManager closed for file: {}", filePath);
    }

    private static void forceDirectory(Path directory) throws IOException {
        try (FileChannel directoryChannel = FileChannel.open(directory, StandardOpenOption.READ)) {
            directoryChannel.force(true);
        }
    }
}
