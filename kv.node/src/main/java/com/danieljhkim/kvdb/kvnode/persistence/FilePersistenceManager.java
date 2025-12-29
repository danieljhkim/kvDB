package com.danieljhkim.kvdb.kvnode.persistence;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilePersistenceManager<T> implements PersistenceManager<T> {

	private static final Logger logger = LoggerFactory.getLogger(FilePersistenceManager.class);

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
			logger.warn("Failed to create directory for persistence: {}", filePath, e);
		}
	}

	@Override
	public void save(T data) throws IOException {
		lock.writeLock().lock();
		try {
			if (data == null) {
				logger.warn("Data to save is null; skipping persistence");
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
}
