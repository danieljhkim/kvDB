package com.danieljhkim.kvdb.kvnode.storage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.danieljhkim.kvdb.kvcommon.annotations.Timer;
import com.danieljhkim.kvdb.kvcommon.config.SystemConfig;
import com.danieljhkim.kvdb.kvcommon.persistence.WALManager;
import com.danieljhkim.kvdb.kvnode.persistence.FilePersistenceManager;
import com.danieljhkim.kvdb.kvnode.persistence.PersistenceManager;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KVStore implements KVStorageBase {

	private static final Logger logger = LoggerFactory.getLogger(KVStore.class);
	private static final SystemConfig CONFIG = SystemConfig.getInstance();

	private static final String OK_RESPONSE = "OK";
	private static final String NIL_RESPONSE = "(nil)";

	private static final int FLUSH_INTERVAL = Integer
			.parseInt(CONFIG.getProperty("kvdb.persistence.autoFlushInterval", "1000"));
	private static final boolean ENABLE_AUTO_FLUSH = Boolean
			.parseBoolean(CONFIG.getProperty("kvdb.persistence.enableAutoFlush", "true"));
	private static final String TABLE_NAME = CONFIG.getProperty("kvdb.persistence.table.name", "default");

	/** Singleton instance. */
	private static final KVStore INSTANCE = new KVStore();

	/** Counts the number of write ops since last flush. */
	private final AtomicInteger curFlushInterval = new AtomicInteger(0);

	/** Guard to ensure only one flush/saveToDisk runs at a time. */
	private final ReentrantLock flushLock = new ReentrantLock();

	private final Map<String, String> store = new ConcurrentHashMap<>();
	private final PersistenceManager<Map<String, String>> persistenceManager;
	private final WALManager walManager = new WALManager(
			CONFIG.getProperty("kvdb.persistence.wal.filepath", "data/wal.log"));

	private KVStore() {
		TypeReference<Map<String, String>> typeRef = new TypeReference<>() {
		};
		String persistenceFilePath = CONFIG.getProperty("kvdb.persistence.filepath");

		this.persistenceManager = new FilePersistenceManager<>(persistenceFilePath, typeRef);

		loadFromDisk();
		recover();

		logger.info("KVStore initialized. Auto-flush interval: {}, auto-flush enabled: {}", FLUSH_INTERVAL,
				ENABLE_AUTO_FLUSH);
	}

	public static KVStore getInstance() {
		return INSTANCE;
	}

	@Timer
	private void loadFromDisk() {
		try {
			Map<String, String> loadedData = persistenceManager.load();
			if (loadedData == null) {
				logger.warn("Loaded data is null, initializing empty store");
				return;
			}
			store.putAll(loadedData);
			logger.info("Loaded {} entries from disk", loadedData.size());
		} catch (IOException e) {
			logger.error("Failed to load data from disk", e);
		}
	}

	@Timer
	private void saveToDisk() {
		logger.info("Saving data to disk");
		try {
			persistenceManager.save(store);
		} catch (IOException e) {
			logger.error("Failed to save data to disk", e);
		}
	}

	public boolean set(String key, String value) {
		Objects.requireNonNull(key, "Key cannot be null");
		Objects.requireNonNull(value, "Value cannot be null");

		walManager.log("SET", key, value);
		store.put(key, value);
		flush();
		return true;
	}

	@Timer
	public String get(String key) {
		Objects.requireNonNull(key, "Key cannot be null");
		return store.getOrDefault(key, NIL_RESPONSE);
	}

	public boolean del(String key) {
		Objects.requireNonNull(key, "Key cannot be null");

		walManager.log("DEL", key, null);
		boolean removed = (store.remove(key) != null);
		flush();
		return removed;
	}

	public int size() {
		return store.size();
	}

	public int clear() {
		logger.info("Clearing the KVStore");
		int sizeBeforeClear = store.size();
		store.clear();
		walManager.clear();
		saveToDisk();
		curFlushInterval.set(0);
		return sizeBeforeClear;
	}

	public boolean exists(String key) {
		Objects.requireNonNull(key, "Key cannot be null");
		return store.containsKey(key);
	}

	public Map<String, String> getAll() {
		return Collections.unmodifiableMap(store);
	}

	public List<String> getAllKeys() {
		return List.copyOf(store.keySet());
	}

	public Map<String, String> getMultiple(List<String> keys) {
		Objects.requireNonNull(keys, "Keys cannot be null");
		Map<String, String> result = new HashMap<>(keys.size());
		for (String key : keys) {
			String value = store.get(key);
			if (value != null) {
				result.put(key, value);
			}
		}
		return result;
	}

	/**
	 * Incremental auto-flush mechanism: every write increments a counter. Once it
	 * reaches
	 * FLUSH_INTERVAL, we persist the current state and clear the WAL.
	 */
	public void flush() {
		if (!ENABLE_AUTO_FLUSH) {
			return;
		}

		int count = curFlushInterval.incrementAndGet();
		if (count < FLUSH_INTERVAL) {
			return;
		}

		if (!flushLock.tryLock()) {
			// Another thread is already flushing
			return;
		}
		try {
			// Re-check under the lock
			if (curFlushInterval.get() >= FLUSH_INTERVAL) {
				logger.info("Auto-flushing data to disk");
				saveToDisk();
				walManager.clear();
				curFlushInterval.set(0);
			}
		} finally {
			flushLock.unlock();
		}
	}

	@Override
	public void shutdown() {
		logger.info("Shutting down KVStore");
		// Final flush
		saveToDisk();
		walManager.clear();

		try {
			persistenceManager.close();
		} catch (IOException e) {
			logger.warn("Error closing persistence manager", e);
		}

		// If WALManager has a close(), call it here
		try {
			walManager.close();
		} catch (Exception e) {
			logger.warn("Error closing WAL manager", e);
		}
	}

	public void initialize(String tableName) {
		logger.info("Initializing KVStore with table name: {}", tableName);
		// TODO: implement multi-table support if needed
	}

	/**
	 * Replays WAL entries into the in-memory store. Called after loading the base
	 * snapshot from
	 * disk.
	 */
	public void recover() {
		List<String[]> ops = walManager.replay();
		for (String[] op : ops) {
			if (op.length < 2) {
				continue;
			}
			String operation = op[0];
			String key = op[1];
			String value = op.length > 2 ? op[2] : null;

			switch (operation) {
				case "SET" -> store.put(key, value);
				case "DEL" -> store.remove(key);
				default -> logger.warn("Unknown WAL op: {}", operation);
			}
		}
		if (!ops.isEmpty()) {
			logger.info("Replayed {} WAL operations during recovery", ops.size());
		}
	}

	public String getTableName() {
		return TABLE_NAME;
	}
}
