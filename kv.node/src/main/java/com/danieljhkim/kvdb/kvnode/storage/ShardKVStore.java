package com.danieljhkim.kvdb.kvnode.storage;

import com.danieljhkim.kvdb.kvcommon.annotations.Timer;
import com.danieljhkim.kvdb.kvcommon.persistence.WALManager;
import com.danieljhkim.kvdb.kvnode.persistence.FilePersistenceManager;
import com.danieljhkim.kvdb.kvnode.persistence.PersistenceManager;
import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A shard-scoped key-value store with its own snapshot + WAL.
 *
 * <p>
 * Persistence layout is controlled by the provided snapshot + WAL file paths.
 */
public class ShardKVStore {

    private static final Logger logger = LoggerFactory.getLogger(ShardKVStore.class);

    private static final String NIL_RESPONSE = "(nil)";

    private final String shardId;
    private final int flushInterval;
    private final boolean enableAutoFlush;

    private final AtomicInteger curFlushInterval = new AtomicInteger(0);
    private final ReentrantLock flushLock = new ReentrantLock();

    private final Map<String, String> store = new ConcurrentHashMap<>();
    private final PersistenceManager<Map<String, String>> persistenceManager;
    private final WALManager walManager;

    public ShardKVStore(
            String shardId, String snapshotFilePath, String walFilePath, int flushInterval, boolean enableAutoFlush) {
        this.shardId = Objects.requireNonNull(shardId, "shardId");
        this.flushInterval = flushInterval;
        this.enableAutoFlush = enableAutoFlush;

        TypeReference<Map<String, String>> typeRef = new TypeReference<>() {};
        this.persistenceManager = new FilePersistenceManager<>(snapshotFilePath, typeRef);
        this.walManager = new WALManager(walFilePath);

        loadFromDisk();
        recoverFromWal();

        logger.info(
                "ShardKVStore initialized shardId={}, autoFlushInterval={}, autoFlushEnabled={}",
                shardId,
                flushInterval,
                enableAutoFlush);
    }

    public String getShardId() {
        return shardId;
    }

    public boolean set(String key, String value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");

        walManager.log("SET", key, value);
        store.put(key, value);
        flushIfNeeded();
        return true;
    }

    public boolean del(String key) {
        Objects.requireNonNull(key, "key");

        walManager.log("DEL", key, null);
        boolean removed = (store.remove(key) != null);
        flushIfNeeded();
        return removed;
    }

    @Timer
    public String get(String key) {
        Objects.requireNonNull(key, "key");
        return store.getOrDefault(key, NIL_RESPONSE);
    }

    public int size() {
        return store.size();
    }

    public Map<String, String> snapshot() {
        return Collections.unmodifiableMap(store);
    }

    public Map<String, String> getMultiple(List<String> keys) {
        Objects.requireNonNull(keys, "keys");
        // Pre-size HashMap to avoid rehashing - account for load factor
        Map<String, String> result = new HashMap<>((int) (keys.size() / 0.75f) + 1);
        for (String key : keys) {
            String value = store.get(key);
            if (value != null) {
                result.put(key, value);
            }
        }
        return result;
    }

    @Timer
    private void loadFromDisk() {
        try {
            Map<String, String> loadedData = persistenceManager.load();
            if (loadedData == null) {
                return;
            }
            store.putAll(loadedData);
            logger.info("Loaded {} entries from disk for shardId={}", loadedData.size(), shardId);
        } catch (IOException e) {
            logger.error("Failed to load shard snapshot from disk for shardId={}", shardId, e);
        }
    }

    @Timer
    private void saveToDisk() {
        try {
            persistenceManager.save(store);
        } catch (IOException e) {
            logger.error("Failed to save shard snapshot to disk for shardId={}", shardId, e);
        }
    }

    @Timer
    private void recoverFromWal() {
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
                default -> logger.warn("Unknown WAL op={} for shardId={}", operation, shardId);
            }
        }
        if (!ops.isEmpty()) {
            logger.info("Replayed {} WAL operations during recovery for shardId={}", ops.size(), shardId);
        }
    }

    /**
     * Incremental auto-flush mechanism: every write increments a counter. Once it reaches flushInterval, we persist the
     * current state and clear the WAL.
     */
    public void flushIfNeeded() {
        if (!enableAutoFlush) {
            return;
        }

        int count = curFlushInterval.incrementAndGet();
        if (count < flushInterval) {
            return;
        }

        if (!flushLock.tryLock()) {
            return;
        }
        try {
            if (curFlushInterval.get() >= flushInterval) {
                saveToDisk();
                walManager.clear();
                curFlushInterval.set(0);
            }
        } finally {
            flushLock.unlock();
        }
    }

    /**
     * Force persistence immediately (snapshot + WAL clear), regardless of the auto-flush counter.
     */
    public void persistNow() {
        if (!flushLock.tryLock()) {
            return;
        }
        try {
            saveToDisk();
            walManager.clear();
            curFlushInterval.set(0);
        } finally {
            flushLock.unlock();
        }
    }

    public void shutdown() {
        saveToDisk();
        walManager.clear();
        try {
            persistenceManager.close();
        } catch (IOException e) {
            logger.warn("Error closing persistence manager for shardId={}", shardId, e);
        }
        try {
            walManager.close();
        } catch (Exception e) {
            logger.warn("Error closing WAL manager for shardId={}", shardId, e);
        }
    }
}
