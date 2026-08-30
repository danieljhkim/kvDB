package com.danieljhkim.kvdb.kvnode.storage;

import com.danieljhkim.kvdb.kvcommon.annotations.Timer;
import com.danieljhkim.kvdb.kvcommon.observability.Metrics;
import com.danieljhkim.kvdb.kvcommon.persistence.WALManager;
import com.danieljhkim.kvdb.kvnode.persistence.FilePersistenceManager;
import com.danieljhkim.kvdb.kvnode.persistence.PersistenceManager;
import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.io.UncheckedIOException;
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
    /** Serializes the WAL append, in-memory mutation, and snapshot/WAL cut. */
    private final ReentrantLock stateLock = new ReentrantLock();

    private final Map<String, String> store = new ConcurrentHashMap<>();
    private final PersistenceManager<Map<String, String>> persistenceManager;
    private final WALManager walManager;

    public ShardKVStore(
            String shardId, String snapshotFilePath, String walFilePath, int flushInterval, boolean enableAutoFlush) {
        this(
                shardId,
                flushInterval,
                enableAutoFlush,
                new FilePersistenceManager<>(snapshotFilePath, new TypeReference<Map<String, String>>() {}),
                new WALManager(walFilePath));
    }

    ShardKVStore(
            String shardId,
            int flushInterval,
            boolean enableAutoFlush,
            PersistenceManager<Map<String, String>> persistenceManager,
            WALManager walManager) {
        this.shardId = Objects.requireNonNull(shardId, "shardId");
        this.flushInterval = flushInterval;
        this.enableAutoFlush = enableAutoFlush;
        this.persistenceManager = Objects.requireNonNull(persistenceManager, "persistenceManager");
        this.walManager = Objects.requireNonNull(walManager, "walManager");

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

        stateLock.lock();
        try {
            walManager.log("SET", key, value);
            store.put(key, value);
            flushIfNeededLocked();
            return true;
        } finally {
            stateLock.unlock();
        }
    }

    public boolean del(String key) {
        Objects.requireNonNull(key, "key");

        stateLock.lock();
        try {
            walManager.log("DEL", key, null);
            boolean removed = (store.remove(key) != null);
            flushIfNeededLocked();
            return removed;
        } finally {
            stateLock.unlock();
        }
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
        stateLock.lock();
        try {
            return Collections.unmodifiableMap(new HashMap<>(store));
        } finally {
            stateLock.unlock();
        }
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
            Metrics.increment("kvdb_snapshot_failures_total", "node", "load", "error");
            throw new UncheckedIOException("Failed to load shard snapshot for shardId=" + shardId, e);
        }
    }

    @Timer
    private void saveToDisk() {
        try {
            persistenceManager.save(new HashMap<>(store));
        } catch (IOException e) {
            Metrics.increment("kvdb_snapshot_failures_total", "node", "save", "error");
            throw new UncheckedIOException("Failed to save shard snapshot for shardId=" + shardId, e);
        }
    }

    @Timer
    private void recoverFromWal() {
        List<String[]> ops = walManager.replay();
        for (String[] op : ops) {
            if (op.length != 3) {
                throw new WALManager.WALCorruptionException("Malformed WAL operation for shardId=" + shardId);
            }
            String operation = op[0];
            String key = op[1];
            String value = op.length > 2 ? op[2] : null;

            switch (operation) {
                case "SET" -> store.put(key, value);
                case "DEL" -> store.remove(key);
                default -> throw new WALManager.WALCorruptionException(
                        "Unknown WAL operation " + operation + " for shardId=" + shardId);
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
        stateLock.lock();
        try {
            flushIfNeededLocked();
        } finally {
            stateLock.unlock();
        }
    }

    /**
     * Force persistence immediately (snapshot + WAL clear), regardless of the auto-flush counter.
     */
    public void persistNow() {
        stateLock.lock();
        try {
            snapshotAndRotateWal();
        } finally {
            stateLock.unlock();
        }
    }

    public void shutdown() {
        stateLock.lock();
        RuntimeException failure = null;
        try {
            try {
                snapshotAndRotateWal();
            } catch (RuntimeException e) {
                failure = e;
            }
            try {
                persistenceManager.close();
            } catch (IOException e) {
                RuntimeException closeFailure =
                        new UncheckedIOException("Failed to close persistence manager for shardId=" + shardId, e);
                failure = combineFailures(failure, closeFailure);
            }
            try {
                walManager.close();
            } catch (RuntimeException e) {
                failure = combineFailures(failure, e);
            }
        } finally {
            stateLock.unlock();
        }
        if (failure != null) {
            throw failure;
        }
    }

    public WALManager.Durability acknowledgedDurability() {
        return walManager.durability();
    }

    private void flushIfNeededLocked() {
        if (!enableAutoFlush) {
            return;
        }
        if (curFlushInterval.incrementAndGet() >= flushInterval) {
            snapshotAndRotateWal();
        }
    }

    private void snapshotAndRotateWal() {
        saveToDisk();
        walManager.clear();
        curFlushInterval.set(0);
    }

    private static RuntimeException combineFailures(RuntimeException primary, RuntimeException additional) {
        if (primary == null) {
            return additional;
        }
        primary.addSuppressed(additional);
        return primary;
    }
}
