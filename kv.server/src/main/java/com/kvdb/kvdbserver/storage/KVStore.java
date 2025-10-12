package com.kvdb.kvdbserver.storage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.kvdb.kvcommon.config.SystemConfig;
import com.kvdb.kvcommon.annotations.Timer;
import com.kvdb.kvdbserver.persistence.FilePersistenceManager;
import com.kvdb.kvdbserver.persistence.PersistenceManager;
import com.kvdb.kvcommon.persistence.WALManager;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class KVStore implements KVStorageBase {

    private static final Logger LOGGER = Logger.getLogger(KVStore.class.getName());
    private static final SystemConfig config = SystemConfig.getInstance();
    private static final String OK_RESPONSE = "OK";
    private static final String NIL_RESPONSE = "(nil)";
    private static final KVStore INSTANCE = new KVStore();
    private static final int FLUSH_INTERVAL = Integer.parseInt(config.getProperty("kvdb.persistence.autoFlushInterval", "1000"));
    private static final boolean ENABLE_AUTO_FLUSH = Boolean.parseBoolean(config.getProperty("kvdb.persistence.enableAutoFlush", "true"));
    private static final String tableName = config.getProperty("kvdb.persistence.table.name", "default");
    private static int curFlushInterval = 0;
    private final Map<String, String> store = new ConcurrentHashMap<>();
    private final PersistenceManager<Map<String, String>> persistenceManager;
    private final WALManager walManager = new WALManager(config.getProperty("kvdb.persistence.wal.filepath", "data/wal.log"));

    private KVStore() {
        TypeReference<Map<String, String>> typeRef = new TypeReference<>() {};
        String persistenceFilePath = config.getProperty("kvdb.persistence.filepath");
        this.persistenceManager = new FilePersistenceManager<>(persistenceFilePath, typeRef);
        loadFromDisk();
        recover();
        System.out.println(FLUSH_INTERVAL);
    }

    public static synchronized KVStore getInstance() {
        return INSTANCE;
    }

    @Timer
    private void loadFromDisk() {
        try {
            Map<String, String> loadedData = persistenceManager.load();
            if (loadedData == null) {
                LOGGER.warning("Loaded data is null, initializing empty store");
                return;
            }
            store.putAll(loadedData);
            LOGGER.info("Loaded " + loadedData.size() + " entries from disk");
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to load data from disk", e);
        }
    }

    @Timer
    private void saveToDisk() {
        LOGGER.info("Saving data to disk");
        try {
            persistenceManager.save(store);
            curFlushInterval = 0;
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to save data to disk", e);
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
        flush();
        return store.remove(key) != null;
    }

    public int size() {
        return store.size();
    }

    public int clear() {
        LOGGER.info("Clearing the KVStore");
        int sizeBeforeClear = store.size();
        store.clear();
        walManager.clear();
        saveToDisk();
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
        Map<String, String> result = new HashMap<>();
        for (String key : keys) {
            if (store.containsKey(key)) {
                result.put(key, store.get(key));
            }
        }
        return result;
    }

    public void flush() {
        curFlushInterval++;
        if (ENABLE_AUTO_FLUSH && curFlushInterval >= FLUSH_INTERVAL) {
            LOGGER.info("Auto-flushing data to disk");
            saveToDisk();
            walManager.clear();
        }
    }

    @Override
    public void shutdown() {
        saveToDisk();
        try {
            persistenceManager.close();
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Error closing persistence manager", e);
        }
    }

    public void initialize(String tableName) {
        LOGGER.info("Initializing KVStore with table name: " + tableName);
        // TODO: implement multi-table support if needed
    }

    public void recover() {
        List<String[]> ops = walManager.replay();
        for (String[] op : ops) {
            String operation = op[0];
            String key = op[1];
            String value = op.length > 2 ? op[2] : null;

            switch (operation) {
                case "SET": store.put(key, value); break;
                case "DEL": store.remove(key); break;
                default: System.err.println("Unknown op: " + operation);
            }
        }
    }

    public String getTableName() {
        return tableName;
    }

}