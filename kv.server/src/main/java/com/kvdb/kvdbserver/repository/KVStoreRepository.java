package com.kvdb.kvdbserver.repository;

import com.kvdb.kvdbserver.storage.KVStore;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class KVStoreRepository implements BaseRepository {

    private static final Logger LOGGER = Logger.getLogger(KVStoreRepository.class.getName());

    private final KVStore store = KVStore.getInstance();

    public KVStoreRepository() {}

    public String get(String key) {
        return store.get(key);
    }

    public boolean delete(String key) {
        return store.del(key);
    }

    public boolean update(String key, String value) {
        return store.set(key, value);
    }

    public boolean put(String key, String value) {
        return store.set(key, value);
    }

    public boolean exists(String key) {
        return store.get(key) != null;
    }

    public List<String> getAllKeys() {
        return store.getAllKeys();
    }

    public Map<String, String> getMultiple(List<String> keys) {
        return store.getMultiple(keys);
    }

    public boolean isHealthy() {
        return true;
    }

    public void initialize(String tableName) {
        LOGGER.info("Initializing KVStore with table name: " + tableName);
    }

    public int truncate() {
        return store.clear();
    }

    public String getTableName() {
        return store.getTableName();
    }

    public void shutdown() {
        store.shutdown();
        LOGGER.info("KVStoreRepository shutdown complete.");
    }
}
