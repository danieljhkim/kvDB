package com.kvdb.kvdbserver.protocol;

import com.kvdb.kvcommon.protocol.CommandExecutor;

import java.util.HashMap;
import java.util.Map;

/**
 * Fake CommandExecutor for testing purposes
 */
public class FakeCommandExecutor implements CommandExecutor {
    
    private final Map<String, String> store = new HashMap<>();
    private String tableName = "test_table";
    private boolean healthy = true;
    
    @Override
    public String get(String key) {
        return store.get(key);
    }
    
    @Override
    public boolean put(String key, String value) {
        store.put(key, value);
        return true;
    }
    
    @Override
    public boolean delete(String key) {
        return store.remove(key) != null;
    }
    
    @Override
    public boolean exists(String key) {
        return store.containsKey(key);
    }
    
    @Override
    public int truncate() {
        int count = store.size();
        store.clear();
        return count;
    }
    
    @Override
    public String shutdown() {
        store.clear();
        return "OK: Shutdown completed.";
    }
    
    @Override
    public boolean isHealthy() {
        return healthy;
    }
    
    @Override
    public String getTableName() {
        return tableName;
    }
    
    @Override
    public void initialize(String name) {
        this.tableName = name;
    }
    
    public void setHealthy(boolean healthy) {
        this.healthy = healthy;
    }
    
    public int size() {
        return store.size();
    }
}
