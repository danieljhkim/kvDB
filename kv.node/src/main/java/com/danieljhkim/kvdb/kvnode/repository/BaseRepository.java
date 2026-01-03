package com.danieljhkim.kvdb.kvnode.repository;

import java.util.List;
import java.util.Map;

public interface BaseRepository {
    String get(String key);

    boolean put(String key, String value);

    boolean update(String key, String value);

    boolean delete(String key);

    boolean exists(String key);

    List<String> getAllKeys();

    Map<String, String> getMultiple(List<String> keys);

    int truncate();

    boolean isHealthy();

    void initialize(String tableName);

    String getTableName();

    void shutdown();
}
