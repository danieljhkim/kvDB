package com.kvdb.kvcommon.protocol;

/**
 * Common interface for all command executors (repository, GRPC client, etc.)
 */
public interface CommandExecutor {
    
    /**
     * Get a value by key
     */
    String get(String key);
    
    /**
     * Set a value for a key
     */
    boolean put(String key, String value);
    
    /**
     * Delete a key-value pair
     */
    boolean delete(String key);
    
    /**
     * Check if a key exists
     */
    default boolean exists(String key) {
        return get(key) != null;
    }
    
    /**
     * Remove all entries. Returns the number of entries removed.
     * Default implementation throws UnsupportedOperationException.
     * @return Number of entries removed, or -1 to signal 'not supported'
     * @throws UnsupportedOperationException if the operation is not supported
     */
    default int truncate() {
        throw new UnsupportedOperationException("truncate() is not supported by this executor");
    }
    
    /**
     * Shutdown the executor and release resources.
     * Default implementation throws UnsupportedOperationException.
     * @return Status message
     * @throws UnsupportedOperationException if the operation is not supported
     */
    default String shutdown() {
        throw new UnsupportedOperationException("shutdown() is not supported by this executor");
    }
    
    /**
     * Check if the executor is healthy and operational.
     * Default implementation performs a basic ping by checking if get(null) doesn't throw an exception.
     * @return true if healthy, false otherwise
     */
    default boolean isHealthy() {
        try {
            get(null);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
    
    /**
     * Get the name of the data source (table name, etc.)
     */
    default String getTableName() {
        return "default";
    }
    
    /**
     * Initialize with a specific name (e.g., table name)
     */
    default void initialize(String name) {
        // Default implementation does nothing
    }
}
