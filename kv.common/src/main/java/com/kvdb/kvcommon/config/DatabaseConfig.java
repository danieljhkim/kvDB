package com.kvdb.kvcommon.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DatabaseConfig {

    private static final Logger LOGGER = Logger.getLogger(DatabaseConfig.class.getName());
    private static final String DEFAULT_DB = "default";
    private static final String DB_PREFIX = "kvdb.database.";
    private static DatabaseConfig INSTANCE;

    private final SystemConfig systemConfig;
    private final Map<String, HikariDataSource> dataSources;
    private final Set<String> availableDatabases;

    private DatabaseConfig(String dbName) {
        this.systemConfig = SystemConfig.getInstance();
        this.dataSources = new ConcurrentHashMap<>();
        this.availableDatabases = findAvailableDatabases();
        if (availableDatabases.contains(dbName)) {
            initializeDataSource(dbName);
        } else {
            LOGGER.warning("No database configuration found for: " + dbName);
            throw new IllegalArgumentException("No database configuration found for: " + dbName);
        }
    }

    public static synchronized DatabaseConfig getInstance() {
        return getInstance(DEFAULT_DB);
    }

    public static synchronized DatabaseConfig getInstance(String dbName) {
        if (INSTANCE == null) {
            INSTANCE = new DatabaseConfig(dbName);
        }
        return INSTANCE;
    }

    public static void shutdown() {
        if (INSTANCE != null) {
            for (String dbName : INSTANCE.dataSources.keySet()) {
                INSTANCE.closeDataSource(dbName);
            }
            INSTANCE = null;
        }
    }

    private Set<String> findAvailableDatabases() {
        Set<String> databases = new HashSet<>();
        for (String propName : systemConfig.getAllPropertyNames()) {
            if (propName.startsWith(DB_PREFIX) && propName.contains(".url")) {
                String dbName =
                        propName.substring(
                                DB_PREFIX.length(), propName.length() - 4); // remove ".url"
                databases.add(dbName);
                LOGGER.info("Found database configuration for: " + dbName);
            }
        }
        return Collections.unmodifiableSet(databases);
    }

    private void initializeDataSource(String dbName) {
        if (dataSources.containsKey(dbName)) {
            return;
        }
        try {
            String prefix = DB_PREFIX + dbName + ".";
            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(systemConfig.getProperty(prefix + "url"));
            config.setUsername(systemConfig.getProperty(prefix + "username"));
            config.setPassword(systemConfig.getProperty(prefix + "password"));
            config.setDriverClassName(systemConfig.getProperty(prefix + "driver"));

            config.setMaximumPoolSize(
                    Integer.parseInt(systemConfig.getProperty(prefix + "pool.maxSize", "10")));
            config.setMinimumIdle(
                    Integer.parseInt(systemConfig.getProperty(prefix + "pool.minIdle", "5")));
            config.setIdleTimeout(
                    Long.parseLong(systemConfig.getProperty(prefix + "pool.idleTimeout", "30000")));
            config.setConnectionTimeout(
                    Long.parseLong(
                            systemConfig.getProperty(prefix + "pool.connectionTimeout", "30000")));

            HikariDataSource dataSource = new HikariDataSource(config);
            dataSources.put(dbName, dataSource);
            LOGGER.info("Database connection pool initialized for: " + dbName);
        } catch (Exception e) {
            LOGGER.log(
                    Level.SEVERE,
                    "Failed to initialize database connection pool for: " + dbName,
                    e);
            throw new RuntimeException("Database configuration error for " + dbName, e);
        }
    }

    private void initializeDataSource() {
        initializeDataSource(DEFAULT_DB);
    }

    public Connection getConnection() throws SQLException {
        return getConnection(DEFAULT_DB);
    }

    public Connection getConnection(String dbName) throws SQLException {
        if (!availableDatabases.contains(dbName)) {
            throw new SQLException("No database configuration found for: " + dbName);
        }

        if (!dataSources.containsKey(dbName)) {
            initializeDataSource(dbName);
        }

        try {
            return dataSources.get(dbName).getConnection();
        } catch (SQLException e) {
            LOGGER.log(
                    Level.WARNING,
                    "Failed to get connection for " + dbName + ", attempting to reinitialize",
                    e);
            reinitializeDataSource(dbName);
            return dataSources.get(dbName).getConnection();
        }
    }

    public void reinitializeDataSource(String dbName) {
        LOGGER.info("Reinitializing database connection pool for: " + dbName);
        closeDataSource(dbName);
        initializeDataSource(dbName);
    }

    public boolean isHealthy(String dbName) {
        if (!dataSources.containsKey(dbName)) {
            return false;
        }

        try (Connection conn = dataSources.get(dbName).getConnection()) {
            return conn.isValid(2);
        } catch (SQLException e) {
            LOGGER.log(Level.WARNING, "Database health check failed for: " + dbName, e);
            return false;
        }
    }

    public void closeDataSource(String dbName) {
        HikariDataSource dataSource = dataSources.remove(dbName);
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
            LOGGER.info("Database connection pool closed for: " + dbName);
        }
    }

    public String getDefaultTableName(String dbName) {
        String tableName = systemConfig.getProperty("database." + dbName + ".table");
        return tableName != null ? tableName : "kv_store";
    }

    public Set<String> getAvailableDatabases() {
        return availableDatabases;
    }
}
