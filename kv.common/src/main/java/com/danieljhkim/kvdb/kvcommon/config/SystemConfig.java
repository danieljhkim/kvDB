package com.danieljhkim.kvdb.kvcommon.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SystemConfig {

    private static final Logger logger = LoggerFactory.getLogger(SystemConfig.class);
    private static final String DEFAULT_CONFIG_FILE = "application.properties";

    private static SystemConfig INSTANCE;
    private final Properties properties;
    private String resourcePath = "";

    private SystemConfig() {
        this.properties = new Properties();
        loadDefaultConfigFile();
        loadEnvSpecificConfigFile();
    }

    private SystemConfig(String resourcePath) {
        this.resourcePath = resourcePath;
        this.properties = new Properties();
        loadDefaultConfigFile();
        loadEnvSpecificConfigFile();
    }

    public static synchronized SystemConfig getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new SystemConfig();
        }
        return INSTANCE;
    }

    public static synchronized SystemConfig getInstance(String resourcePath) {
        if (INSTANCE == null) {
            INSTANCE = new SystemConfig(resourcePath);
        }
        return INSTANCE;
    }

    private void loadDefaultConfigFile() {
        Path path = Paths.get(resourcePath, DEFAULT_CONFIG_FILE);
        if (Files.exists(path)) {
            try (InputStream input = new FileInputStream(path.toFile())) {
                properties.load(input);
                logger.info("Loaded default configuration from filesystem: {}", path);
                return;
            } catch (IOException e) {
                logger.warn("Failed to load default configuration from filesystem", e);
            }
        }
        try (InputStream input =
                getClass().getClassLoader().getResourceAsStream(resourcePath + "/" + DEFAULT_CONFIG_FILE)) {
            if (input != null) {
                properties.load(input);
                logger.info("Loaded default configuration from classpath: {}/{}", resourcePath, DEFAULT_CONFIG_FILE);
            } else {
                logger.warn(
                        "Default configuration file not found in classpath: {}/{}", resourcePath, DEFAULT_CONFIG_FILE);
            }
        } catch (IOException e) {
            logger.warn("Failed to load default configuration from classpath", e);
        }
    }

    private void loadEnvSpecificConfigFile() {
        String env = System.getProperty("kvdb.env");
        if (env != null && !env.isEmpty()) {
            String envConfigFile = resourcePath + "/application-" + env + ".properties";
            Path envPath = Paths.get(envConfigFile);

            if (Files.exists(envPath)) {
                try (InputStream input = new FileInputStream(envConfigFile)) {
                    properties.load(input);
                    logger.info("Loaded environment-specific configuration from: {}", envConfigFile);
                } catch (IOException e) {
                    logger.warn("Failed to load environment-specific configuration file: {}", envConfigFile, e);
                }
            } else {
                logger.warn("Environment-specific configuration file not found: {}", envConfigFile);
            }
            try (InputStream input = getClass().getClassLoader().getResourceAsStream(envConfigFile)) {
                if (input != null) {
                    properties.load(input);
                    logger.info("Loaded env configuration from classpath: {}", envConfigFile);
                }
            } catch (IOException e) {
                logger.warn("Failed to load env configuration from classpath", e);
            }
        }
    }

    public String getProperty(String key, String defaultValue) {
        String value = System.getProperty("kvdb." + key);
        if (value != null && !value.isEmpty()) {
            return value;
        }
        String envKey = "KVDB_" + key.toUpperCase().replace('.', '_');
        value = System.getenv(envKey);
        if (value != null && !value.isEmpty()) {
            return value;
        }
        return properties.getProperty(key, defaultValue);
    }

    public String getProperty(String key) {
        return getProperty(key, null);
    }

    public Set<String> getAllPropertyNames(String prefix) {
        Set<String> propertyNames = new HashSet<>();
        for (Object key : properties.keySet()) {
            propertyNames.add(key.toString());
        }
        if (prefix == null || prefix.isEmpty()) {
            return propertyNames;
        }
        Set<String> filteredProperties = new HashSet<>();
        for (String propName : propertyNames) {
            if (propName.startsWith(prefix)) {
                filteredProperties.add(propName);
            }
        }
        return Collections.unmodifiableSet(filteredProperties);
    }

    public Set<String> getAllPropertyNames() {
        return getAllPropertyNames(null);
    }
}
