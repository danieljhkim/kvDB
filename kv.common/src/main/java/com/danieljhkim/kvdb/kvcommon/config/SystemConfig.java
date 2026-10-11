package com.danieljhkim.kvdb.kvcommon.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SystemConfig {

    private static final Logger logger = LoggerFactory.getLogger(SystemConfig.class);
    private static final String DEFAULT_CONFIG_FILE = "application.properties";

    private static SystemConfig INSTANCE;
    private final Properties properties;
    private final ClassLoader classLoader;
    private final Function<String, String> environment;
    private String resourcePath = "";

    private SystemConfig() {
        this("");
    }

    private SystemConfig(String resourcePath) {
        this(resourcePath, SystemConfig.class.getClassLoader());
    }

    /** Package-private so tests can supply the classloader that serves classpath resources. */
    SystemConfig(String resourcePath, ClassLoader classLoader) {
        this(resourcePath, classLoader, System::getenv);
    }

    /** Package-private so tests can isolate environment overrides without changing the process environment. */
    SystemConfig(String resourcePath, ClassLoader classLoader, Function<String, String> environment) {
        this.resourcePath = resourcePath;
        this.classLoader = classLoader;
        this.environment = environment;
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
        String resource = resourceName(DEFAULT_CONFIG_FILE);
        try (InputStream input = classLoader.getResourceAsStream(resource)) {
            if (input != null) {
                properties.load(input);
                logger.info("Loaded default configuration from classpath: {}", resource);
            } else {
                logger.warn("Default configuration file not found in classpath: {}", resource);
            }
        } catch (IOException e) {
            logger.warn("Failed to load default configuration from classpath", e);
        }
    }

    private void loadEnvSpecificConfigFile() {
        String env = System.getProperty("kvdb.env");
        if (env != null && !env.isEmpty()) {
            String envFileName = "application-" + env + ".properties";
            Path envPath = Paths.get(resourcePath, envFileName);

            if (Files.exists(envPath)) {
                try (InputStream input = new FileInputStream(envPath.toFile())) {
                    properties.load(input);
                    logger.info("Loaded environment-specific configuration from: {}", envPath);
                } catch (IOException e) {
                    logger.warn("Failed to load environment-specific configuration file: {}", envPath, e);
                }
            } else {
                logger.warn("Environment-specific configuration file not found: {}", envPath);
            }
            String resource = resourceName(envFileName);
            try (InputStream input = classLoader.getResourceAsStream(resource)) {
                if (input != null) {
                    properties.load(input);
                    logger.info("Loaded env configuration from classpath: {}", resource);
                }
            } catch (IOException e) {
                logger.warn("Failed to load env configuration from classpath", e);
            }
        }
    }

    /** Classpath names are relative to the root, so an empty resourcePath must not produce a leading slash. */
    private String resourceName(String fileName) {
        return resourcePath.isEmpty() ? fileName : resourcePath + "/" + fileName;
    }

    public String getProperty(String key, String defaultValue) {
        String qualifiedKey = key.startsWith("kvdb.") ? key : "kvdb." + key;
        String value = System.getProperty(qualifiedKey);
        if (value != null && !value.isEmpty()) {
            return value;
        }
        String envKey = qualifiedKey.toUpperCase(Locale.ROOT).replace('.', '_');
        value = environment.apply(envKey);
        if (value != null && !value.isEmpty()) {
            return value;
        }
        return properties.getProperty(qualifiedKey, properties.getProperty(key, defaultValue));
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
