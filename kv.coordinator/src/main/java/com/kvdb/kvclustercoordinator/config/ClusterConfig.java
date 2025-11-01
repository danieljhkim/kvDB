package com.kvdb.kvclustercoordinator.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.kvdb.kvclustercoordinator.cluster.ClusterNode;
import org.yaml.snakeyaml.Yaml;

/**
 * Configuration class for cluster nodes.
 * Loads and manages cluster node configurations from a YAML file.
 */
public class ClusterConfig {

    private static final Logger LOGGER = Logger.getLogger(ClusterConfig.class.getName());
    private final List<ClusterNode> nodes = new ArrayList<>();

    public ClusterConfig(String configFilePath) {
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(configFilePath)) {
            if (input == null) {
                throw new IllegalArgumentException("Cluster config file not found in classpath: " + configFilePath);
            }
            loadConfig(input);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to load cluster config from " + configFilePath, e);
            throw new RuntimeException("Failed to load cluster config", e);
        }
    }

    public ClusterConfig(File configFile) {
        try (InputStream input = new FileInputStream(configFile)) {
            loadConfig(input);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to load cluster config from file: " + configFile.getPath(), e);
            throw new RuntimeException("Failed to load cluster config", e);
        }
    }

    private void loadConfig(InputStream input) {
        try {
            Yaml yaml = new Yaml();
            Map<String, Object> yamlMap = yaml.load(input);

            if (yamlMap == null || !yamlMap.containsKey("nodes")) {
                throw new IllegalArgumentException("No 'nodes' section found in configuration");
            }

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> nodesList = (List<Map<String, Object>>) yamlMap.get("nodes");
            if (nodesList.isEmpty()) {
                throw new IllegalArgumentException("No cluster nodes found in configuration");
            }

            for (Map<String, Object> item : nodesList) {
                validateNodeConfig(item);
                String id = (String) item.get("id");
                String host = (String) item.get("host");
                int port = (Integer) item.get("port");
                boolean useGrpc = item.getOrDefault("useGrpc", true) instanceof Boolean &&
                        (Boolean) item.getOrDefault("useGrpc", true);
                nodes.add(new ClusterNode(id, host, port, useGrpc));
            }

            LOGGER.info("Loaded " + nodes.size() + " cluster nodes from configuration");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to parse cluster configuration", e);
            throw new RuntimeException("Failed to parse cluster configuration", e);
        }
    }
    
    private void validateNodeConfig(Map<String, Object> item) {
        if (!item.containsKey("id") || !(item.get("id") instanceof String)) {
            throw new IllegalArgumentException("Node configuration missing or invalid 'id' field");
        }
        if (!item.containsKey("host") || !(item.get("host") instanceof String)) {
            throw new IllegalArgumentException("Node configuration missing or invalid 'host' field");
        }
        if (!item.containsKey("port") || !(item.get("port") instanceof Integer)) {
            throw new IllegalArgumentException("Node configuration missing or invalid 'port' field");
        }
        
        int port = (Integer) item.get("port");
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("Port must be between 1 and 65535, got: " + port);
        }
    }

    public List<ClusterNode> getNodes() {
        return Collections.unmodifiableList(nodes);
    }
}
