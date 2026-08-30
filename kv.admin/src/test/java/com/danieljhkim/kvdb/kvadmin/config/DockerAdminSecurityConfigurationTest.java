package com.danieljhkim.kvdb.kvadmin.config;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.danieljhkim.kvdb.kvadmin.security.AdminApiKeyFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class DockerAdminSecurityConfigurationTest {

    @Test
    void dockerDeploymentKeepsAdminPrivateAndRequiresAnApiKey() throws IOException {
        String compose = Files.readString(Path.of("..", "docker-compose.yml"));
        String dockerProperties =
                Files.readString(Path.of("src", "main", "resources", "application-docker.properties"));

        assertTrue(compose.contains("127.0.0.1:8089:8089"));
        assertTrue(compose.contains(
                "KVDB_ADMIN_SECURITY_API_KEY: ${KVDB_ADMIN_SECURITY_API_KEY:?Set KVDB_ADMIN_SECURITY_API_KEY}"));
        assertFalse(compose.contains("KVDB_ADMIN_SECURITY_ENABLED=false"));
        assertFalse(dockerProperties.contains("kvdb.admin.security.enabled=false"));
        assertTrue(dockerProperties.contains("kvdb.admin.security.api-key=${KVDB_ADMIN_SECURITY_API_KEY}"));
    }

    @Test
    void securityConfigurationRejectsBlankKeysAndRegistersTheApiKeyFilter() {
        SecurityConfig config = new SecurityConfig();
        config.setApiKey(" ");

        assertThrows(IllegalArgumentException.class, config::adminApiKeyFilter);

        config.setApiKey("test-api-key");
        assertInstanceOf(AdminApiKeyFilter.class, config.adminApiKeyFilter().getFilter());
    }
}
