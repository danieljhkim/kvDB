package com.danieljhkim.kvdb.kvcommon.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.danieljhkim.kvdb.kvcommon.grpc.GrpcIdentity.Role;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class GrpcSecurityConfigTest {

    @Test
    void plaintextDowngradeFailsClosedOutsideExplicitDevelopment() {
        Map<String, String> environment = new HashMap<>();
        environment.put("KVDB_GRPC_SECURITY_MODE", "development-plaintext");
        environment.put("KVDB_ENV", "production");

        assertThrows(IllegalStateException.class, () -> GrpcSecurityConfig.internal(Role.GATEWAY, environment));
    }

    @Test
    void plaintextDevelopmentRequiresExplicitDeploymentAndRetainsRole() {
        Map<String, String> environment = new HashMap<>();
        environment.put("KVDB_GRPC_SECURITY_MODE", "development-plaintext");
        environment.put("KVDB_ENV", "local");
        environment.put("KVDB_IDENTITY_PRINCIPAL", "gateway-local");

        GrpcSecurityConfig config = GrpcSecurityConfig.internal(Role.GATEWAY, environment);

        assertEquals(GrpcSecurityConfig.Mode.DEVELOPMENT_PLAINTEXT, config.mode());
        assertEquals(Role.GATEWAY, config.localRole());
        assertEquals("127.0.0.1", config.bindAddress().getHostAddress());
    }

    @Test
    void mtlsRejectsMissingTrustAndIdentityMaterial() {
        Map<String, String> environment = Map.of(
                "KVDB_GRPC_SECURITY_MODE", "mtls",
                "KVDB_IDENTITY_ROLE", "gateway",
                "KVDB_IDENTITY_PRINCIPAL", "gateway-1");

        assertThrows(IllegalStateException.class, () -> GrpcSecurityConfig.internal(Role.GATEWAY, environment));
    }

    @Test
    void plaintextBindOverrideAllowsContainerWildcardAndIpv6Loopback() throws Exception {
        Map<String, String> environment = new HashMap<>(Map.of(
                "KVDB_GRPC_SECURITY_MODE", "development-plaintext",
                "KVDB_ENV", "test",
                "KVDB_BIND_ADDRESS", " 0.0.0.0 "));
        assertTrue(GrpcSecurityConfig.internal(Role.COORDINATOR, environment)
                .bindAddress()
                .isAnyLocalAddress());
        environment.put("KVDB_BIND_ADDRESS", "::1");
        assertEquals(
                InetAddress.getByName("::1"),
                GrpcSecurityConfig.internal(Role.GATEWAY, environment).bindAddress());
        environment.put("KVDB_BIND_ADDRESS", "localhost");
        assertTrue(GrpcSecurityConfig.internal(Role.STORAGE_NODE, environment)
                .bindAddress()
                .isLoopbackAddress());
    }

    @Test
    void blankBindOverrideKeepsLoopbackAndInvalidOverrideFailsClosed() {
        Map<String, String> environment = new HashMap<>(Map.of(
                "KVDB_GRPC_SECURITY_MODE", "development-plaintext",
                "KVDB_ENV", "local",
                "KVDB_BIND_ADDRESS", "  "));
        assertEquals(
                "127.0.0.1",
                GrpcSecurityConfig.internal(Role.STORAGE_NODE, environment)
                        .bindAddress()
                        .getHostAddress());
        environment.put("KVDB_BIND_ADDRESS", "invalid address");
        IllegalStateException error = assertThrows(
                IllegalStateException.class, () -> GrpcSecurityConfig.internal(Role.STORAGE_NODE, environment));
        assertTrue(error.getMessage().contains("KVDB_BIND_ADDRESS"));
    }

    @Test
    void mtlsRetainsWildcardDefaultAndHonorsOverride(@TempDir Path directory) throws Exception {
        Path credential = Files.writeString(directory.resolve("credential"), "configuration test only");
        Map<String, String> environment = new HashMap<>(Map.of(
                "KVDB_IDENTITY_ROLE", "gateway",
                "KVDB_IDENTITY_PRINCIPAL", "gateway-1",
                "KVDB_INTERNAL_TLS_CERT_CHAIN", credential.toString(),
                "KVDB_INTERNAL_TLS_PRIVATE_KEY", credential.toString(),
                "KVDB_INTERNAL_TLS_TRUST_BUNDLE", credential.toString()));
        assertTrue(GrpcSecurityConfig.internal(Role.GATEWAY, environment)
                .bindAddress()
                .isAnyLocalAddress());
        environment.put("KVDB_BIND_ADDRESS", "127.0.0.1");
        assertTrue(GrpcSecurityConfig.internal(Role.GATEWAY, environment)
                .bindAddress()
                .isLoopbackAddress());
    }
}
