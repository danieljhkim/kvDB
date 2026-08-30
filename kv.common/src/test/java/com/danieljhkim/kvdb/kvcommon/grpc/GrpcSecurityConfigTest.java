package com.danieljhkim.kvdb.kvcommon.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.danieljhkim.kvdb.kvcommon.grpc.GrpcIdentity.Role;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

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
    }

    @Test
    void mtlsRejectsMissingTrustAndIdentityMaterial() {
        Map<String, String> environment = Map.of(
                "KVDB_GRPC_SECURITY_MODE", "mtls",
                "KVDB_IDENTITY_ROLE", "gateway",
                "KVDB_IDENTITY_PRINCIPAL", "gateway-1");

        assertThrows(IllegalStateException.class, () -> GrpcSecurityConfig.internal(Role.GATEWAY, environment));
    }
}
