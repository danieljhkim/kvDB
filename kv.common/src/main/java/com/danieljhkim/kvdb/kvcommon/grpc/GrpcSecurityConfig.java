package com.danieljhkim.kvdb.kvcommon.grpc;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/** Fail-closed TLS configuration sourced from secret file paths. */
public record GrpcSecurityConfig(
        Mode mode,
        GrpcIdentity.Role localRole,
        String localPrincipal,
        Path certificateChain,
        Path privateKey,
        Path trustBundle,
        Path revocationList) {

    public enum Mode {
        MTLS,
        DEVELOPMENT_PLAINTEXT
    }

    public static GrpcSecurityConfig internal(GrpcIdentity.Role expectedRole) {
        return internal(expectedRole, System.getenv());
    }

    static GrpcSecurityConfig internal(GrpcIdentity.Role expectedRole, Map<String, String> environment) {
        return fromEnvironment(expectedRole, "KVDB_INTERNAL_TLS_", environment);
    }

    public static GrpcSecurityConfig gatewayServer() {
        return fromEnvironment(GrpcIdentity.Role.GATEWAY, "KVDB_GATEWAY_TLS_", System.getenv());
    }

    public static GrpcSecurityConfig currentInternalIdentity() {
        String configuredRole = required(System.getenv(), "KVDB_IDENTITY_ROLE");
        return internal(GrpcIdentity.Role.parse(configuredRole));
    }

    public static GrpcSecurityConfig development(GrpcIdentity.Role role, String principal) {
        return new GrpcSecurityConfig(Mode.DEVELOPMENT_PLAINTEXT, role, principal, null, null, null, null);
    }

    private static GrpcSecurityConfig fromEnvironment(
            GrpcIdentity.Role expectedRole, String tlsPrefix, Map<String, String> environment) {
        Objects.requireNonNull(expectedRole, "expectedRole");
        String modeValue = environment
                .getOrDefault("KVDB_GRPC_SECURITY_MODE", "mtls")
                .trim()
                .toLowerCase(Locale.ROOT);
        if (modeValue.equals("development-plaintext")) {
            String deployment = environment.getOrDefault("KVDB_ENV", "").trim().toLowerCase(Locale.ROOT);
            if (!(deployment.equals("dev")
                    || deployment.equals("development")
                    || deployment.equals("local")
                    || deployment.equals("test"))) {
                throw new IllegalStateException(
                        "development-plaintext requires KVDB_ENV=dev, development, local, or test");
            }
            String principal = environment.getOrDefault("KVDB_IDENTITY_PRINCIPAL", expectedRole.sanValue() + "-dev");
            return development(expectedRole, principal);
        }
        if (!modeValue.equals("mtls")) {
            throw new IllegalStateException("KVDB_GRPC_SECURITY_MODE must be mtls or development-plaintext");
        }

        String configuredRole = required(environment, "KVDB_IDENTITY_ROLE");
        if (GrpcIdentity.Role.parse(configuredRole) != expectedRole) {
            throw new IllegalStateException("KVDB_IDENTITY_ROLE must be " + expectedRole.sanValue());
        }
        String principal = required(environment, "KVDB_IDENTITY_PRINCIPAL");
        Path cert = readableFile(environment, tlsPrefix + "CERT_CHAIN");
        Path key = readableFile(environment, tlsPrefix + "PRIVATE_KEY");
        Path trust = readableFile(environment, tlsPrefix + "TRUST_BUNDLE");
        String revocationPath =
                environment.getOrDefault(tlsPrefix + "REVOCATION_LIST", "").trim();
        Path revocations = revocationPath.isEmpty() ? null : readableFile(environment, tlsPrefix + "REVOCATION_LIST");
        return new GrpcSecurityConfig(Mode.MTLS, expectedRole, principal, cert, key, trust, revocations);
    }

    private static String required(Map<String, String> environment, String name) {
        String value = environment.getOrDefault(name, "").trim();
        if (value.isEmpty()) {
            throw new IllegalStateException(name + " is required when mTLS is enabled");
        }
        return value;
    }

    private static Path readableFile(Map<String, String> environment, String name) {
        Path path = Path.of(required(environment, name)).toAbsolutePath().normalize();
        if (!Files.isRegularFile(path) || !Files.isReadable(path)) {
            throw new IllegalStateException(name + " must name a readable file: " + path);
        }
        return path;
    }
}
