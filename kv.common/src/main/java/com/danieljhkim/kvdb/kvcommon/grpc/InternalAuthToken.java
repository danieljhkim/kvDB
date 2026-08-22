package com.danieljhkim.kvdb.kvcommon.grpc;

import com.danieljhkim.kvdb.kvcommon.config.AppConfig;
import io.grpc.Metadata;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/**
 * Shared internal gRPC credential: a cluster-wide token carried in {@code x-kvdb-internal-token}.
 *
 * <p>Resolution order: {@code KVDB_INTERNAL_GRPC_TOKEN} env, {@code kvdb.internal.grpc.token} system
 * property, then {@code security.internalGrpcToken} from {@link AppConfig}.
 */
public final class InternalAuthToken {

    public static final String ENV_VAR = "KVDB_INTERNAL_GRPC_TOKEN";
    public static final String METADATA_HEADER = "x-kvdb-internal-token";
    public static final Metadata.Key<String> METADATA_KEY =
            Metadata.Key.of(METADATA_HEADER, Metadata.ASCII_STRING_MARSHALLER);

    private InternalAuthToken() {}

    public static String resolve() {
        return firstNonBlank(System.getenv(ENV_VAR), System.getProperty("kvdb.internal.grpc.token"));
    }

    public static String resolve(AppConfig config) {
        String fromEnvOrProperty = resolve();
        if (!fromEnvOrProperty.isEmpty()) {
            return fromEnvOrProperty;
        }
        if (config != null && config.getSecurity() != null) {
            return firstNonBlank(config.getSecurity().getInternalGrpcToken());
        }
        return "";
    }

    public static String require(AppConfig config) {
        String token = resolve(config);
        if (token.isEmpty()) {
            throw new IllegalStateException(
                    "Internal gRPC token is required. Set " + ENV_VAR + " or security.internalGrpcToken.");
        }
        return token;
    }

    public static boolean matches(String expected, String provided) {
        if (expected == null || expected.isEmpty() || provided == null || provided.isEmpty()) {
            return false;
        }
        byte[] left = expected.getBytes(StandardCharsets.UTF_8);
        byte[] right = provided.getBytes(StandardCharsets.UTF_8);
        return MessageDigest.isEqual(left, right);
    }

    private static String firstNonBlank(String... values) {
        if (values == null) {
            return "";
        }
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value.trim();
            }
        }
        return "";
    }
}
