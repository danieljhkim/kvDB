package com.danieljhkim.kvdb.kvcommon.grpc;

import java.util.Locale;

/** A workload or client identity authenticated by the TLS peer certificate. */
public record GrpcIdentity(Role role, String tenant, String principal) {

    public enum Role {
        COORDINATOR("coordinator"),
        STORAGE_NODE("storage-node"),
        GATEWAY("gateway"),
        ADMIN("admin"),
        EXTERNAL_CLIENT("client");

        private final String sanValue;

        Role(String sanValue) {
            this.sanValue = sanValue;
        }

        public String sanValue() {
            return sanValue;
        }

        public static Role parse(String value) {
            String normalized = value.trim().toLowerCase(Locale.ROOT).replace('_', '-');
            for (Role role : values()) {
                if (role.sanValue.equals(normalized)) {
                    return role;
                }
            }
            throw new IllegalArgumentException("Unknown gRPC identity role: " + value);
        }
    }
}
