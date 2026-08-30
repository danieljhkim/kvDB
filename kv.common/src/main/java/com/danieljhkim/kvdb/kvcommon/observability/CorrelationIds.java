package com.danieljhkim.kvdb.kvcommon.observability;

import java.util.UUID;

/** Keeps a validated correlation id available to outbound gRPC calls on the request thread. */
public final class CorrelationIds {

    private static final ThreadLocal<String> CURRENT = new ThreadLocal<>();

    private CorrelationIds() {}

    public static String current() {
        return CURRENT.get();
    }

    public static String newOrValidated(String candidate) {
        if (candidate != null) {
            try {
                return UUID.fromString(candidate).toString();
            } catch (IllegalArgumentException ignored) {
                // A client-controlled id is never used as a trusted log value.
            }
        }
        return UUID.randomUUID().toString();
    }

    public static void set(String correlationId) {
        CURRENT.set(correlationId);
    }

    public static void clear() {
        CURRENT.remove();
    }
}
