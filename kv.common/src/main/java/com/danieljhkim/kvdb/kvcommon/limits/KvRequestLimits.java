package com.danieljhkim.kvdb.kvcommon.limits;

import com.danieljhkim.kvdb.kvcommon.config.AppConfig;
import com.danieljhkim.kvdb.kvcommon.exception.InvalidRequestException;
import com.danieljhkim.kvdb.kvcommon.exception.PayloadTooLargeException;
import com.danieljhkim.kvdb.proto.gateway.RequestContext;
import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;
import java.util.Objects;
import java.util.regex.Pattern;

/** Shared allocation and context bounds enforced at both data-plane hops. */
public final class KvRequestLimits {

    private static final Pattern TRACEPARENT =
            Pattern.compile("^[0-9a-f]{2}-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}(?:-.*)?$");

    private final int maxKeyBytes;
    private final int maxValueBytes;
    private final int maxMessageBytes;
    private final int maxBatchEntries;
    private final int maxConcurrentRequestsPerConnection;
    private final int maxContextFieldBytes;

    public KvRequestLimits(AppConfig.LimitsConfig config) {
        AppConfig.LimitsConfig effective = config == null ? new AppConfig.LimitsConfig() : config;
        this.maxKeyBytes = positive(effective.getMaxKeyBytes(), "maxKeyBytes");
        this.maxValueBytes = positive(effective.getMaxValueBytes(), "maxValueBytes");
        this.maxMessageBytes = positive(effective.getMaxMessageBytes(), "maxMessageBytes");
        this.maxBatchEntries = positive(effective.getMaxBatchEntries(), "maxBatchEntries");
        this.maxConcurrentRequestsPerConnection =
                positive(effective.getMaxConcurrentRequestsPerConnection(), "maxConcurrentRequestsPerConnection");
        this.maxContextFieldBytes = positive(effective.getMaxContextFieldBytes(), "maxContextFieldBytes");
        if (maxMessageBytes < maxKeyBytes || maxMessageBytes < maxValueBytes) {
            throw new IllegalArgumentException("maxMessageBytes must be at least the key and value limits");
        }
    }

    public void validateKey(ByteString key) {
        Objects.requireNonNull(key, "key");
        if (key.isEmpty()) {
            throw new InvalidRequestException("Key cannot be empty");
        }
        bounded("key", key.size(), maxKeyBytes);
    }

    public void validateValue(ByteString value) {
        Objects.requireNonNull(value, "value");
        bounded("value", value.size(), maxValueBytes);
    }

    public void validateMessage(MessageLite message) {
        Objects.requireNonNull(message, "message");
        bounded("message", message.getSerializedSize(), maxMessageBytes);
    }

    public void validateBatchSize(int entries) {
        bounded("batch entries", entries, maxBatchEntries);
    }

    public void validateWriteContext(RequestContext context) {
        Objects.requireNonNull(context, "context");
        if (context.getRequestId().isBlank()) {
            throw new InvalidRequestException("request_id is required for writes and must be reused by retries");
        }
        boundedUtf8("request_id", context.getRequestId());
        boundedUtf8("tenant_id", context.getTenantId());
        boundedUtf8("principal", context.getPrincipal());
        boundedUtf8("traceparent", context.getTraceparent());
        if (!context.getTraceparent().isBlank()
                && !TRACEPARENT.matcher(context.getTraceparent()).matches()) {
            throw new InvalidRequestException("traceparent is malformed");
        }
    }

    public int maxMessageBytes() {
        return maxMessageBytes;
    }

    public int maxConcurrentRequestsPerConnection() {
        return maxConcurrentRequestsPerConnection;
    }

    public int maxBatchEntries() {
        return maxBatchEntries;
    }

    private void boundedUtf8(String field, String value) {
        bounded(field, value.getBytes(java.nio.charset.StandardCharsets.UTF_8).length, maxContextFieldBytes);
    }

    private static void bounded(String field, int actual, int maximum) {
        if (actual > maximum) {
            throw new PayloadTooLargeException(
                    field + " exceeds configured limit (actual=" + actual + ", max=" + maximum + ")");
        }
    }

    private static int positive(int value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }
}
