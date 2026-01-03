package com.danieljhkim.kvdb.kvgateway.retry;

import io.grpc.Status;
import java.util.Set;

/**
 * Configuration for retry behavior in the gateway. Defines retry attempts, backoff parameters, and retryable error
 * codes.
 */
public class RetryPolicy {

    private final int maxAttempts;
    private final long initialBackoffMs;
    private final long maxBackoffMs;
    private final double backoffMultiplier;
    private final double jitterPercent;
    private final Set<Status.Code> retryableStatusCodes;

    private RetryPolicy(Builder builder) {
        this.maxAttempts = builder.maxAttempts;
        this.initialBackoffMs = builder.initialBackoffMs;
        this.maxBackoffMs = builder.maxBackoffMs;
        this.backoffMultiplier = builder.backoffMultiplier;
        this.jitterPercent = builder.jitterPercent;
        this.retryableStatusCodes = Set.copyOf(builder.retryableStatusCodes);
    }

    /**
     * Creates a RetryPolicy with default settings.
     */
    public static RetryPolicy defaults() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public long getInitialBackoffMs() {
        return initialBackoffMs;
    }

    public long getMaxBackoffMs() {
        return maxBackoffMs;
    }

    public double getBackoffMultiplier() {
        return backoffMultiplier;
    }

    public double getJitterPercent() {
        return jitterPercent;
    }

    public Set<Status.Code> getRetryableStatusCodes() {
        return retryableStatusCodes;
    }

    /**
     * Checks if the given status code is retryable.
     */
    public boolean isRetryable(Status.Code code) {
        return retryableStatusCodes.contains(code);
    }

    /**
     * Calculates the backoff delay for a given attempt number.
     *
     * @param attempt attempt number (1-based)
     * @return backoff delay in milliseconds with jitter applied
     */
    public long calculateBackoff(int attempt) {
        if (attempt <= 1) {
            return applyJitter(initialBackoffMs);
        }

        double backoff = initialBackoffMs * Math.pow(backoffMultiplier, attempt - 1);
        long cappedBackoff = (long) Math.min(backoff, maxBackoffMs);
        return applyJitter(cappedBackoff);
    }

    private long applyJitter(long backoffMs) {
        double jitter = backoffMs * jitterPercent * Math.random();
        return backoffMs + (long) jitter;
    }

    public static class Builder {
        private int maxAttempts = 3;
        private long initialBackoffMs = 25;
        private long maxBackoffMs = 1000;
        private double backoffMultiplier = 2.0;
        private double jitterPercent = 0.25;
        private Set<Status.Code> retryableStatusCodes =
                Set.of(Status.Code.UNAVAILABLE, Status.Code.DEADLINE_EXCEEDED, Status.Code.RESOURCE_EXHAUSTED);

        public Builder maxAttempts(int maxAttempts) {
            this.maxAttempts = maxAttempts;
            return this;
        }

        public Builder initialBackoffMs(long initialBackoffMs) {
            this.initialBackoffMs = initialBackoffMs;
            return this;
        }

        public Builder maxBackoffMs(long maxBackoffMs) {
            this.maxBackoffMs = maxBackoffMs;
            return this;
        }

        public Builder backoffMultiplier(double backoffMultiplier) {
            this.backoffMultiplier = backoffMultiplier;
            return this;
        }

        public Builder jitterPercent(double jitterPercent) {
            this.jitterPercent = jitterPercent;
            return this;
        }

        public Builder retryableStatusCodes(Set<Status.Code> retryableStatusCodes) {
            this.retryableStatusCodes = retryableStatusCodes;
            return this;
        }

        public RetryPolicy build() {
            return new RetryPolicy(this);
        }
    }
}
