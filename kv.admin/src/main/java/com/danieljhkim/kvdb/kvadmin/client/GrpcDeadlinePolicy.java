package com.danieljhkim.kvdb.kvadmin.client;

import io.grpc.stub.AbstractStub;
import java.util.concurrent.TimeUnit;

/**
 * Helper for enforcing gRPC deadlines uniformly.
 */
public class GrpcDeadlinePolicy {

    /**
     * Apply a deadline to a gRPC stub.
     *
     * @param stub The gRPC stub
     * @param timeoutSeconds Timeout in seconds
     * @param <T> Stub type
     * @return Stub with deadline applied
     */
    public static <T extends AbstractStub<T>> T withDeadline(T stub, long timeoutSeconds) {
        return stub.withDeadlineAfter(timeoutSeconds, TimeUnit.SECONDS);
    }

    /**
     * Apply a deadline to a gRPC stub with custom time unit.
     */
    public static <T extends AbstractStub<T>> T withDeadline(T stub, long timeout, TimeUnit unit) {
        return stub.withDeadlineAfter(timeout, unit);
    }
}
