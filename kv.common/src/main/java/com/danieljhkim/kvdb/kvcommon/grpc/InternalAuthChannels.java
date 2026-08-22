package com.danieljhkim.kvdb.kvcommon.grpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

/**
 * Helpers for building plaintext internal channels that carry the cluster token.
 */
public final class InternalAuthChannels {

    private InternalAuthChannels() {}

    public static ManagedChannel plaintext(String host, int port, String token) {
        return withToken(ManagedChannelBuilder.forAddress(host, port).usePlaintext(), token)
                .build();
    }

    public static ManagedChannel plaintextTarget(String target, String token) {
        return withToken(ManagedChannelBuilder.forTarget(target).usePlaintext(), token)
                .build();
    }

    public static ManagedChannelBuilder<?> withToken(ManagedChannelBuilder<?> builder, String token) {
        if (token != null && !token.isBlank()) {
            builder.intercept(new InternalAuthClientInterceptor(token));
        }
        return builder;
    }
}
