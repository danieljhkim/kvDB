package com.danieljhkim.kvdb.kvcommon.grpc;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.io.IOException;

/**
 * Builds internal channels with verified TLS peer identities.
 */
public final class InternalAuthChannels {

    private InternalAuthChannels() {}

    static final Metadata.Key<String> DEVELOPMENT_IDENTITY =
            Metadata.Key.of("x-kvdb-development-identity", Metadata.ASCII_STRING_MARSHALLER);

    public static ManagedChannel forAddress(String host, int port) {
        return forAddress(host, port, GrpcSecurityConfig.currentInternalIdentity());
    }

    public static ManagedChannel forAddress(String host, int port, GrpcSecurityConfig config) {
        return configure(NettyChannelBuilder.forAddress(host, port), config).build();
    }

    public static ManagedChannel forTarget(String target) {
        return forTarget(target, GrpcSecurityConfig.currentInternalIdentity());
    }

    public static ManagedChannel forTarget(String target, GrpcSecurityConfig config) {
        return configure(NettyChannelBuilder.forTarget(target), config).build();
    }

    public static NettyChannelBuilder configure(NettyChannelBuilder builder, GrpcSecurityConfig config) {
        if (config.mode() == GrpcSecurityConfig.Mode.DEVELOPMENT_PLAINTEXT) {
            return builder.usePlaintext().intercept(new ClientInterceptor() {
                @Override
                public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                        MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
                    return new io.grpc.ForwardingClientCall.SimpleForwardingClientCall<>(
                            next.newCall(method, callOptions)) {
                        @Override
                        public void start(ClientCall.Listener<RespT> responseListener, Metadata headers) {
                            headers.put(
                                    DEVELOPMENT_IDENTITY,
                                    config.localRole().sanValue() + "/" + config.localPrincipal());
                            super.start(responseListener, headers);
                        }
                    };
                }
            });
        }
        try {
            return builder.sslContext(GrpcSslContexts.forClient()
                    .trustManager(ReloadingRevocationTrustManager.create(config.trustBundle(), config.revocationList()))
                    .keyManager(
                            config.certificateChain().toFile(),
                            config.privateKey().toFile())
                    .build());
        } catch (IOException e) {
            throw new IllegalStateException("Unable to load internal gRPC TLS credentials", e);
        }
    }
}
