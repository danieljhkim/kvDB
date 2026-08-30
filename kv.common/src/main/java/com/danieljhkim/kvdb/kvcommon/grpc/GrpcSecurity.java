package com.danieljhkim.kvdb.kvcommon.grpc;

import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import java.io.IOException;

/** Applies fail-closed transport security to gRPC servers. */
public final class GrpcSecurity {

    private GrpcSecurity() {}

    public static NettyServerBuilder configureServer(NettyServerBuilder builder, GrpcSecurityConfig config) {
        if (config.mode() == GrpcSecurityConfig.Mode.DEVELOPMENT_PLAINTEXT) {
            return builder;
        }
        try {
            return builder.sslContext(GrpcSslContexts.forServer(
                            config.certificateChain().toFile(),
                            config.privateKey().toFile())
                    .trustManager(ReloadingRevocationTrustManager.create(config.trustBundle(), config.revocationList()))
                    .clientAuth(ClientAuth.REQUIRE)
                    .build());
        } catch (IOException e) {
            throw new IllegalStateException("Unable to load gRPC TLS credentials", e);
        }
    }
}
