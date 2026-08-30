package com.danieljhkim.kvdb.kvcommon.grpc;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.danieljhkim.kvdb.kvcommon.grpc.GrpcIdentity.Role;
import com.danieljhkim.kvdb.proto.coordinator.CoordinatorGrpc;
import com.danieljhkim.kvdb.proto.coordinator.InitShardsRequest;
import com.danieljhkim.kvdb.proto.gateway.GetRequest;
import com.danieljhkim.kvdb.proto.gateway.KvGatewayGrpc;
import com.danieljhkim.kvdb.proto.gateway.RequestContext;
import com.kvdb.proto.kvstore.KVServiceGrpc;
import com.kvdb.proto.kvstore.PingRequest;
import com.kvdb.proto.kvstore.PingResponse;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.SelfSignedCertificate;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.HexFormat;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class GrpcTlsNegativeIntegrationTest {

    @Test
    void unknownCaIsRejectedDuringHandshake() throws Exception {
        try (CertificateFixture serverCertificate = new CertificateFixture("localhost");
                CertificateFixture unknownCa = new CertificateFixture("unknown-ca")) {
            RunningServer server = startServer(serverCertificate);
            ManagedChannel channel = channel(server.port(), unknownCa);
            try {
                assertThrows(StatusRuntimeException.class, () -> ping(channel));
            } finally {
                channel.shutdownNow();
                server.close();
            }
        }
    }

    @Test
    void wrongServerSanIsRejectedDuringHandshake() throws Exception {
        try (CertificateFixture wrongSan = new CertificateFixture("wrong.example")) {
            RunningServer server = startServer(wrongSan);
            ManagedChannel channel = channel(server.port(), wrongSan);
            try {
                assertThrows(StatusRuntimeException.class, () -> ping(channel));
            } finally {
                channel.shutdownNow();
                server.close();
            }
        }
    }

    @Test
    void plaintextDowngradeCannotReachTlsListener() throws Exception {
        try (CertificateFixture serverCertificate = new CertificateFixture("localhost")) {
            RunningServer server = startServer(serverCertificate);
            ManagedChannel channel = NettyChannelBuilder.forAddress("localhost", server.port())
                    .usePlaintext()
                    .build();
            try {
                assertThrows(StatusRuntimeException.class, () -> ping(channel));
            } finally {
                channel.shutdownNow();
                server.close();
            }
        }
    }

    @Test
    void nodeRoleEscalationIsRejectedOverRpc() throws Exception {
        InternalAuthServerInterceptor interceptor =
                new InternalAuthServerInterceptor(GrpcSecurityConfig.development(Role.COORDINATOR, "server"));
        Server server = NettyServerBuilder.forPort(0)
                .addService(ServerInterceptors.intercept(new CoordinatorGrpc.CoordinatorImplBase() {}, interceptor))
                .build()
                .start();
        ManagedChannel channel = NettyChannelBuilder.forAddress("localhost", server.getPort())
                .usePlaintext()
                .build();
        Metadata headers = new Metadata();
        headers.put(InternalAuthChannels.DEVELOPMENT_IDENTITY, "storage-node/node-1");
        try {
            var stub = CoordinatorGrpc.newBlockingStub(channel)
                    .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
            StatusRuntimeException exception = assertThrows(
                    StatusRuntimeException.class, () -> stub.initShards(InitShardsRequest.getDefaultInstance()));
            assertTrue(exception.getStatus().getCode() == io.grpc.Status.Code.PERMISSION_DENIED);
        } finally {
            channel.shutdownNow();
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void replayedBearerIsRejectedOverGatewayRpc() throws Exception {
        InternalAuthServerInterceptor interceptor =
                new InternalAuthServerInterceptor(GrpcSecurityConfig.development(Role.GATEWAY, "server"));
        Server server = NettyServerBuilder.forPort(0)
                .addService(ServerInterceptors.intercept(new KvGatewayGrpc.KvGatewayImplBase() {}, interceptor))
                .build()
                .start();
        ManagedChannel channel = NettyChannelBuilder.forAddress("localhost", server.getPort())
                .usePlaintext()
                .build();
        Metadata headers = new Metadata();
        headers.put(InternalAuthChannels.DEVELOPMENT_IDENTITY, "client/tenant-a/alice");
        headers.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer replayed");
        try {
            var stub = KvGatewayGrpc.newBlockingStub(channel)
                    .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
            StatusRuntimeException exception =
                    assertThrows(StatusRuntimeException.class, () -> stub.get(GetRequest.getDefaultInstance()));
            assertTrue(exception.getStatus().getCode() == io.grpc.Status.Code.UNAUTHENTICATED);
        } finally {
            channel.shutdownNow();
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void requestTenantCannotOverrideVerifiedClientTenant() throws Exception {
        InternalAuthServerInterceptor interceptor =
                new InternalAuthServerInterceptor(GrpcSecurityConfig.development(Role.GATEWAY, "server"));
        Server server = NettyServerBuilder.forPort(0)
                .addService(ServerInterceptors.intercept(new KvGatewayGrpc.KvGatewayImplBase() {}, interceptor))
                .build()
                .start();
        ManagedChannel channel = NettyChannelBuilder.forAddress("localhost", server.getPort())
                .usePlaintext()
                .build();
        Metadata headers = new Metadata();
        headers.put(InternalAuthChannels.DEVELOPMENT_IDENTITY, "client/tenant-a/alice");
        GetRequest request = GetRequest.newBuilder()
                .setCtx(RequestContext.newBuilder().setTenantId("tenant-b").setPrincipal("alice"))
                .build();
        try {
            var stub = KvGatewayGrpc.newBlockingStub(channel)
                    .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
            StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, () -> stub.get(request));
            assertTrue(exception.getStatus().getCode() == io.grpc.Status.Code.PERMISSION_DENIED);
        } finally {
            channel.shutdownNow();
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void expiredIdentityIsRejectedBeforeAuthorization() throws Exception {
        Date before = Date.from(Instant.now().minus(2, ChronoUnit.DAYS));
        Date after = Date.from(Instant.now().minus(1, ChronoUnit.DAYS));
        try (CertificateFixture expired = new CertificateFixture("expired", before, after)) {
            InternalAuthServerInterceptor interceptor =
                    new InternalAuthServerInterceptor(GrpcSecurityConfig.development(Role.COORDINATOR, "server"));

            assertThrows(Exception.class, () -> interceptor.authenticateCertificate(expired.certificate.cert()));
        }
    }

    @Test
    void revokedIdentityIsRejectedWithoutRestart(@TempDir Path tempDir) throws Exception {
        try (CertificateFixture revoked = new CertificateFixture("revoked")) {
            String fingerprint = HexFormat.of()
                    .formatHex(MessageDigest.getInstance("SHA-256")
                            .digest(revoked.certificate.cert().getEncoded()));
            Path revocations = tempDir.resolve("revoked.sha256");
            Files.writeString(revocations, fingerprint + System.lineSeparator());
            GrpcSecurityConfig config = new GrpcSecurityConfig(
                    GrpcSecurityConfig.Mode.MTLS,
                    Role.COORDINATOR,
                    "server",
                    revoked.certificate.certificate().toPath(),
                    revoked.certificate.privateKey().toPath(),
                    revoked.certificate.certificate().toPath(),
                    revocations);
            InternalAuthServerInterceptor interceptor = new InternalAuthServerInterceptor(config);
            ReloadingRevocationTrustManager trustManager =
                    ReloadingRevocationTrustManager.create(config.trustBundle(), revocations);

            Exception exception = assertThrows(
                    Exception.class, () -> interceptor.authenticateCertificate(revoked.certificate.cert()));
            assertTrue(exception.getMessage().contains("revoked"));
            assertThrows(
                    Exception.class,
                    () -> trustManager.checkServerTrusted(
                            new java.security.cert.X509Certificate[] {revoked.certificate.cert()}, "RSA"));
        }
    }

    private static void ping(ManagedChannel channel) {
        KVServiceGrpc.newBlockingStub(channel)
                .withDeadlineAfter(3, TimeUnit.SECONDS)
                .ping(PingRequest.getDefaultInstance());
    }

    private static ManagedChannel channel(int port, CertificateFixture trust) throws Exception {
        return NettyChannelBuilder.forAddress("localhost", port)
                .sslContext(GrpcSslContexts.forClient()
                        .trustManager(trust.certificate.certificate())
                        .build())
                .build();
    }

    private static RunningServer startServer(CertificateFixture certificate) throws Exception {
        Server server = NettyServerBuilder.forPort(0)
                .sslContext(GrpcSslContexts.forServer(
                                certificate.certificate.certificate(), certificate.certificate.privateKey())
                        .build())
                .addService(new KVServiceGrpc.KVServiceImplBase() {
                    @Override
                    public void ping(PingRequest request, StreamObserver<PingResponse> responseObserver) {
                        responseObserver.onNext(PingResponse.getDefaultInstance());
                        responseObserver.onCompleted();
                    }
                })
                .build()
                .start();
        return new RunningServer(server);
    }

    private record RunningServer(Server server) implements AutoCloseable {
        int port() {
            return server.getPort();
        }

        @Override
        public void close() throws InterruptedException {
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static final class CertificateFixture implements AutoCloseable {
        private final SelfSignedCertificate certificate;

        private CertificateFixture(String name) throws Exception {
            this.certificate = new SelfSignedCertificate(name);
        }

        private CertificateFixture(String name, Date before, Date after) throws Exception {
            this.certificate = new SelfSignedCertificate(name, before, after);
        }

        @Override
        public void close() {
            certificate.delete();
        }
    }
}
