package com.danieljhkim.kvdb.kvcommon.grpc;

import com.danieljhkim.kvdb.kvcommon.grpc.GrpcIdentity.Role;
import com.danieljhkim.kvdb.proto.coordinator.CoordinatorGrpc;
import com.danieljhkim.kvdb.proto.gateway.KvGatewayGrpc;
import com.danieljhkim.kvdb.proto.gateway.RequestContext;
import com.danieljhkim.kvdb.proto.raft.RaftServiceGrpc;
import com.kvdb.proto.kvstore.KVServiceGrpc;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;

/** Authenticates TLS certificate identities and enforces per-RPC role scopes. */
public final class InternalAuthServerInterceptor implements ServerInterceptor {

    private static final Metadata.Key<String> AUTHORIZATION =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
    private static final String SPIFFE_PREFIX = "spiffe://kvdb/";
    private static final Status UNAUTHENTICATED =
            Status.UNAUTHENTICATED.withDescription("A verified TLS peer identity is required");

    private static final Map<String, Set<Role>> AUTHORIZATION_SCOPES = Map.ofEntries(
            entry(
                    CoordinatorGrpc.getGetShardMapMethod(),
                    Role.COORDINATOR,
                    Role.STORAGE_NODE,
                    Role.GATEWAY,
                    Role.ADMIN),
            entry(CoordinatorGrpc.getWatchShardMapMethod(), Role.STORAGE_NODE, Role.GATEWAY),
            entry(CoordinatorGrpc.getResolveShardMethod(), Role.STORAGE_NODE, Role.GATEWAY, Role.ADMIN),
            entry(CoordinatorGrpc.getGetNodeMethod(), Role.COORDINATOR, Role.STORAGE_NODE, Role.GATEWAY, Role.ADMIN),
            entry(CoordinatorGrpc.getListNodesMethod(), Role.COORDINATOR, Role.GATEWAY, Role.ADMIN),
            entry(
                    CoordinatorGrpc.getGetCoordinatorLeaderMethod(),
                    Role.COORDINATOR,
                    Role.STORAGE_NODE,
                    Role.GATEWAY,
                    Role.ADMIN),
            entry(CoordinatorGrpc.getHeartbeatMethod(), Role.STORAGE_NODE),
            entry(CoordinatorGrpc.getReportShardLeaderMethod(), Role.STORAGE_NODE),
            entry(CoordinatorGrpc.getRegisterNodeMethod(), Role.STORAGE_NODE, Role.ADMIN),
            entry(CoordinatorGrpc.getInitShardsMethod(), Role.ADMIN),
            entry(CoordinatorGrpc.getSetNodeStatusMethod(), Role.ADMIN),
            entry(CoordinatorGrpc.getSetShardReplicasMethod(), Role.ADMIN),
            entry(CoordinatorGrpc.getSetShardLeaderMethod(), Role.ADMIN),
            entry(KVServiceGrpc.getGetMethod(), Role.GATEWAY),
            entry(KVServiceGrpc.getSetMethod(), Role.GATEWAY),
            entry(KVServiceGrpc.getDeleteMethod(), Role.GATEWAY),
            entry(KVServiceGrpc.getReplicateMutationMethod(), Role.STORAGE_NODE),
            entry(KVServiceGrpc.getRepairReplicaMethod(), Role.STORAGE_NODE),
            entry(KVServiceGrpc.getFetchReplicaStateMethod(), Role.STORAGE_NODE),
            entry(KVServiceGrpc.getPingMethod(), Role.COORDINATOR, Role.STORAGE_NODE, Role.GATEWAY, Role.ADMIN),
            entry(KVServiceGrpc.getShutdownMethod(), Role.ADMIN),
            entry(RaftServiceGrpc.getRequestVoteMethod(), Role.COORDINATOR),
            entry(RaftServiceGrpc.getAppendEntriesMethod(), Role.COORDINATOR),
            entry(RaftServiceGrpc.getInstallSnapshotMethod(), Role.COORDINATOR),
            entry(RaftServiceGrpc.getAddServerMethod(), Role.ADMIN),
            entry(RaftServiceGrpc.getRemoveServerMethod(), Role.ADMIN),
            entry(KvGatewayGrpc.getGetMethod(), Role.EXTERNAL_CLIENT, Role.ADMIN),
            entry(KvGatewayGrpc.getBatchGetMethod(), Role.EXTERNAL_CLIENT, Role.ADMIN),
            entry(KvGatewayGrpc.getPutMethod(), Role.EXTERNAL_CLIENT, Role.ADMIN),
            entry(KvGatewayGrpc.getDeleteMethod(), Role.EXTERNAL_CLIENT, Role.ADMIN));

    private final GrpcSecurityConfig config;

    public InternalAuthServerInterceptor(GrpcSecurityConfig config) {
        this.config = config;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        if (headers.get(AUTHORIZATION) != null) {
            return reject(
                    call,
                    Status.UNAUTHENTICATED.withDescription(
                            "Bearer credentials are not accepted; authenticate with a client certificate"));
        }

        GrpcIdentity identity;
        try {
            identity = authenticate(call, headers);
        } catch (Exception e) {
            return reject(call, UNAUTHENTICATED.withCause(e));
        }

        Set<Role> allowedRoles =
                AUTHORIZATION_SCOPES.get(call.getMethodDescriptor().getFullMethodName());
        if (allowedRoles == null || !allowedRoles.contains(identity.role())) {
            return reject(
                    call,
                    Status.PERMISSION_DENIED.withDescription(
                            "Authenticated role " + identity.role().sanValue() + " is not authorized for this RPC"));
        }

        Context context = Context.current().withValue(GrpcPeerIdentity.CURRENT, identity);
        ServerCall.Listener<ReqT> listener = Contexts.interceptCall(context, call, headers, next);
        if (identity.role() != Role.EXTERNAL_CLIENT) {
            return listener;
        }
        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(listener) {
            private boolean rejected;

            @Override
            public void onMessage(ReqT message) {
                RequestContext requestContext = gatewayRequestContext(message);
                if (requestContext != null && !matchesVerifiedIdentity(requestContext, identity)) {
                    rejected = true;
                    call.close(
                            Status.PERMISSION_DENIED.withDescription(
                                    "Request tenant/principal conflicts with the verified client identity"),
                            new Metadata());
                    return;
                }
                super.onMessage(message);
            }

            @Override
            public void onHalfClose() {
                if (!rejected) {
                    super.onHalfClose();
                }
            }
        };
    }

    private static RequestContext gatewayRequestContext(Object message) {
        if (message instanceof com.danieljhkim.kvdb.proto.gateway.GetRequest request) {
            return request.getCtx();
        }
        if (message instanceof com.danieljhkim.kvdb.proto.gateway.BatchGetRequest request) {
            return request.getCtx();
        }
        if (message instanceof com.danieljhkim.kvdb.proto.gateway.PutRequest request) {
            return request.getCtx();
        }
        if (message instanceof com.danieljhkim.kvdb.proto.gateway.DeleteRequest request) {
            return request.getCtx();
        }
        return null;
    }

    private static boolean matchesVerifiedIdentity(RequestContext requestContext, GrpcIdentity identity) {
        return (requestContext.getTenantId().isBlank()
                        || requestContext.getTenantId().equals(identity.tenant()))
                && (requestContext.getPrincipal().isBlank()
                        || requestContext.getPrincipal().equals(identity.principal()));
    }

    private GrpcIdentity authenticate(ServerCall<?, ?> call, Metadata headers) throws Exception {
        if (config.mode() == GrpcSecurityConfig.Mode.DEVELOPMENT_PLAINTEXT) {
            return parseDevelopmentIdentity(headers.get(InternalAuthChannels.DEVELOPMENT_IDENTITY));
        }

        SSLSession session = call.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION);
        if (session == null) {
            throw new SSLPeerUnverifiedException("plaintext transport rejected");
        }
        Certificate[] peerCertificates = session.getPeerCertificates();
        if (peerCertificates.length == 0 || !(peerCertificates[0] instanceof X509Certificate certificate)) {
            throw new SSLPeerUnverifiedException("X.509 client certificate required");
        }
        return authenticateCertificate(certificate);
    }

    GrpcIdentity authenticateCertificate(X509Certificate certificate) throws Exception {
        certificate.checkValidity();
        if (isRevoked(certificate)) {
            throw new SSLPeerUnverifiedException("client certificate is revoked");
        }
        return identityFromCertificate(certificate);
    }

    private GrpcIdentity parseDevelopmentIdentity(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("development identity is missing");
        }
        String[] parts = value.split("/", 3);
        Role role = Role.parse(parts[0]);
        if (role == Role.EXTERNAL_CLIENT) {
            if (parts.length != 3 || parts[1].isBlank() || parts[2].isBlank()) {
                throw new IllegalArgumentException("development client identity must include tenant and principal");
            }
            return new GrpcIdentity(role, parts[1], parts[2]);
        }
        if (parts.length != 2 || parts[1].isBlank()) {
            throw new IllegalArgumentException("development workload identity must include principal");
        }
        return new GrpcIdentity(role, "", parts[1]);
    }

    static GrpcIdentity identityFromCertificate(X509Certificate certificate) throws Exception {
        Collection<List<?>> sans = certificate.getSubjectAlternativeNames();
        if (sans == null) {
            throw new SSLPeerUnverifiedException("certificate has no subject alternative names");
        }
        for (List<?> san : sans) {
            if (san.size() < 2 || !Integer.valueOf(6).equals(san.get(0))) {
                continue;
            }
            URI uri = URI.create(String.valueOf(san.get(1)));
            String value = uri.toString();
            if (!value.startsWith(SPIFFE_PREFIX)) {
                continue;
            }
            String[] parts = value.substring(SPIFFE_PREFIX.length()).split("/", -1);
            Role role = Role.parse(parts[0]);
            if (role == Role.EXTERNAL_CLIENT) {
                if (parts.length != 3 || parts[1].isBlank() || parts[2].isBlank()) {
                    throw new SSLPeerUnverifiedException(
                            "client URI SAN must be spiffe://kvdb/client/<tenant>/<principal>");
                }
                return new GrpcIdentity(role, parts[1], parts[2]);
            }
            if (parts.length != 2 || parts[1].isBlank()) {
                throw new SSLPeerUnverifiedException("workload URI SAN must be spiffe://kvdb/<role>/<principal>");
            }
            return new GrpcIdentity(role, "", parts[1]);
        }
        throw new SSLPeerUnverifiedException("certificate has no recognized kvDB URI SAN");
    }

    private boolean isRevoked(X509Certificate certificate) throws Exception {
        if (config.revocationList() == null) {
            return false;
        }
        String fingerprint =
                HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(certificate.getEncoded()));
        try (var lines = Files.lines(config.revocationList(), StandardCharsets.UTF_8)) {
            return lines.map(String::trim)
                    .filter(line -> !line.isEmpty() && !line.startsWith("#"))
                    .map(line -> line.replace(":", "").toLowerCase(Locale.ROOT))
                    .anyMatch(fingerprint::equals);
        }
    }

    private static Map.Entry<String, Set<Role>> entry(
            io.grpc.MethodDescriptor<?, ?> method, Role first, Role... additional) {
        EnumSet<Role> roles = EnumSet.of(first, additional);
        return Map.entry(method.getFullMethodName(), Set.copyOf(roles));
    }

    private static <ReqT> ServerCall.Listener<ReqT> reject(ServerCall<ReqT, ?> call, Status status) {
        call.close(status, new Metadata());
        return new ServerCall.Listener<>() {};
    }
}
