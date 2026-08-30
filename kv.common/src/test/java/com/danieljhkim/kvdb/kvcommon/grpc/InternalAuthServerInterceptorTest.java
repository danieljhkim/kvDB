package com.danieljhkim.kvdb.kvcommon.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.danieljhkim.kvdb.kvcommon.grpc.GrpcIdentity.Role;
import com.danieljhkim.kvdb.proto.coordinator.CoordinatorGrpc;
import com.danieljhkim.kvdb.proto.gateway.KvGatewayGrpc;
import com.danieljhkim.kvdb.proto.raft.RaftServiceGrpc;
import com.kvdb.proto.kvstore.KVServiceGrpc;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class InternalAuthServerInterceptorTest {

    @Test
    void storageNodeCannotInvokeAdminMutation() {
        RecordingCall<?, ?> call = new RecordingCall<>(CoordinatorGrpc.getInitShardsMethod());

        intercept(call, identity("storage-node/node-1"), new AtomicReference<>());

        assertEquals(Status.Code.PERMISSION_DENIED, call.closedStatus.getCode());
    }

    @Test
    void rolesAreScopedAcrossControlDataAndRaftPlanes() {
        assertAllowed(CoordinatorGrpc.getInitShardsMethod(), "admin/operator-1");
        assertAllowed(CoordinatorGrpc.getHeartbeatMethod(), "storage-node/node-1");
        assertAllowed(KVServiceGrpc.getSetMethod(), "gateway/gateway-1");
        assertAllowed(KVServiceGrpc.getReplicateSetMethod(), "storage-node/node-1");
        assertAllowed(RaftServiceGrpc.getAppendEntriesMethod(), "coordinator/coordinator-1");
    }

    @Test
    void gatewayIdentityComesFromVerifiedContext() {
        RecordingCall<?, ?> call = new RecordingCall<>(KvGatewayGrpc.getPutMethod());
        AtomicReference<GrpcIdentity> observed = new AtomicReference<>();

        intercept(call, identity("client/tenant-a/alice"), observed);

        assertNull(call.closedStatus);
        assertEquals(new GrpcIdentity(Role.EXTERNAL_CLIENT, "tenant-a", "alice"), observed.get());
    }

    @Test
    void replayedBearerCredentialIsRejectedEvenWithIdentity() {
        RecordingCall<?, ?> call = new RecordingCall<>(KvGatewayGrpc.getGetMethod());
        Metadata headers = identity("client/tenant-a/alice");
        headers.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer captured-value");

        intercept(call, headers, new AtomicReference<>());

        assertEquals(Status.Code.UNAUTHENTICATED, call.closedStatus.getCode());
        assertTrue(call.closedStatus.getDescription().contains("client certificate"));
    }

    @Test
    void missingIdentityIsRejected() {
        RecordingCall<?, ?> call = new RecordingCall<>(KVServiceGrpc.getPingMethod());

        intercept(call, new Metadata(), new AtomicReference<>());

        assertEquals(Status.Code.UNAUTHENTICATED, call.closedStatus.getCode());
    }

    private static void assertAllowed(MethodDescriptor<?, ?> method, String identity) {
        RecordingCall<?, ?> call = new RecordingCall<>(method);
        AtomicReference<GrpcIdentity> observed = new AtomicReference<>();
        intercept(call, identity(identity), observed);
        assertNull(call.closedStatus);
        assertTrue(observed.get() != null);
    }

    private static Metadata identity(String value) {
        Metadata headers = new Metadata();
        headers.put(InternalAuthChannels.DEVELOPMENT_IDENTITY, value);
        return headers;
    }

    private static void intercept(
            RecordingCall<?, ?> call, Metadata headers, AtomicReference<GrpcIdentity> observedIdentity) {
        InternalAuthServerInterceptor interceptor =
                new InternalAuthServerInterceptor(GrpcSecurityConfig.development(Role.COORDINATOR, "test-server"));
        AtomicBoolean nextCalled = new AtomicBoolean(false);
        interceptor.interceptCall(call, headers, markingHandler(nextCalled, observedIdentity));
        if (call.closedStatus == null) {
            assertTrue(nextCalled.get());
        }
    }

    private static <ReqT, RespT> ServerCallHandler<ReqT, RespT> markingHandler(
            AtomicBoolean nextCalled, AtomicReference<GrpcIdentity> observedIdentity) {
        return (call, headers) -> {
            nextCalled.set(true);
            observedIdentity.set(GrpcPeerIdentity.require());
            return new ServerCall.Listener<>() {};
        };
    }

    private static final class RecordingCall<ReqT, RespT> extends ServerCall<ReqT, RespT> {
        private final MethodDescriptor<ReqT, RespT> method;
        private Status closedStatus;

        private RecordingCall(MethodDescriptor<ReqT, RespT> method) {
            this.method = method;
        }

        @Override
        public void request(int numMessages) {}

        @Override
        public void sendHeaders(Metadata headers) {}

        @Override
        public void sendMessage(RespT message) {}

        @Override
        public void close(Status status, Metadata trailers) {
            this.closedStatus = status;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public MethodDescriptor<ReqT, RespT> getMethodDescriptor() {
            return method;
        }
    }
}
