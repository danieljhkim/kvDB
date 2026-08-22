package com.danieljhkim.kvdb.kvcommon.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.danieljhkim.kvdb.proto.coordinator.CoordinatorGrpc;
import com.kvdb.proto.kvstore.KVServiceGrpc;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class InternalAuthServerInterceptorTest {

    private static final String TOKEN = "test-internal-token";
    private static final InternalAuthServerInterceptor INTERCEPTOR = new InternalAuthServerInterceptor(TOKEN);

    static MethodDescriptor<?, ?>[] protectedMethods() {
        return new MethodDescriptor<?, ?>[] {
            CoordinatorGrpc.getRegisterNodeMethod(),
            CoordinatorGrpc.getInitShardsMethod(),
            CoordinatorGrpc.getSetNodeStatusMethod(),
            CoordinatorGrpc.getSetShardReplicasMethod(),
            CoordinatorGrpc.getSetShardLeaderMethod(),
            CoordinatorGrpc.getReportShardLeaderMethod(),
            KVServiceGrpc.getSetMethod(),
            KVServiceGrpc.getDeleteMethod(),
            KVServiceGrpc.getReplicateSetMethod(),
            KVServiceGrpc.getReplicateDeleteMethod(),
            KVServiceGrpc.getShutdownMethod()
        };
    }

    @ParameterizedTest
    @MethodSource("protectedMethods")
    void unauthenticatedProtectedRpcIsRejected(MethodDescriptor<?, ?> method) {
        RecordingCall<?, ?> call = new RecordingCall<>(method);
        AtomicBoolean nextCalled = new AtomicBoolean(false);

        INTERCEPTOR.interceptCall(call, new Metadata(), markingHandler(nextCalled));

        assertNotNull(call.closedStatus);
        assertEquals(Status.Code.UNAUTHENTICATED, call.closedStatus.getCode());
        assertTrue(call.closedStatus.getDescription().contains("internal gRPC token"));
        assertTrue(!nextCalled.get());
    }

    @ParameterizedTest
    @MethodSource("protectedMethods")
    void wrongTokenProtectedRpcIsRejected(MethodDescriptor<?, ?> method) {
        RecordingCall<?, ?> call = new RecordingCall<>(method);
        Metadata headers = new Metadata();
        headers.put(InternalAuthToken.METADATA_KEY, "wrong-token");
        AtomicBoolean nextCalled = new AtomicBoolean(false);

        INTERCEPTOR.interceptCall(call, headers, markingHandler(nextCalled));

        assertEquals(Status.Code.UNAUTHENTICATED, call.closedStatus.getCode());
        assertTrue(!nextCalled.get());
    }

    @ParameterizedTest
    @MethodSource("protectedMethods")
    void authenticatedProtectedRpcIsAllowed(MethodDescriptor<?, ?> method) {
        RecordingCall<?, ?> call = new RecordingCall<>(method);
        Metadata headers = new Metadata();
        headers.put(InternalAuthToken.METADATA_KEY, TOKEN);
        AtomicBoolean nextCalled = new AtomicBoolean(false);

        INTERCEPTOR.interceptCall(call, headers, markingHandler(nextCalled));

        assertNull(call.closedStatus);
        assertTrue(nextCalled.get());
    }

    @Test
    void pingRemainsUnauthenticated() {
        RecordingCall<?, ?> call = new RecordingCall<>(KVServiceGrpc.getPingMethod());
        AtomicBoolean nextCalled = new AtomicBoolean(false);

        INTERCEPTOR.interceptCall(call, new Metadata(), markingHandler(nextCalled));

        assertNull(call.closedStatus);
        assertTrue(nextCalled.get());
    }

    @Test
    void getRemainsUnauthenticated() {
        RecordingCall<?, ?> call = new RecordingCall<>(KVServiceGrpc.getGetMethod());
        AtomicBoolean nextCalled = new AtomicBoolean(false);

        INTERCEPTOR.interceptCall(call, new Metadata(), markingHandler(nextCalled));

        assertNull(call.closedStatus);
        assertTrue(nextCalled.get());
    }

    @Test
    void emptyServerTokenRejectsProtectedRpc() {
        InternalAuthServerInterceptor failClosed = new InternalAuthServerInterceptor("");
        RecordingCall<?, ?> call = new RecordingCall<>(KVServiceGrpc.getShutdownMethod());
        Metadata headers = new Metadata();
        headers.put(InternalAuthToken.METADATA_KEY, TOKEN);
        AtomicBoolean nextCalled = new AtomicBoolean(false);

        failClosed.interceptCall(call, headers, markingHandler(nextCalled));

        assertEquals(Status.Code.UNAUTHENTICATED, call.closedStatus.getCode());
        assertTrue(!nextCalled.get());
    }

    private static <ReqT, RespT> ServerCallHandler<ReqT, RespT> markingHandler(AtomicBoolean nextCalled) {
        return (call, headers) -> {
            nextCalled.set(true);
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
