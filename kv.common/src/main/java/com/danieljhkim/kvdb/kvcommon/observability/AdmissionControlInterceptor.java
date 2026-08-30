package com.danieljhkim.kvdb.kvcommon.observability;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

/** Rejects newly admitted application RPCs while preserving in-flight calls for graceful draining. */
public final class AdmissionControlInterceptor implements ServerInterceptor {

    private final ServiceLifecycle lifecycle;

    public AdmissionControlInterceptor(ServiceLifecycle lifecycle) {
        this.lifecycle = lifecycle;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        if (!lifecycle.tryAdmit()) {
            call.close(Status.UNAVAILABLE.withDescription("service is draining"), new Metadata());
            return new ServerCall.Listener<>() {};
        }
        return next.startCall(call, headers);
    }
}
