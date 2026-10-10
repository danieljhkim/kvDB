package com.danieljhkim.kvdb.kvcommon.observability;

import io.grpc.ForwardingServerCall.SimpleForwardingServerCall;
import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Rejects newly admitted application RPCs while preserving in-flight calls for graceful draining. Each admitted call
 * owns exactly one lifecycle token, released once when the call closes or is cancelled; rejected calls never hold one.
 */
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
        AtomicBoolean released = new AtomicBoolean();
        Runnable release = () -> {
            if (released.compareAndSet(false, true)) {
                lifecycle.complete();
            }
        };
        ServerCall<ReqT, RespT> admittedCall = new SimpleForwardingServerCall<>(call) {
            @Override
            public void close(Status status, Metadata trailers) {
                release.run();
                super.close(status, trailers);
            }
        };
        ServerCall.Listener<ReqT> listener;
        try {
            listener = next.startCall(admittedCall, headers);
        } catch (RuntimeException e) {
            release.run();
            throw e;
        }
        return new SimpleForwardingServerCallListener<>(listener) {
            @Override
            public void onCancel() {
                release.run();
                super.onCancel();
            }
        };
    }
}
