package com.danieljhkim.kvdb.kvcommon.observability;

import io.grpc.ForwardingServerCall.SimpleForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import java.util.concurrent.atomic.AtomicBoolean;

/** Records bounded per-method RPC outcome and latency metrics without inspecting request payloads. */
public final class RequestMetricsInterceptor implements ServerInterceptor {

    private final String service;
    private final ServiceLifecycle lifecycle;

    public RequestMetricsInterceptor(String service, ServiceLifecycle lifecycle) {
        this.service = service;
        this.lifecycle = lifecycle;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        long started = System.nanoTime();
        String method = call.getMethodDescriptor().getBareMethodName();
        Metrics.increment("kvdb_rpc_requests_started_total", service, method, "started");
        AtomicBoolean completed = new AtomicBoolean();
        ServerCall<ReqT, RespT> measuredCall = new SimpleForwardingServerCall<>(call) {
            @Override
            public void close(io.grpc.Status status, Metadata trailers) {
                if (completed.compareAndSet(false, true)) {
                    String outcome =
                            status.isOk() ? "ok" : status.getCode().name().toLowerCase();
                    Metrics.increment("kvdb_rpc_requests_total", service, method, outcome);
                    Metrics.observe(
                            "kvdb_rpc_duration_seconds",
                            service,
                            method,
                            (System.nanoTime() - started) / 1_000_000_000d);
                    lifecycle.complete();
                }
                super.close(status, trailers);
            }
        };
        return next.startCall(measuredCall, headers);
    }
}
