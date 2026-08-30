package com.danieljhkim.kvdb.kvcommon.observability;

import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import org.slf4j.MDC;

/** Adds a generated or UUID-validated correlation id to structured log context for one RPC. */
public final class CorrelationIdInterceptor implements ServerInterceptor {

    public static final String HEADER_NAME = "x-kvdb-correlation-id";
    public static final Metadata.Key<String> HEADER = Metadata.Key.of(HEADER_NAME, Metadata.ASCII_STRING_MARSHALLER);

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        String correlationId = CorrelationIds.newOrValidated(headers.get(HEADER));
        CorrelationIds.set(correlationId);
        MDC.put("correlationId", correlationId);
        ServerCall.Listener<ReqT> delegate = next.startCall(call, headers);
        return new SimpleForwardingServerCallListener<>(delegate) {
            @Override
            public void onComplete() {
                try {
                    super.onComplete();
                } finally {
                    clear();
                }
            }

            @Override
            public void onCancel() {
                try {
                    super.onCancel();
                } finally {
                    clear();
                }
            }

            private void clear() {
                MDC.remove("correlationId");
                CorrelationIds.clear();
            }
        };
    }
}
