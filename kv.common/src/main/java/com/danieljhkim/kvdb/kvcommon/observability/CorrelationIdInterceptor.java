package com.danieljhkim.kvdb.kvcommon.observability;

import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import org.slf4j.MDC;

/**
 * Adds a generated or UUID-validated correlation id to structured log context for one RPC.
 *
 * <p>The id is installed only for the duration of each handler invocation ({@code startCall} and every listener
 * callback) and the previous thread state is restored afterwards, so interleaved calls and callbacks that hop threads
 * always observe their own id and unrelated executor work never sees it.
 */
public final class CorrelationIdInterceptor implements ServerInterceptor {

    public static final String HEADER_NAME = "x-kvdb-correlation-id";
    public static final Metadata.Key<String> HEADER = Metadata.Key.of(HEADER_NAME, Metadata.ASCII_STRING_MARSHALLER);

    private static final String MDC_KEY = "correlationId";

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        String correlationId = CorrelationIds.newOrValidated(headers.get(HEADER));
        ServerCall.Listener<ReqT> delegate;
        try (Scope ignored = Scope.install(correlationId)) {
            delegate = next.startCall(call, headers);
        }
        return new SimpleForwardingServerCallListener<>(delegate) {
            @Override
            public void onMessage(ReqT message) {
                try (Scope ignored = Scope.install(correlationId)) {
                    super.onMessage(message);
                }
            }

            @Override
            public void onHalfClose() {
                try (Scope ignored = Scope.install(correlationId)) {
                    super.onHalfClose();
                }
            }

            @Override
            public void onReady() {
                try (Scope ignored = Scope.install(correlationId)) {
                    super.onReady();
                }
            }

            @Override
            public void onComplete() {
                try (Scope ignored = Scope.install(correlationId)) {
                    super.onComplete();
                }
            }

            @Override
            public void onCancel() {
                try (Scope ignored = Scope.install(correlationId)) {
                    super.onCancel();
                }
            }
        };
    }

    /** Installs a correlation id on the current thread and restores the prior MDC and thread-local state on close. */
    private static final class Scope implements AutoCloseable {
        private final String previousMdc;
        private final String previousCurrent;

        private Scope(String previousMdc, String previousCurrent) {
            this.previousMdc = previousMdc;
            this.previousCurrent = previousCurrent;
        }

        static Scope install(String correlationId) {
            Scope scope = new Scope(MDC.get(MDC_KEY), CorrelationIds.current());
            CorrelationIds.set(correlationId);
            MDC.put(MDC_KEY, correlationId);
            return scope;
        }

        @Override
        public void close() {
            if (previousMdc == null) {
                MDC.remove(MDC_KEY);
            } else {
                MDC.put(MDC_KEY, previousMdc);
            }
            if (previousCurrent == null) {
                CorrelationIds.clear();
            } else {
                CorrelationIds.set(previousCurrent);
            }
        }
    }
}
