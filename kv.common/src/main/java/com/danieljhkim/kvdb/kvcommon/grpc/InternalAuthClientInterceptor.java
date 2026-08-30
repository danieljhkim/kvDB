package com.danieljhkim.kvdb.kvcommon.grpc;

import com.danieljhkim.kvdb.kvcommon.observability.CorrelationIdInterceptor;
import com.danieljhkim.kvdb.kvcommon.observability.CorrelationIds;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import java.util.Objects;

/**
 * Attaches the internal gRPC token to outgoing call metadata.
 */
public final class InternalAuthClientInterceptor implements ClientInterceptor {

    private final String token;

    public InternalAuthClientInterceptor(String token) {
        this.token = Objects.requireNonNull(token, "token");
        if (token.isBlank()) {
            throw new IllegalArgumentException("internal gRPC token must not be blank");
        }
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
        return new SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                headers.put(InternalAuthToken.METADATA_KEY, token);
                String correlationId = CorrelationIds.current();
                if (correlationId != null) {
                    headers.put(CorrelationIdInterceptor.HEADER, correlationId);
                }
                super.start(responseListener, headers);
            }
        };
    }
}
