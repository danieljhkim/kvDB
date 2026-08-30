package com.danieljhkim.kvdb.kvcommon.grpc;

import io.grpc.Context;

/** Authenticated peer identity made available to services and audit interceptors. */
public final class GrpcPeerIdentity {

    public static final Context.Key<GrpcIdentity> CURRENT = Context.key("kvdb-authenticated-peer");

    private GrpcPeerIdentity() {}

    public static GrpcIdentity require() {
        GrpcIdentity identity = CURRENT.get();
        if (identity == null) {
            throw new IllegalStateException("No authenticated gRPC peer in the current context");
        }
        return identity;
    }
}
