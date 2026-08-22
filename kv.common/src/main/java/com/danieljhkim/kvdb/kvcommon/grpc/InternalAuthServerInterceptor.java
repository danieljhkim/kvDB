package com.danieljhkim.kvdb.kvcommon.grpc;

import com.danieljhkim.kvdb.proto.coordinator.CoordinatorGrpc;
import com.danieljhkim.kvdb.proto.raft.RaftServiceGrpc;
import com.kvdb.proto.kvstore.KVServiceGrpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.util.Objects;
import java.util.Set;

/**
 * Rejects unauthenticated calls to control-plane mutations, data-plane writes, replication, shutdown,
 * and Raft RPCs. Read/health methods (Get, Ping, GetCoordinatorLeader, etc.) stay unauthenticated.
 */
public final class InternalAuthServerInterceptor implements ServerInterceptor {

    static final Set<String> PROTECTED_METHODS = Set.of(
            CoordinatorGrpc.getRegisterNodeMethod().getFullMethodName(),
            CoordinatorGrpc.getInitShardsMethod().getFullMethodName(),
            CoordinatorGrpc.getSetNodeStatusMethod().getFullMethodName(),
            CoordinatorGrpc.getSetShardReplicasMethod().getFullMethodName(),
            CoordinatorGrpc.getSetShardLeaderMethod().getFullMethodName(),
            CoordinatorGrpc.getReportShardLeaderMethod().getFullMethodName(),
            KVServiceGrpc.getSetMethod().getFullMethodName(),
            KVServiceGrpc.getDeleteMethod().getFullMethodName(),
            KVServiceGrpc.getReplicateSetMethod().getFullMethodName(),
            KVServiceGrpc.getReplicateDeleteMethod().getFullMethodName(),
            KVServiceGrpc.getShutdownMethod().getFullMethodName(),
            RaftServiceGrpc.getRequestVoteMethod().getFullMethodName(),
            RaftServiceGrpc.getAppendEntriesMethod().getFullMethodName(),
            RaftServiceGrpc.getInstallSnapshotMethod().getFullMethodName(),
            RaftServiceGrpc.getAddServerMethod().getFullMethodName(),
            RaftServiceGrpc.getRemoveServerMethod().getFullMethodName());

    private static final Status UNAUTHENTICATED =
            Status.UNAUTHENTICATED.withDescription("Missing or invalid internal gRPC token");

    private final String expectedToken;

    public InternalAuthServerInterceptor(String expectedToken) {
        this.expectedToken = Objects.requireNonNullElse(expectedToken, "");
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        if (!PROTECTED_METHODS.contains(call.getMethodDescriptor().getFullMethodName())) {
            return next.startCall(call, headers);
        }

        String provided = headers.get(InternalAuthToken.METADATA_KEY);
        if (!InternalAuthToken.matches(expectedToken, provided)) {
            call.close(UNAUTHENTICATED, new Metadata());
            return new ServerCall.Listener<>() {};
        }
        return next.startCall(call, headers);
    }
}
