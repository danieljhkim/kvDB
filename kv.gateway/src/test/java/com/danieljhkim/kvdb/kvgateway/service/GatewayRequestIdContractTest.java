package com.danieljhkim.kvdb.kvgateway.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.danieljhkim.kvdb.kvcommon.cache.ShardMapCache;
import com.danieljhkim.kvdb.kvcommon.grpc.GlobalExceptionInterceptor;
import com.danieljhkim.kvdb.kvcommon.grpc.GrpcIdentity;
import com.danieljhkim.kvdb.kvcommon.grpc.GrpcPeerIdentity;
import com.danieljhkim.kvdb.kvgateway.cache.NodeFailureTracker;
import com.danieljhkim.kvdb.kvgateway.client.NodeConnectionPool;
import com.danieljhkim.kvdb.kvgateway.retry.RequestExecutor;
import com.danieljhkim.kvdb.kvgateway.retry.RetryPolicy;
import com.danieljhkim.kvdb.kvnode.service.KVServiceImpl;
import com.danieljhkim.kvdb.kvnode.storage.ShardStoreRegistry;
import com.danieljhkim.kvdb.proto.coordinator.ClusterState;
import com.danieljhkim.kvdb.proto.coordinator.NodeRecord;
import com.danieljhkim.kvdb.proto.coordinator.NodeStatus;
import com.danieljhkim.kvdb.proto.coordinator.PartitioningConfig;
import com.danieljhkim.kvdb.proto.coordinator.ShardRecord;
import com.danieljhkim.kvdb.proto.gateway.DeleteRequest;
import com.danieljhkim.kvdb.proto.gateway.GetRequest;
import com.danieljhkim.kvdb.proto.gateway.KvGatewayGrpc;
import com.danieljhkim.kvdb.proto.gateway.PutRequest;
import com.danieljhkim.kvdb.proto.gateway.PutResponse;
import com.danieljhkim.kvdb.proto.gateway.RequestContext;
import com.danieljhkim.kvdb.proto.gateway.Status;
import com.danieljhkim.kvdb.proto.gateway.WriteOptions;
import com.google.protobuf.ByteString;
import com.kvdb.proto.kvstore.KVServiceGrpc;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Real gateway and storage RPCs, durable shard journal, and the production retry executor and exception mapper. */
@Timeout(20)
class GatewayRequestIdContractTest {

    @TempDir
    Path tempDir;

    private final AtomicInteger nodeCalls = new AtomicInteger();
    private ShardStoreRegistry registry;
    private KVServiceImpl node;
    private Server nodeServer;
    private Server gatewayServer;
    private ManagedChannel nodeChannel;
    private ManagedChannel gatewayChannel;
    private KvGatewayGrpc.KvGatewayBlockingStub gateway;

    @BeforeEach
    void setUp() throws Exception {
        ShardMapCache cache = new ShardMapCache();
        cache.refreshFromFullState(ClusterState.newBuilder()
                .setMapVersion(1)
                .setPartitioning(PartitioningConfig.newBuilder().setNumShards(1).setReplicationFactor(1))
                .putNodes(
                        "node-1",
                        NodeRecord.newBuilder()
                                .setNodeId("node-1")
                                .setAddress("fixture:9000")
                                .setStatus(NodeStatus.ALIVE)
                                .build())
                .putShards(
                        "shard-0",
                        ShardRecord.newBuilder()
                                .setShardId("shard-0")
                                .setEpoch(1)
                                .setLeader("node-1")
                                .addReplicas("node-1")
                                .build())
                .build());
        registry = new ShardStoreRegistry(tempDir.toString(), "snapshot.json", "wal.log", 100, false);
        node = new KVServiceImpl("node-1", cache, registry, null, Duration.ofMillis(100));
        ServerInterceptor counter = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
                nodeCalls.incrementAndGet();
                return next.startCall(call, headers);
            }
        };
        nodeServer = ServerBuilder.forPort(0)
                .addService(ServerInterceptors.intercept(node, counter, new GlobalExceptionInterceptor()))
                .build()
                .start();
        nodeChannel = channel(nodeServer);
        NodeConnectionPool pool = new NodeConnectionPool() {
            @Override
            public KVServiceGrpc.KVServiceBlockingStub getStub(String address) {
                return KVServiceGrpc.newBlockingStub(nodeChannel);
            }
        };
        RequestExecutor executor = new RequestExecutor(pool, new NodeFailureTracker(), RetryPolicy.defaults(), 1_000);
        ServerInterceptor identity = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
                return Contexts.interceptCall(
                        Context.current()
                                .withValue(
                                        GrpcPeerIdentity.CURRENT,
                                        new GrpcIdentity(GrpcIdentity.Role.EXTERNAL_CLIENT, "test", "contract")),
                        call,
                        headers,
                        next);
            }
        };
        gatewayServer = ServerBuilder.forPort(0)
                .addService(ServerInterceptors.intercept(new KvGatewayServiceImpl(cache, executor), identity))
                .build()
                .start();
        gatewayChannel = channel(gatewayServer);
        gateway = KvGatewayGrpc.newBlockingStub(gatewayChannel).withDeadlineAfter(10, TimeUnit.SECONDS);
    }

    @AfterEach
    void tearDown() throws Exception {
        for (ManagedChannel channel : new ManagedChannel[] {gatewayChannel, nodeChannel}) {
            if (channel != null) {
                channel.shutdownNow();
                assertTrue(channel.awaitTermination(5, TimeUnit.SECONDS));
            }
        }
        for (Server server : new Server[] {gatewayServer, nodeServer}) {
            if (server != null) {
                server.shutdownNow();
                assertTrue(server.awaitTermination(5, TimeUnit.SECONDS));
            }
        }
        if (node != null) {
            node.shutdownReplication();
        }
        if (registry != null) {
            registry.shutdown();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void conflictingPutIdentityIsNonRetryableAndOriginalMutationIsPreserved(boolean replaySafe) {
        PutRequest original = put("put-id", "key", "v1", replaySafe);
        PutResponse first = gateway.put(original);
        assertEquals(Status.Code.OK, first.getStatus().getCode());
        assertTrue(first.getVersion() > 0);
        assertEquals(first, gateway.put(original));

        assertConflict(() ->
                gateway.put(original.toBuilder().setKey(bytes("other")).build()).getStatus());
        assertConflict(() ->
                gateway.put(original.toBuilder().setValue(bytes("v2")).build()).getStatus());
        assertConflict(() -> gateway.delete(delete("put-id", "key", replaySafe)).getStatus());
        assertConflict(() -> gateway.put(original.toBuilder()
                        .setOptions(original.getOptions().toBuilder().setTtlMs(5_000))
                        .build())
                .getStatus());
        assertConflict(() -> gateway.put(original.toBuilder()
                        .setOptions(original.getOptions().toBuilder().setIfVersionEquals(first.getVersion()))
                        .build())
                .getStatus());
        assertConflict(() -> gateway.put(original.toBuilder()
                        .setOptions(original.getOptions().toBuilder().setIfNotExists(true))
                        .build())
                .getStatus());

        var read = gateway.get(GetRequest.newBuilder().setKey(bytes("key")).build());
        assertEquals(Status.Code.OK, read.getStatus().getCode());
        assertEquals(bytes("v1"), read.getKv().getValue());
        assertEquals(first.getVersion(), read.getKv().getVersion());
        assertEquals(
                Status.Code.NOT_FOUND,
                gateway.get(GetRequest.newBuilder().setKey(bytes("other")).build())
                        .getStatus()
                        .getCode());
        assertEquals(first, gateway.put(original));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void conflictingDeleteIdentityIsNonRetryableAndIdenticalReplayReturnsOriginalVersion(boolean replaySafe) {
        gateway.put(put("initial-put", "key", "v1", replaySafe));
        DeleteRequest original = delete("delete-id", "key", replaySafe);
        var first = gateway.delete(original);
        assertEquals(Status.Code.OK, first.getStatus().getCode());
        assertTrue(first.getVersion() > 0);
        assertEquals(first, gateway.delete(original));
        assertConflict(
                () -> gateway.delete(original.toBuilder().setKey(bytes("other")).build())
                        .getStatus());
        assertConflict(
                () -> gateway.put(put("delete-id", "key", "v2", replaySafe)).getStatus());

        PutResponse newer = gateway.put(put("new-put", "key", "new", replaySafe));
        assertEquals(Status.Code.OK, newer.getStatus().getCode());
        assertEquals(first, gateway.delete(original));
        var read = gateway.get(GetRequest.newBuilder().setKey(bytes("key")).build());
        assertEquals(Status.Code.OK, read.getStatus().getCode());
        assertEquals(bytes("new"), read.getKv().getValue());
        assertEquals(newer.getVersion(), read.getKv().getVersion());
    }

    private void assertConflict(Supplier<Status> operation) {
        int before = nodeCalls.get();
        Status status = operation.get();
        assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
        assertEquals("request_id was already used for a different mutation", status.getMessage());
        assertEquals("shard-0", status.getShardId());
        assertFalse(RetryPolicy.defaults().isRetryable(io.grpc.Status.Code.INVALID_ARGUMENT));
        assertEquals(before + 1, nodeCalls.get(), "A permanent conflict must not be replayed by the gateway");
    }

    private static ManagedChannel channel(Server server) {
        return ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort())
                .usePlaintext()
                .build();
    }

    private static PutRequest put(String id, String key, String value, boolean replaySafe) {
        return PutRequest.newBuilder()
                .setCtx(RequestContext.newBuilder().setRequestId(id))
                .setKey(bytes(key))
                .setValue(bytes(value))
                .setOptions(WriteOptions.newBuilder().setRequireIdempotency(replaySafe))
                .build();
    }

    private static DeleteRequest delete(String id, String key, boolean replaySafe) {
        return DeleteRequest.newBuilder()
                .setCtx(RequestContext.newBuilder().setRequestId(id))
                .setKey(bytes(key))
                .setOptions(WriteOptions.newBuilder().setRequireIdempotency(replaySafe))
                .build();
    }

    private static ByteString bytes(String value) {
        return ByteString.copyFromUtf8(value);
    }
}
