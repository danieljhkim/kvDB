package com.danieljhkim.kvdb.kvadmin.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.danieljhkim.kvdb.proto.coordinator.CoordinatorGrpc;
import com.danieljhkim.kvdb.proto.coordinator.GetCoordinatorLeaderRequest;
import com.danieljhkim.kvdb.proto.coordinator.GetCoordinatorLeaderResponse;
import com.danieljhkim.kvdb.proto.coordinator.ListNodesRequest;
import com.danieljhkim.kvdb.proto.coordinator.ListNodesResponse;
import com.danieljhkim.kvdb.proto.coordinator.RegisterNodeRequest;
import com.danieljhkim.kvdb.proto.coordinator.RegisterNodeResponse;
import com.kvdb.proto.kvstore.KVServiceGrpc;
import com.kvdb.proto.kvstore.PingRequest;
import com.kvdb.proto.kvstore.PingResponse;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class AdminGrpcDeadlineTest {

    private final List<Long> observedDeadlines = new CopyOnWriteArrayList<>();
    private final CountDownLatch requestReceived = new CountDownLatch(1);
    private final CountDownLatch requestCancelled = new CountDownLatch(1);
    private volatile boolean withholdResponse;
    private Server server;
    private ManagedChannel channel;
    private String address;

    @BeforeEach
    void startServer() throws Exception {
        // Like the existing admin client tests, run a server in this process over loopback.
        server = NettyServerBuilder.forPort(0)
                .addService(new CoordinatorGrpc.CoordinatorImplBase() {
                    @Override
                    public void getCoordinatorLeader(
                            GetCoordinatorLeaderRequest request,
                            StreamObserver<GetCoordinatorLeaderResponse> observer) {
                        reply(
                                observer,
                                GetCoordinatorLeaderResponse.newBuilder()
                                        .setIsLeader(true)
                                        .build(),
                                false);
                    }

                    @Override
                    public void listNodes(ListNodesRequest request, StreamObserver<ListNodesResponse> observer) {
                        reply(observer, ListNodesResponse.getDefaultInstance(), true);
                    }

                    @Override
                    public void registerNode(
                            RegisterNodeRequest request, StreamObserver<RegisterNodeResponse> observer) {
                        reply(observer, RegisterNodeResponse.getDefaultInstance(), true);
                    }
                })
                .addService(new KVServiceGrpc.KVServiceImplBase() {
                    @Override
                    public void ping(PingRequest request, StreamObserver<PingResponse> observer) {
                        reply(observer, PingResponse.getDefaultInstance(), true);
                    }
                })
                .build()
                .start();
        address = "localhost:" + server.getPort();
        channel = NettyChannelBuilder.forAddress("localhost", server.getPort())
                .usePlaintext()
                .build();
        // Establish the connection before measuring the per-RPC budget.
        KVServiceGrpc.newBlockingStub(channel)
                .withDeadlineAfter(5, TimeUnit.SECONDS)
                .ping(PingRequest.getDefaultInstance());
        observedDeadlines.clear();
    }

    @AfterEach
    void shutdown() throws InterruptedException {
        if (channel != null) {
            channel.shutdownNow();
            assertTrue(channel.awaitTermination(5, TimeUnit.SECONDS));
        }
        if (server != null) {
            server.shutdownNow();
            assertTrue(server.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @ParameterizedTest
    @ValueSource(longs = {500, 1500})
    void coordinatorReadPreservesMillisecondDeadline(long timeoutMillis) {
        CoordinatorReadClient client = new CoordinatorReadClient(
                List.of(address), timeoutMillis, TimeUnit.MILLISECONDS, (host, port) -> channel);
        try {
            assertTrue(client.listNodes().isEmpty());
            assertDeadlines(timeoutMillis, 2);
        } finally {
            client.shutdown();
        }
    }

    @ParameterizedTest
    @ValueSource(longs = {500, 1500})
    void coordinatorAdminPreservesMillisecondDeadline(long timeoutMillis) {
        CoordinatorAdminClient client = new CoordinatorAdminClient(
                List.of(address), timeoutMillis, TimeUnit.MILLISECONDS, (host, port) -> channel);
        try {
            assertEquals(RegisterNodeResponse.getDefaultInstance(), client.registerNode("node-a", address, "zone-a"));
            assertDeadlines(timeoutMillis, 2);
        } finally {
            client.shutdown();
        }
    }

    @ParameterizedTest
    @ValueSource(longs = {500, 1500})
    void nodePreservesMillisecondDeadline(long timeoutMillis) {
        NodeAdminClient client = new NodeAdminClient(timeoutMillis, TimeUnit.MILLISECONDS, (host, port) -> channel);
        try {
            assertTrue(client.ping(address));
            assertDeadlines(timeoutMillis, 1);
        } finally {
            client.close();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void withheldResponseExpiresAtSubSecondDeadline(boolean node) throws Exception {
        withholdResponse = true;
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CoordinatorReadClient readClient =
                new CoordinatorReadClient(List.of(address), 500, TimeUnit.MILLISECONDS, (host, port) -> channel);
        NodeAdminClient nodeClient = new NodeAdminClient(500, TimeUnit.MILLISECONDS, (host, port) -> channel);
        try {
            var result = executor.submit(() -> {
                if (node) {
                    assertFalse(nodeClient.ping(address));
                } else {
                    StatusRuntimeException error = assertThrows(StatusRuntimeException.class, readClient::listNodes);
                    assertEquals(
                            Status.Code.DEADLINE_EXCEEDED, error.getStatus().getCode());
                }
            });
            assertTrue(requestReceived.await(5, TimeUnit.SECONDS), "RPC must reach the server before expiring");
            result.get(5, TimeUnit.SECONDS);
            assertTrue(requestCancelled.await(5, TimeUnit.SECONDS), "Deadline must cancel the held server call");
            assertDeadlines(500, node ? 1 : 2);
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            readClient.shutdown();
            nodeClient.close();
        }
    }

    private <T> void reply(StreamObserver<T> observer, T response, boolean mayWithhold) {
        Context context = Context.current();
        observedDeadlines.add(context.getDeadline().timeRemaining(TimeUnit.MILLISECONDS));
        if (withholdResponse && mayWithhold) {
            context.addListener(ignored -> requestCancelled.countDown(), Runnable::run);
            requestReceived.countDown();
            return;
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    private void assertDeadlines(long timeoutMillis, int expectedCalls) {
        assertEquals(expectedCalls, observedDeadlines.size());
        for (long remainingMillis : observedDeadlines) {
            // Allow transport latency, while distinguishing 1500 ms from a truncated 1000 ms.
            assertTrue(remainingMillis > timeoutMillis - 250, "Remaining deadline: " + remainingMillis);
            assertTrue(remainingMillis <= timeoutMillis, "Remaining deadline: " + remainingMillis);
        }
    }
}
