package com.danieljhkim.kvdb.kvadmin.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.danieljhkim.kvdb.proto.coordinator.CoordinatorGrpc;
import com.danieljhkim.kvdb.proto.coordinator.GetCoordinatorLeaderRequest;
import com.danieljhkim.kvdb.proto.coordinator.GetCoordinatorLeaderResponse;
import com.danieljhkim.kvdb.proto.coordinator.InitShardsRequest;
import com.danieljhkim.kvdb.proto.coordinator.InitShardsResponse;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CoordinatorAdminClientConcurrencyTest {

    private ExecutorService executor;
    private CoordinatorAdminClient client;
    private Server[] servers = new Server[0];

    @BeforeEach
    void startExecutor() {
        executor = Executors.newFixedThreadPool(2);
    }

    @AfterEach
    void shutdown() throws InterruptedException {
        executor.shutdownNow();
        executor.awaitTermination(5, TimeUnit.SECONDS);
        if (client != null) {
            client.shutdown();
        }
        for (Server server : servers) {
            server.shutdownNow();
        }
        for (Server server : servers) {
            server.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void concurrentLeaderDiscoveryDoesNotFailWhileRememberingAHint() throws Exception {
        CountDownLatch firstDiscoveryStarted = new CountDownLatch(1);
        CountDownLatch releaseFirstDiscovery = new CountDownLatch(1);
        AtomicInteger discoveryRequests = new AtomicInteger();
        String[] hint = new String[1];
        FakeCoordinatorService follower = new FakeCoordinatorService() {
            @Override
            public void getCoordinatorLeader(
                    GetCoordinatorLeaderRequest request,
                    StreamObserver<GetCoordinatorLeaderResponse> responseObserver) {
                GetCoordinatorLeaderResponse.Builder response =
                        GetCoordinatorLeaderResponse.newBuilder().setIsLeader(false);
                if (discoveryRequests.incrementAndGet() == 1) {
                    firstDiscoveryStarted.countDown();
                    try {
                        if (!releaseFirstDiscovery.await(5, TimeUnit.SECONDS)) {
                            responseObserver.onError(Status.DEADLINE_EXCEEDED.asRuntimeException());
                            return;
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        responseObserver.onError(Status.CANCELLED.asRuntimeException());
                        return;
                    }
                } else {
                    response.setLeaderAddress(hint[0]);
                }
                responseObserver.onNext(response.build());
                responseObserver.onCompleted();
            }
        };
        FakeCoordinatorService configuredLeader = new FakeCoordinatorService();
        FakeCoordinatorService hintedLeader = new FakeCoordinatorService();
        servers = new Server[] {start(follower), start(configuredLeader), start(hintedLeader)};
        String followerAddress = address(servers[0]);
        String configuredAddress = address(servers[1]);
        hint[0] = address(servers[2]);
        client = newClient(followerAddress, configuredAddress);

        // The first RPC is parked mid-iteration over the address list while the second remembers a hint.
        Future<InitShardsResponse> firstRequest = executor.submit(() -> client.initShards(1, 1));
        assertTrue(firstDiscoveryStarted.await(5, TimeUnit.SECONDS));
        Future<InitShardsResponse> secondRequest = executor.submit(() -> client.initShards(1, 1));
        secondRequest.get(5, TimeUnit.SECONDS);
        assertEquals(List.of(followerAddress, configuredAddress, hint[0]), client.coordinatorAddresses());

        releaseFirstDiscovery.countDown();
        firstRequest.get(5, TimeUnit.SECONDS);

        assertEquals(2, discoveryRequests.get());
        assertEquals(1, hintedLeader.initShardsCalls.get());
        assertEquals(1, configuredLeader.initShardsCalls.get());
        assertEquals(List.of(followerAddress, configuredAddress, hint[0]), client.coordinatorAddresses());
    }

    @Test
    void overlappingRetriesRememberTheSameLeaderHintOnce() throws Exception {
        CountDownLatch bothRejected = new CountDownLatch(2);
        AtomicInteger initRequests = new AtomicInteger();
        String[] hint = new String[1];
        FakeCoordinatorService staleLeader = new FakeCoordinatorService() {
            @Override
            public void initShards(InitShardsRequest request, StreamObserver<InitShardsResponse> responseObserver) {
                if (initRequests.incrementAndGet() > 2) {
                    super.initShards(request, responseObserver);
                    return;
                }
                // Hold both rejections until both RPCs are in flight so they remember the hint together.
                bothRejected.countDown();
                try {
                    if (!bothRejected.await(5, TimeUnit.SECONDS)) {
                        responseObserver.onError(Status.DEADLINE_EXCEEDED.asRuntimeException());
                        return;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    responseObserver.onError(Status.CANCELLED.asRuntimeException());
                    return;
                }
                responseObserver.onError(Status.FAILED_PRECONDITION
                        .withDescription("Not the leader. Leader hint: " + hint[0])
                        .asRuntimeException());
            }
        };
        FakeCoordinatorService newLeader = new FakeCoordinatorService();
        servers = new Server[] {start(staleLeader), start(newLeader)};
        String staleAddress = address(servers[0]);
        hint[0] = address(servers[1]);
        client = newClient(staleAddress);

        Future<InitShardsResponse> first = executor.submit(() -> client.initShards(1, 1));
        Future<InitShardsResponse> second = executor.submit(() -> client.initShards(1, 1));
        first.get(10, TimeUnit.SECONDS);
        second.get(10, TimeUnit.SECONDS);

        assertEquals(List.of(staleAddress, hint[0]), client.coordinatorAddresses());
        assertEquals(hint[0], client.getLeaderAddress());
    }

    private static Server start(FakeCoordinatorService service) throws Exception {
        return NettyServerBuilder.forPort(0).addService(service).build().start();
    }

    private static String address(Server server) {
        return "localhost:" + server.getPort();
    }

    private static CoordinatorAdminClient newClient(String... addresses) {
        return new CoordinatorAdminClient(
                List.of(addresses),
                5,
                TimeUnit.SECONDS,
                (host, port) -> NettyChannelBuilder.forAddress(host, port)
                        .usePlaintext()
                        .build());
    }

    private static class FakeCoordinatorService extends CoordinatorGrpc.CoordinatorImplBase {
        final AtomicInteger initShardsCalls = new AtomicInteger();

        @Override
        public void getCoordinatorLeader(
                GetCoordinatorLeaderRequest request, StreamObserver<GetCoordinatorLeaderResponse> responseObserver) {
            responseObserver.onNext(
                    GetCoordinatorLeaderResponse.newBuilder().setIsLeader(true).build());
            responseObserver.onCompleted();
        }

        @Override
        public void initShards(InitShardsRequest request, StreamObserver<InitShardsResponse> responseObserver) {
            initShardsCalls.incrementAndGet();
            responseObserver.onNext(InitShardsResponse.getDefaultInstance());
            responseObserver.onCompleted();
        }
    }
}
