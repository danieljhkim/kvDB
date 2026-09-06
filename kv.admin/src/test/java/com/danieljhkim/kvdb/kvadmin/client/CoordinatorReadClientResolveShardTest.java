package com.danieljhkim.kvdb.kvadmin.client;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.danieljhkim.kvdb.kvadmin.api.dto.ShardDto;
import com.danieljhkim.kvdb.proto.coordinator.CoordinatorGrpc;
import com.danieljhkim.kvdb.proto.coordinator.GetCoordinatorLeaderRequest;
import com.danieljhkim.kvdb.proto.coordinator.GetCoordinatorLeaderResponse;
import com.danieljhkim.kvdb.proto.coordinator.ResolveShardRequest;
import com.danieljhkim.kvdb.proto.coordinator.ResolveShardResponse;
import com.danieljhkim.kvdb.proto.coordinator.ShardConfigState;
import com.danieljhkim.kvdb.proto.coordinator.ShardRecord;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CoordinatorReadClientResolveShardTest {

    private static final ShardRecord PLACEMENT = ShardRecord.newBuilder()
            .setShardId("shard-7")
            .setEpoch(42)
            .addReplicas("node-a")
            .addReplicas("node-b")
            .setLeader("node-a")
            .setConfigState(ShardConfigState.STABLE)
            .build();

    private FakeCoordinatorService service;
    private Server server;
    private CoordinatorReadClient client;

    @BeforeEach
    void startCoordinator() throws Exception {
        service = new FakeCoordinatorService();
        server = NettyServerBuilder.forPort(0).addService(service).build().start();
        client = new CoordinatorReadClient(
                List.of("localhost:" + server.getPort()),
                1,
                TimeUnit.SECONDS,
                (host, port) -> NettyChannelBuilder.forAddress(host, port)
                        .usePlaintext()
                        .build());
    }

    @AfterEach
    void shutdown() throws InterruptedException {
        if (client != null) {
            client.shutdown();
        }
        if (server != null) {
            server.shutdownNow();
            server.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void resolveShardForwardsBinaryKeyIncludingNulAndInvalidUtf8() {
        byte[] key = new byte[] {0x00, (byte) 0xFF, (byte) 0xFE, 'k'};

        ShardDto placement = client.resolveShard(key);

        assertArrayEquals(key, service.lastKey.get());
        assertEquals("shard-7", placement.getShardId());
        assertEquals(42, placement.getEpoch());
        assertEquals("node-a", placement.getLeader());
        assertEquals(List.of("node-a", "node-b"), placement.getReplicas());
        assertEquals("STABLE", placement.getConfigState());
    }

    @Test
    void resolveShardSurfacesUnavailable() {
        service.resolveStatus.set(Status.UNAVAILABLE.withDescription("coordinator down"));

        StatusRuntimeException thrown =
                assertThrows(StatusRuntimeException.class, () -> client.resolveShard(new byte[] {0x01}));
        assertEquals(Status.Code.UNAVAILABLE, thrown.getStatus().getCode());
    }

    @Test
    void resolveShardSurfacesDeadlineExceeded() {
        service.delayMs.set(TimeUnit.SECONDS.toMillis(2));

        StatusRuntimeException thrown =
                assertThrows(StatusRuntimeException.class, () -> client.resolveShard(new byte[] {0x01}));
        assertEquals(Status.Code.DEADLINE_EXCEEDED, thrown.getStatus().getCode());
    }

    @Test
    void resolveShardRejectsInvalidHintsAndRecoversFromStaleLeader() throws Exception {
        FakeCoordinatorService recoveredLeader = new FakeCoordinatorService();
        Server recoveredServer = NettyServerBuilder.forPort(0)
                .addService(recoveredLeader)
                .build()
                .start();
        try {
            String recoveredAddress = "localhost:" + recoveredServer.getPort();
            for (String invalidHint : List.of(
                    "null", " ", "", "missing-port", "localhost:not-a-port", "localhost:0", "localhost:65536")) {
                service.isLeader.set(true);
                service.leaderHint.set(invalidHint);
                service.resolveStatus.set(Status.UNAVAILABLE.withDescription("Leader hint: " + invalidHint));
                service.becomeFollowerAfterResolveFailure.set(true);

                client.shutdown();
                client = new CoordinatorReadClient(
                        List.of("localhost:" + server.getPort(), recoveredAddress),
                        1,
                        TimeUnit.SECONDS,
                        (host, port) -> NettyChannelBuilder.forAddress(host, port)
                                .usePlaintext()
                                .build());

                ShardDto placement = client.resolveShard(new byte[] {0x01});

                assertEquals("shard-7", placement.getShardId(), "hint: " + invalidHint);
                service.resolveStatus.set(Status.OK);
                service.becomeFollowerAfterResolveFailure.set(false);
            }
        } finally {
            recoveredServer.shutdownNow();
            recoveredServer.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void resolveShardUsesValidLeaderHint() throws Exception {
        FakeCoordinatorService hintedLeader = new FakeCoordinatorService();
        Server hintedServer =
                NettyServerBuilder.forPort(0).addService(hintedLeader).build().start();
        try {
            String hintedAddress = "127.0.0.1:" + hintedServer.getPort();
            service.isLeader.set(false);
            service.leaderHint.set(hintedAddress);

            client.shutdown();
            client = new CoordinatorReadClient(
                    List.of("localhost:" + server.getPort()),
                    1,
                    TimeUnit.SECONDS,
                    (host, port) -> NettyChannelBuilder.forAddress(host, port)
                            .usePlaintext()
                            .build());

            ShardDto placement = client.resolveShard(new byte[] {0x02});

            assertEquals("shard-7", placement.getShardId());
        } finally {
            hintedServer.shutdownNow();
            hintedServer.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void concurrentLeaderDiscoveryDoesNotFailWhileRememberingAHint() throws Exception {
        CountDownLatch firstDiscoveryStarted = new CountDownLatch(1);
        CountDownLatch releaseFirstDiscovery = new CountDownLatch(1);
        AtomicInteger discoveryRequests = new AtomicInteger();
        AtomicReference<String> concurrentHint = new AtomicReference<>();
        FakeCoordinatorService follower = new FakeCoordinatorService() {
            @Override
            public void getCoordinatorLeader(
                    GetCoordinatorLeaderRequest request,
                    StreamObserver<GetCoordinatorLeaderResponse> responseObserver) {
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
                    responseObserver.onNext(GetCoordinatorLeaderResponse.newBuilder()
                            .setIsLeader(false)
                            .build());
                } else {
                    responseObserver.onNext(GetCoordinatorLeaderResponse.newBuilder()
                            .setIsLeader(false)
                            .setLeaderAddress(concurrentHint.get())
                            .build());
                }
                responseObserver.onCompleted();
            }
        };
        FakeCoordinatorService configuredLeader = new FakeCoordinatorService();
        FakeCoordinatorService hintedLeader = new FakeCoordinatorService();
        Server followerServer =
                NettyServerBuilder.forPort(0).addService(follower).build().start();
        Server configuredLeaderServer = NettyServerBuilder.forPort(0)
                .addService(configuredLeader)
                .build()
                .start();
        Server hintedLeaderServer =
                NettyServerBuilder.forPort(0).addService(hintedLeader).build().start();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            concurrentHint.set("localhost:" + hintedLeaderServer.getPort());
            client.shutdown();
            client = new CoordinatorReadClient(
                    List.of("localhost:" + followerServer.getPort(), "localhost:" + configuredLeaderServer.getPort()),
                    5,
                    TimeUnit.SECONDS,
                    (host, port) -> NettyChannelBuilder.forAddress(host, port)
                            .usePlaintext()
                            .build());

            Future<ShardDto> firstRequest = executor.submit(() -> client.resolveShard(new byte[] {0x01}));
            assertTrue(firstDiscoveryStarted.await(5, TimeUnit.SECONDS));
            Future<ShardDto> secondRequest = executor.submit(() -> client.resolveShard(new byte[] {0x02}));
            assertEquals("shard-7", secondRequest.get(5, TimeUnit.SECONDS).getShardId());

            releaseFirstDiscovery.countDown();
            assertEquals("shard-7", firstRequest.get(5, TimeUnit.SECONDS).getShardId());
            assertEquals(2, discoveryRequests.get());
        } finally {
            releaseFirstDiscovery.countDown();
            executor.shutdownNow();
            executor.awaitTermination(5, TimeUnit.SECONDS);
            followerServer.shutdownNow();
            configuredLeaderServer.shutdownNow();
            hintedLeaderServer.shutdownNow();
            followerServer.awaitTermination(5, TimeUnit.SECONDS);
            configuredLeaderServer.awaitTermination(5, TimeUnit.SECONDS);
            hintedLeaderServer.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static class FakeCoordinatorService extends CoordinatorGrpc.CoordinatorImplBase {
        private final AtomicReference<byte[]> lastKey = new AtomicReference<>();
        private final AtomicReference<Status> resolveStatus = new AtomicReference<>(Status.OK);
        private final AtomicLong delayMs = new AtomicLong();
        private final AtomicBoolean isLeader = new AtomicBoolean(true);
        private final AtomicReference<String> leaderHint = new AtomicReference<>("");
        private final AtomicBoolean becomeFollowerAfterResolveFailure = new AtomicBoolean();

        @Override
        public void getCoordinatorLeader(
                GetCoordinatorLeaderRequest request, StreamObserver<GetCoordinatorLeaderResponse> responseObserver) {
            responseObserver.onNext(GetCoordinatorLeaderResponse.newBuilder()
                    .setIsLeader(isLeader.get())
                    .setLeaderAddress(leaderHint.get())
                    .build());
            responseObserver.onCompleted();
        }

        @Override
        public void resolveShard(ResolveShardRequest request, StreamObserver<ResolveShardResponse> responseObserver) {
            lastKey.set(request.getKey().toByteArray());
            long delay = delayMs.get();
            if (delay > 0) {
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    responseObserver.onError(Status.CANCELLED.asRuntimeException());
                    return;
                }
            }
            Status status = resolveStatus.get();
            if (!status.isOk()) {
                responseObserver.onError(status.asRuntimeException());
                if (becomeFollowerAfterResolveFailure.get()) {
                    isLeader.set(false);
                }
                return;
            }
            responseObserver.onNext(ResolveShardResponse.newBuilder()
                    .setShardId(PLACEMENT.getShardId())
                    .setShard(PLACEMENT)
                    .build());
            responseObserver.onCompleted();
        }
    }
}
