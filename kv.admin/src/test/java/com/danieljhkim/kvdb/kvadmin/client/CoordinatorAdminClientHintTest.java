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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class CoordinatorAdminClientHintTest {

    private final List<Server> servers = new ArrayList<>();
    private final List<String> dialedAddresses = new ArrayList<>();
    private CoordinatorAdminClient client;

    @AfterEach
    void shutdown() throws InterruptedException {
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

    static Stream<String> invalidHints() {
        return Stream.of(
                null,
                "null",
                "",
                " ",
                "missing-port",
                "localhost:not-a-port",
                "localhost:0",
                "localhost:65536",
                "metadata.internal:80:443",
                "localhost:",
                ":9003",
                "localhost:-1",
                "localhost:+80",
                "localhost:000080",
                "localhost:８０",
                "bad_host:9003",
                "-host:9003",
                "host.:9003",
                "local host:9003",
                "a".repeat(64) + ":9003");
    }

    @ParameterizedTest
    @MethodSource("invalidHints")
    void discoveryIgnoresInvalidHintAndUsesConfiguredLeader(String hint) throws Exception {
        FakeCoordinatorService follower = new FakeCoordinatorService();
        follower.isLeader.set(false);
        follower.hint.set(hint);
        FakeCoordinatorService leader = new FakeCoordinatorService();
        String followerAddress = start(follower);
        String leaderAddress = start(leader);
        client = newClient(followerAddress, leaderAddress);

        assertEquals(InitShardsResponse.getDefaultInstance(), client.initShards(1, 1));

        assertEquals(List.of(followerAddress, leaderAddress), dialedAddresses);
        assertEquals(List.of(followerAddress, leaderAddress), client.coordinatorAddresses());
        assertEquals(leaderAddress, client.getLeaderAddress());
        assertEquals(0, follower.initShardsCalls.get());
        assertEquals(1, leader.initShardsCalls.get());
    }

    @ParameterizedTest
    @MethodSource("invalidHints")
    void retryIgnoresInvalidStatusHintAndRediscoversLeader(String hint) throws Exception {
        FakeCoordinatorService staleLeader = new FakeCoordinatorService();
        staleLeader.hint.set(hint);
        staleLeader.rejectMutation.set(true);
        FakeCoordinatorService leader = new FakeCoordinatorService();
        String staleAddress = start(staleLeader);
        String leaderAddress = start(leader);
        client = newClient(staleAddress, leaderAddress);

        assertEquals(InitShardsResponse.getDefaultInstance(), client.initShards(1, 1));

        assertEquals(List.of(staleAddress, leaderAddress), dialedAddresses);
        assertEquals(List.of(staleAddress, leaderAddress), client.coordinatorAddresses());
        assertEquals(leaderAddress, client.getLeaderAddress());
        assertEquals(1, staleLeader.initShardsCalls.get());
        assertEquals(1, leader.initShardsCalls.get());
    }

    @Test
    void discoveryRejectsHintWithSurroundingWhitespace() throws Exception {
        FakeCoordinatorService follower = new FakeCoordinatorService();
        follower.isLeader.set(false);
        FakeCoordinatorService leader = new FakeCoordinatorService();
        String followerAddress = start(follower);
        String leaderAddress = start(leader);
        follower.hint.set(" " + leaderAddress + " ");
        client = newClient(followerAddress, leaderAddress);

        client.initShards(1, 1);

        assertEquals(List.of(followerAddress, leaderAddress), dialedAddresses);
        assertEquals(List.of(followerAddress, leaderAddress), client.coordinatorAddresses());
    }

    @Test
    void discoveryVerifiesAndUsesValidHint() throws Exception {
        FakeCoordinatorService follower = new FakeCoordinatorService();
        follower.isLeader.set(false);
        FakeCoordinatorService leader = new FakeCoordinatorService();
        String followerAddress = start(follower);
        String leaderAddress = start(leader);
        follower.hint.set(leaderAddress);
        client = newClient(followerAddress);

        client.initShards(1, 1);

        assertEquals(List.of(followerAddress, leaderAddress), dialedAddresses);
        assertEquals(List.of(followerAddress, leaderAddress), client.coordinatorAddresses());
        assertEquals(leaderAddress, client.getLeaderAddress());
        assertEquals(1, leader.discoveryCalls.get());
        assertEquals(1, leader.initShardsCalls.get());
        assertTrue(leader.verifiedBeforeMutation.get());
    }

    @Test
    void retryTrimsVerifiesAndUsesValidStatusHint() throws Exception {
        FakeCoordinatorService staleLeader = new FakeCoordinatorService();
        staleLeader.rejectMutation.set(true);
        FakeCoordinatorService leader = new FakeCoordinatorService();
        String staleAddress = start(staleLeader);
        String leaderAddress = start(leader);
        staleLeader.hint.set(" " + leaderAddress + " ");
        client = newClient(staleAddress);

        client.initShards(1, 1);

        assertEquals(List.of(staleAddress, leaderAddress), dialedAddresses);
        assertEquals(List.of(staleAddress, leaderAddress), client.coordinatorAddresses());
        assertEquals(leaderAddress, client.getLeaderAddress());
        assertEquals(1, leader.discoveryCalls.get());
        assertEquals(1, leader.initShardsCalls.get());
        assertTrue(leader.verifiedBeforeMutation.get());
    }

    @Test
    void discoveryDoesNotSendMutationToUnverifiedHint() throws Exception {
        FakeCoordinatorService follower = new FakeCoordinatorService();
        follower.isLeader.set(false);
        FakeCoordinatorService hintedFollower = new FakeCoordinatorService();
        hintedFollower.isLeader.set(false);
        FakeCoordinatorService leader = new FakeCoordinatorService();
        String followerAddress = start(follower);
        String hintAddress = start(hintedFollower);
        String leaderAddress = start(leader);
        follower.hint.set(hintAddress);
        client = newClient(followerAddress, leaderAddress);

        client.initShards(1, 1);

        assertEquals(List.of(followerAddress, hintAddress, leaderAddress), dialedAddresses);
        assertEquals(List.of(followerAddress, leaderAddress), client.coordinatorAddresses());
        assertEquals(0, hintedFollower.initShardsCalls.get());
        assertEquals(1, leader.initShardsCalls.get());
    }

    private String start(FakeCoordinatorService service) throws Exception {
        Server server =
                NettyServerBuilder.forPort(0).addService(service).build().start();
        servers.add(server);
        return "127.0.0.1:" + server.getPort();
    }

    private CoordinatorAdminClient newClient(String... addresses) {
        return new CoordinatorAdminClient(List.of(addresses), 5, TimeUnit.SECONDS, (host, port) -> {
            dialedAddresses.add(host + ":" + port);
            return NettyChannelBuilder.forAddress(host, port).usePlaintext().build();
        });
    }

    private static class FakeCoordinatorService extends CoordinatorGrpc.CoordinatorImplBase {
        final AtomicBoolean isLeader = new AtomicBoolean(true);
        final AtomicReference<String> hint = new AtomicReference<>();
        final AtomicBoolean rejectMutation = new AtomicBoolean();
        final AtomicInteger discoveryCalls = new AtomicInteger();
        final AtomicInteger initShardsCalls = new AtomicInteger();
        final AtomicBoolean verifiedBeforeMutation = new AtomicBoolean();

        @Override
        public void getCoordinatorLeader(
                GetCoordinatorLeaderRequest request, StreamObserver<GetCoordinatorLeaderResponse> responseObserver) {
            discoveryCalls.incrementAndGet();
            GetCoordinatorLeaderResponse.Builder response =
                    GetCoordinatorLeaderResponse.newBuilder().setIsLeader(isLeader.get());
            // Protobuf strings cannot be null: an absent hint is represented by the empty default.
            if (hint.get() != null) {
                response.setLeaderAddress(hint.get());
            }
            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        }

        @Override
        public void initShards(InitShardsRequest request, StreamObserver<InitShardsResponse> responseObserver) {
            initShardsCalls.incrementAndGet();
            if (rejectMutation.get()) {
                isLeader.set(false);
                String description = hint.get() == null ? null : "Not the leader. Leader hint: " + hint.get();
                responseObserver.onError(
                        Status.FAILED_PRECONDITION.withDescription(description).asRuntimeException());
                return;
            }
            verifiedBeforeMutation.set(discoveryCalls.get() > 0 && isLeader.get());
            responseObserver.onNext(InitShardsResponse.getDefaultInstance());
            responseObserver.onCompleted();
        }
    }
}
