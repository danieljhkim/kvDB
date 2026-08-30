package com.danieljhkim.kvdb.kvclustercoordinator.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftCommand;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftConfiguration;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftNode;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.FileBasedRaftLog;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftPersistentStateStore;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.statemachine.StubRaftStateMachine;
import com.danieljhkim.kvdb.kvcommon.grpc.GlobalExceptionInterceptor;
import com.danieljhkim.kvdb.proto.coordinator.CoordinatorGrpc;
import com.danieljhkim.kvdb.proto.coordinator.InitShardsRequest;
import com.danieljhkim.kvdb.proto.coordinator.InitShardsResponse;
import com.danieljhkim.kvdb.proto.coordinator.NodeStatus;
import com.danieljhkim.kvdb.proto.coordinator.RegisterNodeRequest;
import com.danieljhkim.kvdb.proto.coordinator.ReportShardLeaderRequest;
import com.danieljhkim.kvdb.proto.coordinator.SetNodeStatusRequest;
import com.danieljhkim.kvdb.proto.coordinator.SetShardLeaderRequest;
import com.danieljhkim.kvdb.proto.coordinator.SetShardReplicasRequest;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;

class CoordinatorServiceImplTest {

    private static final String LEADER_ADDRESS = "leader.example:9000";

    @TempDir
    Path tempDir;

    @Test
    void productionMutationRpcsOnlySubmitThroughRaft() throws Exception {
        String source = Files.readString(
                Path.of("src/main/java/com/danieljhkim/kvdb/kvclustercoordinator/service/CoordinatorServiceImpl.java"));

        assertFalse(
                Pattern.compile("raftStateMachine\\s*\\.apply\\s*\\(")
                        .matcher(source)
                        .find(),
                "Production RPC code must not apply coordinator state directly");
        assertEquals(
                6,
                Pattern.compile("raftNode\\s*\\.submitCommand\\s*\\(")
                        .matcher(source)
                        .results()
                        .count(),
                "Every mutation RPC must submit exactly one Raft command");
    }

    @Test
    void followerMutationRpcsReturnFailedPreconditionWithLeaderHint() throws Exception {
        Map<String, String> members = Map.of("follower", "localhost:0", "leader", LEADER_ADDRESS);
        RaftConfiguration config = configuration("follower", members, tempDir.resolve("follower"));
        FileBasedRaftLog log = new FileBasedRaftLog(tempDir.resolve("follower.log"));
        RaftNode node = new RaftNode(
                "follower",
                config,
                log,
                new RaftPersistentStateStore(tempDir.resolve("follower-state").toString()),
                new StubRaftStateMachine(),
                (peer, request) -> CompletableFuture.failedFuture(new AssertionError("unexpected vote RPC")),
                (peer, request) -> CompletableFuture.failedFuture(new AssertionError("unexpected append RPC")));
        node.getState().transitionToFollower("leader");

        CoordinatorServiceImpl service =
                new CoordinatorServiceImpl(node, new StubRaftStateMachine(), new WatcherManager());
        Server server = NettyServerBuilder.forPort(0)
                .addService(ServerInterceptors.intercept(service, new GlobalExceptionInterceptor()))
                .build()
                .start();
        ManagedChannel channel = NettyChannelBuilder.forAddress("localhost", server.getPort())
                .usePlaintext()
                .build();

        try {
            CoordinatorGrpc.CoordinatorBlockingStub stub = CoordinatorGrpc.newBlockingStub(channel);

            assertFollowerRejected(() -> stub.reportShardLeader(ReportShardLeaderRequest.newBuilder()
                    .setShardId("shard-0")
                    .setEpoch(1)
                    .setLeaderNodeId("storage-1")
                    .build()));
            assertFollowerRejected(() -> stub.registerNode(RegisterNodeRequest.newBuilder()
                    .setNodeId("storage-1")
                    .setAddress("storage-1:7000")
                    .build()));
            assertFollowerRejected(() -> stub.initShards(InitShardsRequest.newBuilder()
                    .setNumShards(1)
                    .setReplicationFactor(1)
                    .build()));
            assertFollowerRejected(() -> stub.setNodeStatus(SetNodeStatusRequest.newBuilder()
                    .setNodeId("storage-1")
                    .setStatus(NodeStatus.ALIVE)
                    .build()));
            assertFollowerRejected(() -> stub.setShardReplicas(SetShardReplicasRequest.newBuilder()
                    .setShardId("shard-0")
                    .addReplicas("storage-1")
                    .build()));
            assertFollowerRejected(() -> stub.setShardLeader(SetShardLeaderRequest.newBuilder()
                    .setShardId("shard-0")
                    .setEpoch(1)
                    .setLeaderNodeId("storage-1")
                    .build()));
        } finally {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            log.close();
        }
    }

    @Test
    void mutationResponseWaitsUntilAppliedVersionIsVisible() throws Exception {
        GatedStateMachine stateMachine = new GatedStateMachine();
        Map<String, String> members = Map.of("leader", "localhost:0");
        RaftConfiguration config = configuration("leader", members, tempDir.resolve("single"));
        FileBasedRaftLog log = new FileBasedRaftLog(tempDir.resolve("single.log"));
        RaftNode node = new RaftNode(
                "leader",
                config,
                log,
                new RaftPersistentStateStore(tempDir.resolve("single-state").toString()),
                stateMachine,
                (peer, request) -> CompletableFuture.failedFuture(new AssertionError("unexpected vote RPC")),
                (peer, request) -> CompletableFuture.failedFuture(new AssertionError("unexpected append RPC")));
        node.start();
        node.getState().becomeCandidate();
        node.getState().becomeLeader(config.getPeers().keySet());

        RecordingObserver<InitShardsResponse> observer = new RecordingObserver<>();
        try {
            new CoordinatorServiceImpl(node, stateMachine, new WatcherManager())
                    .initShards(
                            InitShardsRequest.newBuilder()
                                    .setNumShards(1)
                                    .setReplicationFactor(1)
                                    .build(),
                            observer);

            assertTrue(stateMachine.awaitApplyStarted(), "Raft applier never invoked the state machine");
            assertTrue(observer.values.isEmpty(), "RPC returned before the state-machine apply completed");
            assertFalse(observer.completed, "RPC completed before the state-machine apply completed");
            assertEquals(0, stateMachine.getMapVersion());

            stateMachine.releaseApply();
            assertTrue(observer.awaitCompleted(), "RPC did not complete after the state-machine apply");
            assertNull(observer.error);
            assertEquals(1, observer.values.size());
            assertTrue(observer.values.getFirst().getSuccess());
            assertEquals(
                    stateMachine.getMapVersion(), observer.values.getFirst().getMapVersion());
            assertTrue(observer.values.getFirst().getMapVersion() > 0);
        } finally {
            stateMachine.releaseApply();
            node.stop();
            log.close();
        }
    }

    private static RaftConfiguration configuration(String nodeId, Map<String, String> members, Path dataDirectory) {
        return RaftConfiguration.builder()
                .nodeId(nodeId)
                .clusterMembers(members)
                .heartbeatInterval(Duration.ofHours(1))
                .electionTimeoutMin(Duration.ofHours(2))
                .electionTimeoutMax(Duration.ofHours(3))
                .dataDirectory(dataDirectory.toString())
                .build();
    }

    private static void assertFollowerRejected(Executable rpc) {
        try {
            rpc.execute();
            fail("Follower mutation RPC unexpectedly succeeded");
        } catch (StatusRuntimeException e) {
            assertEquals(Status.Code.FAILED_PRECONDITION, e.getStatus().getCode());
            assertEquals(LEADER_ADDRESS, e.getTrailers().get(GlobalExceptionInterceptor.LEADER_HINT_KEY));
        } catch (Throwable t) {
            fail("Unexpected exception", t);
        }
    }

    private static final class GatedStateMachine extends StubRaftStateMachine {

        private final CountDownLatch applyStarted = new CountDownLatch(1);
        private final CompletableFuture<Void> applyGate = new CompletableFuture<>();

        @Override
        public CompletableFuture<Void> apply(RaftCommand command) {
            applyStarted.countDown();
            return applyGate.thenCompose(ignored -> super.apply(command));
        }

        boolean awaitApplyStarted() throws InterruptedException {
            return applyStarted.await(5, TimeUnit.SECONDS);
        }

        void releaseApply() {
            applyGate.complete(null);
        }
    }

    private static final class RecordingObserver<T> implements StreamObserver<T> {

        private final List<T> values = new CopyOnWriteArrayList<>();
        private final CountDownLatch completedLatch = new CountDownLatch(1);
        private volatile boolean completed;
        private volatile Throwable error;

        @Override
        public void onNext(T value) {
            values.add(value);
        }

        @Override
        public void onError(Throwable throwable) {
            error = throwable;
            completedLatch.countDown();
        }

        @Override
        public void onCompleted() {
            completed = true;
            completedLatch.countDown();
        }

        boolean awaitCompleted() throws InterruptedException {
            return completedLatch.await(5, TimeUnit.SECONDS);
        }
    }
}
