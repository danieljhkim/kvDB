package com.danieljhkim.kvdb.kvclustercoordinator.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftCommand;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftConfiguration;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftNode;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.FileBasedRaftLog;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftPersistentStateStore;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.statemachine.RaftStateMachine;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.statemachine.RaftStateMachineImpl;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.statemachine.StubRaftStateMachine;
import com.danieljhkim.kvdb.kvclustercoordinator.state.RejectedMutationException;
import com.danieljhkim.kvdb.kvclustercoordinator.state.ShardMapSnapshot;
import com.danieljhkim.kvdb.kvclustercoordinator.state.ShardRecord;
import com.danieljhkim.kvdb.kvcommon.cache.ShardMapCache;
import com.danieljhkim.kvdb.kvcommon.config.AppConfig;
import com.danieljhkim.kvdb.kvcommon.exception.PermissionDeniedException;
import com.danieljhkim.kvdb.kvcommon.grpc.CoordinatorClient;
import com.danieljhkim.kvdb.kvcommon.grpc.CoordinatorClientManager;
import com.danieljhkim.kvdb.kvcommon.grpc.GlobalExceptionInterceptor;
import com.danieljhkim.kvdb.kvcommon.grpc.GrpcIdentity;
import com.danieljhkim.kvdb.kvcommon.grpc.GrpcPeerIdentity;
import com.danieljhkim.kvdb.kvcommon.grpc.GrpcSecurityConfig;
import com.danieljhkim.kvdb.kvcommon.grpc.InternalAuthServerInterceptor;
import com.danieljhkim.kvdb.kvcommon.grpc.WatchShardMapClient;
import com.danieljhkim.kvdb.proto.coordinator.ClusterState;
import com.danieljhkim.kvdb.proto.coordinator.CoordinatorGrpc;
import com.danieljhkim.kvdb.proto.coordinator.GetShardMapResponse;
import com.danieljhkim.kvdb.proto.coordinator.InitShardsRequest;
import com.danieljhkim.kvdb.proto.coordinator.InitShardsResponse;
import com.danieljhkim.kvdb.proto.coordinator.NodeStatus;
import com.danieljhkim.kvdb.proto.coordinator.RegisterNodeRequest;
import com.danieljhkim.kvdb.proto.coordinator.ReportShardLeaderRequest;
import com.danieljhkim.kvdb.proto.coordinator.SetNodeStatusRequest;
import com.danieljhkim.kvdb.proto.coordinator.SetShardLeaderRequest;
import com.danieljhkim.kvdb.proto.coordinator.SetShardReplicasRequest;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
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

    @Test
    void storageNodeCannotCreateOrOverwriteAnotherNodesEndpoint() throws Exception {
        RaftStateMachineImpl stateMachine = new RaftStateMachineImpl();
        try (SingleNodeCoordinator coordinator = new SingleNodeCoordinator(tempDir, stateMachine, stateMachine)) {
            coordinator.stub().registerNode(registerNode("victim", "victim:8001"));
            ShardMapSnapshot before = stateMachine.getSnapshot();
            long logIndex = coordinator.log.lastIndex();
            var attacker = coordinator.stub(GrpcIdentity.Role.STORAGE_NODE, "attacker");

            for (String nodeId : List.of("victim", "new-victim", "Attacker")) {
                StatusRuntimeException denied = assertThrows(
                        StatusRuntimeException.class,
                        () -> attacker.registerNode(registerNode(nodeId, "attacker:8001")));
                assertEquals(Status.Code.PERMISSION_DENIED, denied.getStatus().getCode());
                assertTrue(denied.getStatus().getDescription().contains("matching nodeId"));
                assertEquals(logIndex, coordinator.log.lastIndex());
                assertSame(before, stateMachine.getSnapshot());
            }
            assertEquals(
                    "victim:8001", stateMachine.getSnapshot().getNode("victim").address());
            assertNull(stateMachine.getSnapshot().getNode("new-victim"));
        }
    }

    @Test
    void storageNodeCanRegisterAndUpdateItsExactPrincipal() throws Exception {
        RaftStateMachineImpl stateMachine = new RaftStateMachineImpl();
        try (SingleNodeCoordinator coordinator = new SingleNodeCoordinator(tempDir, stateMachine, stateMachine)) {
            var self = coordinator.stub(GrpcIdentity.Role.STORAGE_NODE, "node-1");
            long logIndex = coordinator.log.lastIndex();
            for (String address : List.of("node-1:8001", "node-1:8002")) {
                long version = stateMachine.getMapVersion();
                var response = self.registerNode(registerNode("node-1", address));
                assertTrue(response.getSuccess());
                assertEquals(++logIndex, coordinator.log.lastIndex());
                assertEquals(version + 1, response.getMapVersion());
                assertEquals(response.getMapVersion(), stateMachine.getMapVersion());
                assertEquals(
                        address, stateMachine.getSnapshot().getNode("node-1").address());
            }
        }
    }

    @Test
    void adminCanRegisterAndUpdateNodesWithDifferentPrincipals() throws Exception {
        RaftStateMachineImpl stateMachine = new RaftStateMachineImpl();
        try (SingleNodeCoordinator coordinator = new SingleNodeCoordinator(tempDir, stateMachine, stateMachine)) {
            var admin = coordinator.stub(GrpcIdentity.Role.ADMIN, "operator");
            coordinator
                    .stub(GrpcIdentity.Role.STORAGE_NODE, "node-1")
                    .registerNode(registerNode("node-1", "node-1:8001"));
            long logIndex = coordinator.log.lastIndex();
            for (String nodeId : List.of("node-1", "node-2")) {
                long version = stateMachine.getMapVersion();
                var response = admin.registerNode(registerNode(nodeId, nodeId + ":9001"));
                assertTrue(response.getSuccess());
                assertEquals(++logIndex, coordinator.log.lastIndex());
                assertEquals(version + 1, response.getMapVersion());
                assertEquals(response.getMapVersion(), stateMachine.getMapVersion());
                assertEquals(
                        nodeId + ":9001",
                        stateMachine.getSnapshot().getNode(nodeId).address());
            }
        }
    }

    @Test
    void registrationServiceFailsClosedWithoutAnAuthorizedIdentity() throws Exception {
        RaftStateMachineImpl stateMachine = new RaftStateMachineImpl();
        try (SingleNodeCoordinator coordinator = new SingleNodeCoordinator(tempDir, stateMachine, stateMachine)) {
            var service = new CoordinatorServiceImpl(coordinator.node, stateMachine, new WatcherManager());
            ShardMapSnapshot before = stateMachine.getSnapshot();
            long logIndex = coordinator.log.lastIndex();
            assertThrows(
                    PermissionDeniedException.class,
                    () -> service.registerNode(registerNode("node-1", "localhost:8001"), new RecordingObserver<>()));
            for (var peer : List.of(
                    new GrpcIdentity(GrpcIdentity.Role.GATEWAY, "", "node-1"),
                    new GrpcIdentity(GrpcIdentity.Role.STORAGE_NODE, "", ""))) {
                Context.current()
                        .withValue(GrpcPeerIdentity.CURRENT, peer)
                        .run(() -> assertThrows(
                                PermissionDeniedException.class,
                                () -> service.registerNode(
                                        registerNode("node-1", "localhost:8001"), new RecordingObserver<>())));
            }
            assertEquals(logIndex, coordinator.log.lastIndex());
            assertSame(before, stateMachine.getSnapshot());
        }
    }

    @Test
    void invalidShardMapMutationsAreRejectedBeforeReachingTheRaftLog() throws Exception {
        RaftStateMachineImpl stateMachine = new RaftStateMachineImpl();
        try (SingleNodeCoordinator coordinator = new SingleNodeCoordinator(tempDir, stateMachine, stateMachine)) {
            CoordinatorGrpc.CoordinatorBlockingStub stub = coordinator.stub();
            stub.registerNode(registerNode("node-1", "localhost:8001"));
            stub.registerNode(registerNode("node-2", "localhost:8002"));
            stub.initShards(InitShardsRequest.newBuilder()
                    .setNumShards(2)
                    .setReplicationFactor(2)
                    .build());
            // Registered after shard assignment, so it is a known node outside every replica set.
            stub.registerNode(registerNode("node-3", "localhost:8003"));
            ShardMapSnapshot before = stateMachine.getSnapshot();
            ShardRecord shard = before.getShard("shard-0");
            long logIndex = coordinator.node.getState().getLog().lastIndex();

            assertInvalidArgument(
                    () -> stub.setShardLeader(setShardLeader(shard.epoch(), "node-9")), "Node not found: node-9");
            assertInvalidArgument(
                    () -> stub.setShardLeader(setShardLeader(shard.epoch(), "node-3")),
                    "node-3 is not a replica of shard-0");
            assertInvalidArgument(
                    () -> stub.setShardLeader(setShardLeader(shard.epoch(), "{\"leader_node_id\":\"node-9\"}")),
                    "Node not found");
            assertInvalidArgument(
                    () -> coordinator
                            .stub(GrpcIdentity.Role.STORAGE_NODE, "node-1")
                            .reportShardLeader(ReportShardLeaderRequest.newBuilder()
                                    .setShardId("shard-0")
                                    .setEpoch(shard.epoch())
                                    .setLeaderNodeId("node-9")
                                    .build()),
                    "Node not found: node-9");
            assertInvalidArgument(() -> stub.setShardReplicas(setShardReplicas()), "cannot be empty");
            assertInvalidArgument(
                    () -> stub.setShardReplicas(setShardReplicas("node-1", "node-9")), "Node not found: node-9");
            assertInvalidArgument(
                    () -> stub.setShardReplicas(setShardReplicas("node-1", "node-1")), "duplicate node node-1");
            assertInvalidArgument(
                    () -> stub.registerNode(registerNode("node-x", "nocolon")), "Invalid node address 'nocolon'");

            assertSame(before, stateMachine.getSnapshot());
            assertEquals(logIndex, coordinator.node.getState().getLog().lastIndex());
            assertNull(stateMachine.getSnapshot().getNode("node-x"));

            String follower = shard.replicas().getLast();
            assertTrue(
                    stub.setShardLeader(setShardLeader(shard.epoch(), follower)).getSuccess());
            assertEquals(
                    follower, stateMachine.getSnapshot().getShard("shard-0").leader());
            assertEquals(before.getMapVersion() + 1, stateMachine.getMapVersion());
        }
    }

    @Test
    void committedCommandRejectedOnApplyReturnsInvalidArgumentAndKeepsTheLeaderServing() throws Exception {
        RaftStateMachineImpl stateMachine = new RaftStateMachineImpl();
        // The service validates against a view in which node-9 is a registered replica, standing in for a snapshot
        // that changes between validation and commit. Only the apply-time check can reject the command.
        RaftStateMachineImpl staleView = new RaftStateMachineImpl();
        staleView.applySync(new RaftCommand.RegisterNode("node-1", "localhost:8001", "zone-a"));
        staleView.applySync(new RaftCommand.RegisterNode("node-9", "localhost:8009", "zone-a"));
        staleView.applySync(new RaftCommand.InitShards(1, 2));
        try (SingleNodeCoordinator coordinator = new SingleNodeCoordinator(tempDir, stateMachine, staleView)) {
            RaftNode node = coordinator.node;
            node.submitCommand(new RaftCommand.RegisterNode("node-1", "localhost:8001", "zone-a"))
                    .get(5, TimeUnit.SECONDS);
            node.submitCommand(new RaftCommand.RegisterNode("node-2", "localhost:8002", "zone-a"))
                    .get(5, TimeUnit.SECONDS);
            node.submitCommand(new RaftCommand.InitShards(1, 2)).get(5, TimeUnit.SECONDS);
            ShardMapSnapshot before = stateMachine.getSnapshot();
            long logIndex = node.getState().getLog().lastIndex();

            assertInvalidArgument(
                    () -> coordinator.stub().setShardReplicas(setShardReplicas("node-1", "node-9")),
                    "Node not found: node-9");
            ExecutionException direct = assertThrows(ExecutionException.class, () -> node.submitCommand(
                            new RaftCommand.SetShardLeader("shard-0", 1, "node-9"))
                    .get(5, TimeUnit.SECONDS));
            assertInstanceOf(RejectedMutationException.class, direct.getCause());

            assertSame(before, stateMachine.getSnapshot());
            assertEquals(logIndex + 2, node.getState().getLog().lastIndex());
            assertEquals(logIndex + 2, node.getState().getLastApplied());

            node.submitCommand(new RaftCommand.SetShardReplicas("shard-0", List.of("node-2", "node-1")))
                    .get(5, TimeUnit.SECONDS);
            assertEquals(
                    List.of("node-2", "node-1"),
                    stateMachine.getSnapshot().getShard("shard-0").replicas());
            assertEquals(before.getMapVersion() + 1, stateMachine.getMapVersion());
        }
    }

    @Test
    void routingChangesReachWatchClientAndConditionalPoll() throws Exception {
        RaftStateMachineImpl stateMachine = new RaftStateMachineImpl();
        try (SingleNodeCoordinator coordinator = new SingleNodeCoordinator(tempDir, stateMachine, stateMachine)) {
            CoordinatorGrpc.CoordinatorBlockingStub stub = coordinator.stub();
            stub.registerNode(registerNode("node-1", "localhost:8001"));
            stub.initShards(InitShardsRequest.newBuilder()
                    .setNumShards(1)
                    .setReplicationFactor(1)
                    .build());
            long bootVersion = stateMachine.getMapVersion();
            assertEquals(2, bootVersion);

            try (RoutingConsumers consumers = RoutingConsumers.start(coordinator.port())) {
                consumers.awaitConverged("localhost:8001", NodeStatus.ALIVE, bootVersion);

                stub.registerNode(registerNode("node-1", "localhost:8001"));
                assertEquals(bootVersion, stateMachine.getMapVersion());
                assertTrue(consumers.poll(bootVersion).getNotModified());
                assertNull(consumers.fetch(bootVersion));
                consumers.awaitConverged("localhost:8001", NodeStatus.ALIVE, bootVersion);

                stub.registerNode(registerNode("node-1", "localhost:8002"));
                assertEquals(bootVersion + 1, stateMachine.getMapVersion());
                assertPublished(consumers, bootVersion, "localhost:8002", NodeStatus.ALIVE);

                stub.setNodeStatus(setNodeStatus("node-1", NodeStatus.SUSPECT));
                assertEquals(bootVersion + 2, stateMachine.getMapVersion());
                assertPublished(consumers, bootVersion + 1, "localhost:8002", NodeStatus.SUSPECT);

                stub.setNodeStatus(setNodeStatus("node-1", NodeStatus.SUSPECT));
                assertEquals(bootVersion + 2, stateMachine.getMapVersion());
                assertTrue(consumers.poll(bootVersion + 2).getNotModified());

                stub.setNodeStatus(setNodeStatus("node-1", NodeStatus.DEAD));
                assertEquals(bootVersion + 3, stateMachine.getMapVersion());
                assertPublished(consumers, bootVersion + 2, "localhost:8002", NodeStatus.DEAD);

                stub.setNodeStatus(setNodeStatus("node-1", NodeStatus.ALIVE));
                assertEquals(bootVersion + 4, stateMachine.getMapVersion());
                assertPublished(consumers, bootVersion + 3, "localhost:8002", NodeStatus.ALIVE);

                stub.setNodeStatus(setNodeStatus("node-1", NodeStatus.DEAD));
                long deadVersion = stateMachine.getMapVersion();
                assertEquals(bootVersion + 5, deadVersion);
                consumers.awaitConverged("localhost:8002", NodeStatus.DEAD, deadVersion);

                stub.registerNode(registerNode("node-1", "localhost:8002"));
                assertEquals(deadVersion + 1, stateMachine.getMapVersion());
                assertPublished(consumers, deadVersion, "localhost:8002", NodeStatus.ALIVE);

                long settled = stateMachine.getMapVersion();
                stub.registerNode(registerNode("node-1", "localhost:8002"));
                assertEquals(settled, stateMachine.getMapVersion());
                assertTrue(consumers.poll(settled).getNotModified());
                assertNull(consumers.fetch(settled));
                consumers.awaitConverged("localhost:8002", NodeStatus.ALIVE, settled);
            }
        }
    }

    private static void assertPublished(
            RoutingConsumers consumers, long previousVersion, String address, NodeStatus status) throws Exception {
        GetShardMapResponse response = consumers.poll(previousVersion);
        assertFalse(response.getNotModified());
        com.danieljhkim.kvdb.proto.coordinator.NodeRecord polled =
                response.getState().getNodesMap().get("node-1");
        assertEquals(address, polled.getAddress());
        assertEquals(status, polled.getStatus());
        assertEquals(previousVersion + 1, response.getState().getMapVersion());

        ClusterState fetched = consumers.fetch(previousVersion);
        assertNotNull(fetched);
        assertTrue(consumers.pollCache().refreshFromFullState(fetched));
        assertEquals(address, consumers.pollCache().getNodeAddress("node-1").orElseThrow());
        assertEquals(status, consumers.pollCache().getLeaderNode("shard-0").getStatus());
        assertEquals(previousVersion + 1, consumers.pollCache().getMapVersion());

        consumers.awaitConverged(address, status, previousVersion + 1);
    }

    private static SetNodeStatusRequest setNodeStatus(String nodeId, NodeStatus status) {
        return SetNodeStatusRequest.newBuilder()
                .setNodeId(nodeId)
                .setStatus(status)
                .build();
    }

    private static RegisterNodeRequest registerNode(String nodeId, String address) {
        return RegisterNodeRequest.newBuilder()
                .setNodeId(nodeId)
                .setAddress(address)
                .setZone("zone-a")
                .build();
    }

    private static SetShardLeaderRequest setShardLeader(long epoch, String leaderNodeId) {
        return SetShardLeaderRequest.newBuilder()
                .setShardId("shard-0")
                .setEpoch(epoch)
                .setLeaderNodeId(leaderNodeId)
                .build();
    }

    private static SetShardReplicasRequest setShardReplicas(String... replicas) {
        return SetShardReplicasRequest.newBuilder()
                .setShardId("shard-0")
                .addAllReplicas(List.of(replicas))
                .build();
    }

    private static void assertInvalidArgument(Executable rpc, String expectedDescription) {
        StatusRuntimeException e = assertThrows(StatusRuntimeException.class, rpc);
        assertEquals(
                Status.Code.INVALID_ARGUMENT,
                e.getStatus().getCode(),
                e.getStatus().toString());
        assertTrue(
                e.getStatus().getDescription().contains(expectedDescription),
                e.getStatus().getDescription());
    }

    /** A single-member coordinator that has elected itself leader, served over gRPC with the production interceptor. */
    private static final class SingleNodeCoordinator implements AutoCloseable {

        private final FileBasedRaftLog log;
        private final RaftNode node;
        private final Server server;
        private final ManagedChannel channel;

        SingleNodeCoordinator(Path tempDir, RaftStateMachine stateMachine, RaftStateMachine serviceView)
                throws Exception {
            RaftConfiguration config =
                    configuration("leader", Map.of("leader", "localhost:0"), tempDir.resolve("single"));
            log = new FileBasedRaftLog(tempDir.resolve("single.log"));
            node = new RaftNode(
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
            WatcherManager watcherManager = new WatcherManager();
            stateMachine.addWatcher(watcherManager);
            server = NettyServerBuilder.forPort(0)
                    .addService(ServerInterceptors.intercept(
                            new CoordinatorServiceImpl(node, serviceView, watcherManager),
                            new InternalAuthServerInterceptor(
                                    GrpcSecurityConfig.development(GrpcIdentity.Role.COORDINATOR, "leader")),
                            new GlobalExceptionInterceptor()))
                    .build()
                    .start();
            channel = NettyChannelBuilder.forAddress("localhost", server.getPort())
                    .usePlaintext()
                    .build();
        }

        int port() {
            return server.getPort();
        }

        CoordinatorGrpc.CoordinatorBlockingStub stub() {
            return stub(GrpcIdentity.Role.ADMIN, "operator");
        }

        CoordinatorGrpc.CoordinatorBlockingStub stub(GrpcIdentity.Role role, String principal) {
            Metadata headers = new Metadata();
            headers.put(
                    Metadata.Key.of("x-kvdb-development-identity", Metadata.ASCII_STRING_MARSHALLER),
                    role.sanValue() + "/" + principal);
            return CoordinatorGrpc.newBlockingStub(channel)
                    .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers))
                    .withDeadlineAfter(5, TimeUnit.SECONDS);
        }

        @Override
        public void close() throws Exception {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
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

    /**
     * Production watch and conditional-poll consumers pointed at the in-process coordinator. The watch cache is filled
     * only by {@link WatchShardMapClient}. The poll cache is filled only by {@link CoordinatorClient#fetchShardMap}.
     */
    private static final class RoutingConsumers implements AutoCloseable {

        private final CoordinatorClientManager clientManager;
        private final CoordinatorClient client;
        private final ShardMapCache watchCache = new ShardMapCache();
        private final ShardMapCache pollCache = new ShardMapCache();
        private final WatchShardMapClient watchClient;

        private RoutingConsumers(int port) {
            AppConfig.NodeConfig node = new AppConfig.NodeConfig();
            node.setId("coordinator-1");
            node.setHost("127.0.0.1");
            node.setPort(port);
            AppConfig.NodeGroupConfig group = new AppConfig.NodeGroupConfig();
            group.setNodes(List.of(node));
            AppConfig config = new AppConfig();
            config.setCoordinatorNodes(group);
            this.clientManager = new CoordinatorClientManager(config);
            this.client = clientManager.getClient("coordinator-1");
            this.watchClient = new WatchShardMapClient(watchCache, clientManager);
            this.watchClient.start(0);
        }

        static RoutingConsumers start(int port) {
            return new RoutingConsumers(port);
        }

        ShardMapCache pollCache() {
            return pollCache;
        }

        GetShardMapResponse poll(long ifVersionGt) {
            return client.getShardMap(ifVersionGt);
        }

        ClusterState fetch(long ifVersionGt) {
            return client.fetchShardMap(ifVersionGt);
        }

        void awaitConverged(String address, NodeStatus status, long version) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (System.nanoTime() < deadline) {
                var node = watchCache.getLeaderNode("shard-0");
                if (watchCache.getMapVersion() == version
                        && node != null
                        && address.equals(node.getAddress())
                        && node.getStatus() == status) {
                    return;
                }
                Thread.sleep(20);
            }
            var node = watchCache.getLeaderNode("shard-0");
            fail("watch cache did not converge to " + address + " " + status + " version " + version + "; cacheVersion="
                    + watchCache.getMapVersion()
                    + " node="
                    + (node == null ? "absent" : node.getAddress() + " " + node.getStatus()));
        }

        @Override
        public void close() {
            watchClient.shutdown();
            clientManager.shutdown();
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
