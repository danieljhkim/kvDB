package com.danieljhkim.kvdb.kvclustercoordinator.health;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftCommand;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftConfiguration;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftNode;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.FileBasedRaftLog;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.persistence.RaftPersistentStateStore;
import com.danieljhkim.kvdb.kvclustercoordinator.raft.statemachine.RaftStateMachineImpl;
import com.kvdb.proto.kvstore.KVServiceGrpc;
import com.kvdb.proto.kvstore.PingResponse;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Leader-only health probes follow the live Raft role on the production state machine.
 */
class NodeHealthCheckerTest {

    private static final String STORAGE_ADDRESS = "localhost:8001";

    @TempDir
    Path tempDir;

    @Test
    void followerSuppressesProbesAfterSteppingDownFromLeader() throws Exception {
        RaftStateMachineImpl machine = new RaftStateMachineImpl();
        machine.apply(new RaftCommand.RegisterNode("storage-1", STORAGE_ADDRESS, "zone-a"))
                .join();

        AtomicInteger pings = new AtomicInteger();
        AtomicInteger submissions = new AtomicInteger();
        Path dataDir = tempDir.resolve("coord");
        try (FileBasedRaftLog log = new FileBasedRaftLog(dataDir.resolve("log"))) {
            RaftNode node = new RaftNode(
                    "coord-1",
                    configuration(dataDir),
                    log,
                    new RaftPersistentStateStore(dataDir.resolve("state").toString()),
                    machine,
                    (peer, request) -> CompletableFuture.failedFuture(new AssertionError("unexpected vote RPC")),
                    (peer, request) -> CompletableFuture.failedFuture(new AssertionError("unexpected append RPC")));
            machine.bindLeadership(node::isLeader);
            NodeHealthChecker checker = new NodeHealthChecker(machine, command -> {
                submissions.incrementAndGet();
                return node.submitCommand(command);
            });
            checker.installProbeStub(STORAGE_ADDRESS, successfulPingStub(pings));

            try {
                node.start();
                assertFalse(node.isLeader());
                assertFalse(machine.isLeader());
                checker.checkAllNodes();
                assertEquals(0, pings.get(), "a follower returns before creating or invoking a ping");

                node.getState().becomeCandidate();
                node.getState().becomeLeader(List.of());
                assertTrue(node.isLeader());
                assertTrue(machine.isLeader());
                checker.checkAllNodes();
                assertEquals(1, pings.get(), "the current leader issues one ping through the health-check seam");

                node.getState().transitionToFollower("other-coordinator");
                assertFalse(node.isLeader());
                assertFalse(machine.isLeader());
                checker.checkAllNodes();
                assertEquals(1, pings.get(), "a follower that stepped down does not invoke another ping");
                assertEquals(0, submissions.get(), "an already-alive node does not submit a status command");
            } finally {
                checker.shutdown();
                node.stop();
            }
        }
    }

    private static KVServiceGrpc.KVServiceBlockingStub successfulPingStub(AtomicInteger pings) {
        Channel channel = new Channel() {
            @Override
            public String authority() {
                return "fixture";
            }

            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(
                    MethodDescriptor<ReqT, RespT> method, io.grpc.CallOptions options) {
                return new ClientCall<ReqT, RespT>() {
                    private Listener<RespT> listener;

                    @Override
                    public void start(Listener<RespT> responseListener, Metadata headers) {
                        listener = responseListener;
                    }

                    @Override
                    public void request(int numMessages) {}

                    @Override
                    public void cancel(String message, Throwable cause) {}

                    @Override
                    public void sendMessage(ReqT message) {}

                    @Override
                    @SuppressWarnings("unchecked")
                    public void halfClose() {
                        pings.incrementAndGet();
                        listener.onMessage((RespT) PingResponse.getDefaultInstance());
                        listener.onClose(Status.OK, new Metadata());
                    }
                };
            }
        };
        return KVServiceGrpc.newBlockingStub(channel);
    }

    private static RaftConfiguration configuration(Path dataDir) {
        return RaftConfiguration.builder()
                .nodeId("coord-1")
                .clusterMembers(Map.of("coord-1", "localhost:9000"))
                .heartbeatInterval(Duration.ofHours(1))
                .electionTimeoutMin(Duration.ofHours(2))
                .electionTimeoutMax(Duration.ofHours(3))
                .dataDirectory(dataDir.toString())
                .build();
    }
}
