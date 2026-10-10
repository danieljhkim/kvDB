package com.danieljhkim.kvdb.kvclustercoordinator.state;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ClusterStateValidationTest {

    private ClusterState state;
    private ShardRecord shard0;

    @BeforeEach
    void bootstrap() {
        state = new ClusterState();
        state.registerNode("node-1", "localhost:8001", "zone-a");
        state.registerNode("node-2", "localhost:8002", "zone-a");
        state.initializeShards(2, 2);
        // Registered after shard assignment, so it is a known node outside every replica set.
        state.registerNode("node-3", "localhost:8003", "zone-a");
        shard0 = state.getShard("shard-0");
        assertEquals(2, shard0.replicas().size());
    }

    @Test
    void setShardLeaderRejectsUnknownNodeWithoutChangingState() {
        assertRejected(() -> state.setShardLeader("shard-0", shard0.epoch(), "node-9"), "Node not found: node-9");
    }

    @Test
    void setShardLeaderRejectsRegisteredNodeOutsideReplicaSet() {
        assertRejected(
                () -> state.setShardLeader("shard-0", shard0.epoch(), "node-3"), "node-3 is not a replica of shard-0");
    }

    @Test
    void setShardLeaderRejectsMalformedLeaderString() {
        assertRejected(
                () -> state.setShardLeader("shard-0", shard0.epoch(), "{\"leader_node_id\":\"node-2\"}"),
                "Node not found");
    }

    @Test
    void setShardLeaderRejectsStaleEpochAndUnknownShard() {
        String replica = shard0.replicas().getLast();
        assertRejected(() -> state.setShardLeader("shard-0", shard0.epoch() + 1, replica), "Epoch mismatch");
        assertRejected(() -> state.setShardLeader("shard-9", 1, replica), "Shard not found: shard-9");
    }

    @Test
    void setShardLeaderAcceptsReplica() {
        String replica = shard0.replicas().getLast();
        long version = state.getMapVersion();

        state.setShardLeader("shard-0", shard0.epoch(), replica);

        assertEquals(replica, state.getShard("shard-0").leader());
        assertEquals(version + 1, state.getMapVersion());
    }

    @Test
    void setShardReplicasRejectsEmptyUnknownDuplicateAndBlankIds() {
        assertRejected(() -> state.setShardReplicas("shard-0", List.of()), "cannot be empty");
        assertRejected(() -> state.setShardReplicas("shard-0", List.of("node-1", "node-9")), "Node not found: node-9");
        assertRejected(() -> state.setShardReplicas("shard-0", List.of("node-1", "node-1")), "duplicate node node-1");
        assertRejected(() -> state.setShardReplicas("shard-0", List.of("node-1", " ")), "blank node ID");
        assertRejected(() -> state.setShardReplicas("shard-0", Arrays.asList("node-1", null)), "blank node ID");
        assertRejected(() -> state.setShardReplicas("shard-9", List.of("node-1")), "Shard not found: shard-9");
    }

    @Test
    void setShardReplicasAcceptsRegisteredDistinctNodes() {
        long version = state.getMapVersion();

        state.setShardReplicas("shard-0", List.of("node-3", "node-1"));

        ShardRecord updated = state.getShard("shard-0");
        assertEquals(List.of("node-3", "node-1"), updated.replicas());
        assertEquals("node-3", updated.leader());
        assertEquals(shard0.epoch() + 1, updated.epoch());
        assertEquals(version + 1, state.getMapVersion());
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "nocolon",
                ":8001",
                "host:",
                "host:0",
                "host:65536",
                "host:123456",
                "host:-1",
                "host:80a",
                "host name:8001",
                "[::1]:8001",
                "host:8001:1",
                " "
            })
    void registerNodeRejectsAddressThatIsNotHostPort(String address) {
        long version = state.getMapVersion();

        assertThrows(RejectedMutationException.class, () -> state.registerNode("node-x", address, "zone-a"));

        assertNull(state.getNode("node-x"));
        assertEquals(version, state.getMapVersion());
    }

    @Test
    void registerNodeRejectsMalformedAddressForExistingNodeWithoutChangingIt() {
        assertThrows(RejectedMutationException.class, () -> state.registerNode("node-1", "nocolon", "zone-b"));

        assertEquals("localhost:8001", state.getNode("node-1").address());
    }

    @ParameterizedTest
    @ValueSource(strings = {"localhost:8001", "127.0.0.1:1", "kv-node_1.svc.local:65535"})
    void registerNodeAcceptsHostPort(String address) {
        assertDoesNotThrow(() -> state.registerNode("node-x", address, "zone-a"));
        assertEquals(NodeRecord.NodeStatus.ALIVE, state.getNode("node-x").status());
    }

    @Test
    void setNodeStatusAndConflictingInitShardsAreRejections() {
        assertRejected(() -> state.setNodeStatus("node-9", NodeRecord.NodeStatus.DEAD), "Node not found: node-9");
        assertRejected(() -> state.initializeShards(4, 2), "already initialized with different configuration");
    }

    private void assertRejected(Runnable mutation, String expectedMessage) {
        ShardMapSnapshot before = state.createSnapshot();

        RejectedMutationException rejection = assertThrows(RejectedMutationException.class, mutation::run);

        assertTrue(rejection.getMessage().contains(expectedMessage), rejection.getMessage());
        assertEquals(before.getMapVersion(), state.getMapVersion());
        assertEquals(before.getShards(), state.getShards());
        assertEquals(before.getNodes(), state.getNodes());
    }
}
