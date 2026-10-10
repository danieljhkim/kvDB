package com.danieljhkim.kvdb.kvcommon.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.danieljhkim.kvdb.proto.coordinator.ClusterState;
import com.danieljhkim.kvdb.proto.coordinator.NodeRecord;
import com.danieljhkim.kvdb.proto.coordinator.ShardRecord;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

/**
 * Schedule-controlled proof that a refresh landing between snapshot capture and shard resolution cannot mix shard
 * records with node records from another map version.
 */
class ShardMapCacheTest {

    private static final String SHARD = "shard-0";

    @Test
    void refreshBetweenCaptureAndLookupKeepsReplacedLeaderOnCapturedSnapshot() throws Exception {
        ClusterState captured = state(1, "old", List.of("old"), "old", "old:8001");
        ClusterState incoming = state(2, "new", List.of("new"), "new", "new:8001");

        NodeRecord leader = duringRefresh(captured, incoming, cache -> cache.getLeaderNode(SHARD));
        Optional<String> address = duringRefresh(captured, incoming, cache -> cache.getLeaderAddress(SHARD));

        assertEquals("old", leader.getNodeId());
        assertEquals("old:8001", leader.getAddress());
        assertEquals(Optional.of("old:8001"), address);
    }

    @Test
    void refreshBetweenCaptureAndLookupKeepsReplicasOnCapturedSnapshot() throws Exception {
        ClusterState captured =
                state(1, "old", List.of("old", "old-replica"), "old", "old:8001", "old-replica", "old:8002");
        ClusterState incoming = state(2, "new", List.of("new"), "new", "new:8001");

        List<NodeRecord> replicas = duringRefresh(captured, incoming, cache -> cache.getReplicaNodes(SHARD));
        Optional<String> anyReplica = duringRefresh(captured, incoming, cache -> cache.getAnyReplicaAddress(SHARD));

        assertEquals(
                List.of("old", "old-replica"),
                replicas.stream().map(NodeRecord::getNodeId).toList());
        assertEquals(
                List.of("old:8001", "old:8002"),
                replicas.stream().map(NodeRecord::getAddress).toList());
        assertEquals(Optional.of("old:8001"), anyReplica);
    }

    @Test
    void replacementNodeAddressStaysWithItsSnapshot() throws Exception {
        ClusterState captured = state(1, "n1", List.of("n1", "n2"), "n1", "old:8001", "n2", "old:8002");
        ClusterState incoming = state(2, "n1", List.of("n1", "n3"), "n1", "new:8001", "n3", "new:8003");

        Optional<String> interleavedLeader = duringRefresh(captured, incoming, cache -> cache.getLeaderAddress(SHARD));
        List<NodeRecord> interleavedReplicas = duringRefresh(captured, incoming, cache -> cache.getReplicaNodes(SHARD));
        Optional<String> interleavedAny = duringRefresh(captured, incoming, cache -> cache.getAnyReplicaAddress(SHARD));

        assertEquals(Optional.of("old:8001"), interleavedLeader);
        assertEquals(
                List.of("n1", "n2"),
                interleavedReplicas.stream().map(NodeRecord::getNodeId).toList());
        assertEquals(
                List.of("old:8001", "old:8002"),
                interleavedReplicas.stream().map(NodeRecord::getAddress).toList());
        assertEquals(Optional.of("old:8001"), interleavedAny);

        ShardMapCache cache = new ShardMapCache();
        assertTrue(cache.refreshFromFullState(captured));
        assertTrue(cache.refreshFromFullState(incoming));

        assertEquals(Optional.of("new:8001"), cache.getLeaderAddress(SHARD));
        assertEquals("n1", cache.getLeaderNode(SHARD).getNodeId());
        assertEquals("new:8001", cache.getLeaderNode(SHARD).getAddress());
        List<NodeRecord> replacedReplicas = cache.getReplicaNodes(SHARD);
        assertEquals(
                List.of("n1", "n3"),
                replacedReplicas.stream().map(NodeRecord::getNodeId).toList());
        assertEquals(
                List.of("new:8001", "new:8003"),
                replacedReplicas.stream().map(NodeRecord::getAddress).toList());
        assertEquals(Optional.of("new:8001"), cache.getAnyReplicaAddress(SHARD));
    }

    private static <T> T duringRefresh(ClusterState current, ClusterState incoming, Function<ShardMapCache, T> lookup)
            throws Exception {
        InterleavingCache cache = new InterleavingCache(incoming);
        assertTrue(cache.refreshFromFullState(current));
        Thread updater = new Thread(cache::refreshWhenCaptured, "shard-map-refresh");
        updater.start();
        try {
            return lookup.apply(cache);
        } finally {
            updater.join(5_000);
            if (updater.isAlive()) {
                updater.interrupt();
                updater.join(1_000);
            }
            assertFalse(updater.isAlive(), "refresh thread did not finish");
            assertEquals(incoming.getMapVersion(), cache.getMapVersion());
        }
    }

    /**
     * Releases the lookup only after a newer full state is published. The lookup still receives the snapshot captured
     * before that publish.
     */
    private static final class InterleavingCache extends ShardMapCache {
        private final CountDownLatch captured = new CountDownLatch(1);
        private final CountDownLatch refreshed = new CountDownLatch(1);
        private final ClusterState incoming;

        private InterleavingCache(ClusterState incoming) {
            this.incoming = incoming;
        }

        @Override
        protected ClusterState captureState() {
            ClusterState snapshot = super.captureState();
            captured.countDown();
            try {
                if (!refreshed.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("refresh was not scheduled");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("lookup interrupted before refresh", e);
            }
            return snapshot;
        }

        private void refreshWhenCaptured() {
            try {
                if (!captured.await(5, TimeUnit.SECONDS)) {
                    return;
                }
                refreshFromFullState(incoming);
                refreshed.countDown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static ClusterState state(long version, String leader, List<String> replicas, String... nodeIdAndAddress) {
        if (nodeIdAndAddress.length % 2 != 0) {
            throw new IllegalArgumentException("node id/address pairs must be even");
        }
        ClusterState.Builder builder = ClusterState.newBuilder().setMapVersion(version);
        ShardRecord.Builder shard = ShardRecord.newBuilder().setShardId(SHARD).setLeader(leader);
        replicas.forEach(shard::addReplicas);
        builder.putShards(SHARD, shard.build());
        for (int i = 0; i < nodeIdAndAddress.length; i += 2) {
            String nodeId = nodeIdAndAddress[i];
            builder.putNodes(
                    nodeId,
                    NodeRecord.newBuilder()
                            .setNodeId(nodeId)
                            .setAddress(nodeIdAndAddress[i + 1])
                            .build());
        }
        return builder.build();
    }
}
