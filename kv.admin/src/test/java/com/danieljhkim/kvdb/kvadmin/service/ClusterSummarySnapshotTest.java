package com.danieljhkim.kvdb.kvadmin.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.danieljhkim.kvdb.kvadmin.api.dto.ClusterSummaryDto;
import com.danieljhkim.kvdb.kvadmin.api.dto.NodeDto;
import com.danieljhkim.kvdb.kvadmin.api.dto.PartitioningConfigDto;
import com.danieljhkim.kvdb.kvadmin.api.dto.ShardDto;
import com.danieljhkim.kvdb.kvadmin.api.dto.ShardMapSnapshotDto;
import com.danieljhkim.kvdb.kvadmin.client.CoordinatorReadClient;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class ClusterSummarySnapshotTest {

    @Test
    void cacheMissUsesOneSnapshotEvenWhenCoordinatorAdvancesAfterRead() {
        ShardMapSnapshotDto captured = populatedSnapshot();
        AdvancingCoordinator coordinator = new AdvancingCoordinator(captured, newerSnapshot());
        CountingNodeService nodes = new CountingNodeService(coordinator);
        CountingShardService shards = new CountingShardService(coordinator);
        ClusterAdminService service = new ClusterAdminService(coordinator, shards, nodes, new StubCache(null));

        assertCapturedSummary(captured, service.getClusterSummary());

        assertEquals(1, coordinator.shardMapReads);
        assertEquals(0, coordinator.nodeReads);
        assertEquals(0, nodes.listCalls);
        assertEquals(0, shards.listCalls);
        // The next read sees the published map, with different membership and version.
        assertEquals(42, coordinator.getShardMap().getMapVersion());
    }

    @Test
    void cacheHitKeepsCachedMembershipBesideCachedVersion() {
        ShardMapSnapshotDto captured = populatedSnapshot();
        AdvancingCoordinator coordinator = new AdvancingCoordinator(newerSnapshot(), newerSnapshot());
        CountingNodeService nodes = new CountingNodeService(coordinator);
        CountingShardService shards = new CountingShardService(coordinator);
        ClusterAdminService service = new ClusterAdminService(coordinator, shards, nodes, new StubCache(captured));

        assertCapturedSummary(captured, service.getClusterSummary());

        assertEquals(0, coordinator.shardMapReads);
        assertEquals(0, coordinator.nodeReads);
        assertEquals(0, nodes.listCalls);
        assertEquals(0, shards.listCalls);
        assertEquals(42, coordinator.getShardMap().getMapVersion());
    }

    @Test
    void emptySnapshotHasEmptyListsAndZeroCounts() {
        ShardMapSnapshotDto empty = ShardMapSnapshotDto.builder()
                .mapVersion(0)
                .nodes(Map.of())
                .shards(Map.of())
                .build();
        AdvancingCoordinator coordinator = new AdvancingCoordinator(empty, newerSnapshot());
        ClusterAdminService service = new ClusterAdminService(
                coordinator,
                new CountingShardService(coordinator),
                new CountingNodeService(coordinator),
                new StubCache(null));

        ClusterSummaryDto summary = service.getClusterSummary();

        assertEquals(0, summary.getMapVersion());
        assertEquals(List.of(), summary.getNodes());
        assertEquals(List.of(), summary.getShards());
        assertEquals(0, summary.getTotalNodes());
        assertEquals(0, summary.getAliveNodes());
        assertEquals(0, summary.getSuspectNodes());
        assertEquals(0, summary.getDeadNodes());
        assertEquals(0, summary.getTotalShards());
        assertEquals(0, summary.getStableShards());
        assertEquals(0, summary.getMovingShards());
        assertEquals(0, summary.getReplicationFactor());
        assertEquals(Map.of(), summary.getPartitioningConfig());
        assertEquals(1, coordinator.shardMapReads);
        assertEquals(0, coordinator.nodeReads);
    }

    private static void assertCapturedSummary(ShardMapSnapshotDto captured, ClusterSummaryDto summary) {
        assertEquals(41, summary.getMapVersion());
        assertEquals(captured.getNodes().values().stream().toList(), summary.getNodes());
        assertEquals(captured.getShards().values().stream().toList(), summary.getShards());
        assertEquals(3, summary.getTotalNodes());
        assertEquals(1, summary.getAliveNodes());
        assertEquals(1, summary.getSuspectNodes());
        assertEquals(1, summary.getDeadNodes());
        assertEquals(2, summary.getTotalShards());
        assertEquals(1, summary.getStableShards());
        assertEquals(1, summary.getMovingShards());
        assertEquals(3, summary.getReplicationFactor());
        assertEquals(Map.of("num_shards", "2", "replication_factor", "3"), summary.getPartitioningConfig());
    }

    private static ShardMapSnapshotDto populatedSnapshot() {
        return ShardMapSnapshotDto.builder()
                .mapVersion(41)
                .nodes(Map.of(
                        "alive",
                                NodeDto.builder()
                                        .nodeId("alive")
                                        .status("ALIVE")
                                        .build(),
                        "suspect",
                                NodeDto.builder()
                                        .nodeId("suspect")
                                        .status("SUSPECT")
                                        .build(),
                        "dead", NodeDto.builder().nodeId("dead").status("DEAD").build()))
                .shards(Map.of(
                        "stable",
                                ShardDto.builder()
                                        .shardId("stable")
                                        .configState("STABLE")
                                        .build(),
                        "moving",
                                ShardDto.builder()
                                        .shardId("moving")
                                        .configState("MOVING")
                                        .build()))
                .partitioning(PartitioningConfigDto.builder()
                        .numShards(2)
                        .replicationFactor(3)
                        .build())
                .build();
    }

    private static ShardMapSnapshotDto newerSnapshot() {
        return ShardMapSnapshotDto.builder()
                .mapVersion(42)
                .nodes(Map.of(
                        "new-node",
                        NodeDto.builder().nodeId("new-node").status("ALIVE").build()))
                .shards(Map.of(
                        "new-shard",
                        ShardDto.builder()
                                .shardId("new-shard")
                                .configState("MOVING")
                                .build()))
                .partitioning(PartitioningConfigDto.builder()
                        .numShards(1)
                        .replicationFactor(1)
                        .build())
                .build();
    }

    private static final class AdvancingCoordinator extends CoordinatorReadClient {
        private final ShardMapSnapshotDto first;
        private final ShardMapSnapshotDto newer;
        private int shardMapReads;
        private int nodeReads;

        AdvancingCoordinator(ShardMapSnapshotDto first, ShardMapSnapshotDto newer) {
            super(List.of("localhost:1"), 1, TimeUnit.MILLISECONDS);
            this.first = first;
            this.newer = newer;
        }

        @Override
        public ShardMapSnapshotDto getShardMap() {
            return shardMapReads++ == 0 ? first : newer;
        }

        @Override
        public List<NodeDto> listNodes() {
            nodeReads++;
            return newer.getNodes().values().stream().toList();
        }
    }

    private static final class CountingNodeService extends NodeAdminService {
        private int listCalls;

        CountingNodeService(CoordinatorReadClient coordinator) {
            super(null, coordinator, null);
        }

        @Override
        public List<NodeDto> listNodes() {
            listCalls++;
            return super.listNodes();
        }
    }

    private static final class CountingShardService extends ShardAdminService {
        private int listCalls;

        CountingShardService(CoordinatorReadClient coordinator) {
            super(null, coordinator, null);
        }

        @Override
        public List<ShardDto> listShards() {
            listCalls++;
            return super.listShards();
        }
    }

    private static final class StubCache extends ShardMapCache {
        private ShardMapSnapshotDto snapshot;

        StubCache(ShardMapSnapshotDto snapshot) {
            this.snapshot = snapshot;
        }

        @Override
        public ShardMapSnapshotDto get() {
            return snapshot;
        }

        @Override
        public void put(ShardMapSnapshotDto snapshot) {
            this.snapshot = snapshot;
        }
    }
}
