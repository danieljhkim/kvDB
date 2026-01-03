package com.danieljhkim.kvdb.kvadmin.service;

import com.danieljhkim.kvdb.kvadmin.api.dto.ClusterSummaryDto;
import com.danieljhkim.kvdb.kvadmin.api.dto.NodeDto;
import com.danieljhkim.kvdb.kvadmin.api.dto.ShardDto;
import com.danieljhkim.kvdb.kvadmin.api.dto.ShardMapSnapshotDto;
import com.danieljhkim.kvdb.kvadmin.client.CoordinatorReadClient;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * Service for cluster-level administration operations.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class ClusterAdminService {

    private final CoordinatorReadClient coordinatorReadClient;
    private final ShardAdminService shardAdminService;
    private final NodeAdminService nodeAdminService;
    private final ShardMapCache shardMapCache;

    public ClusterSummaryDto getClusterSummary() {
        ShardMapSnapshotDto shardMap = getShardMap();
        if (shardMap == null) {
            throw new IllegalStateException("Shard map not available: cannot generate cluster summary");
        }

        List<NodeDto> nodes = nodeAdminService.listNodes();
        List<ShardDto> shards = shardAdminService.listShards();

        long aliveNodes =
                nodes.stream().filter(n -> "ALIVE".equals(n.getStatus())).count();
        long suspectNodes =
                nodes.stream().filter(n -> "SUSPECT".equals(n.getStatus())).count();
        long deadNodes =
                nodes.stream().filter(n -> "DEAD".equals(n.getStatus())).count();

        long stableShards =
                shards.stream().filter(s -> "STABLE".equals(s.getConfigState())).count();
        long movingShards =
                shards.stream().filter(s -> "MOVING".equals(s.getConfigState())).count();

        return ClusterSummaryDto.builder()
                .mapVersion(shardMap.getMapVersion())
                .totalNodes(nodes.size())
                .aliveNodes((int) aliveNodes)
                .suspectNodes((int) suspectNodes)
                .deadNodes((int) deadNodes)
                .totalShards(shards.size())
                .stableShards((int) stableShards)
                .movingShards((int) movingShards)
                .replicationFactor(
                        shardMap.getPartitioning() != null
                                ? shardMap.getPartitioning().getReplicationFactor()
                                : 0)
                .partitioningConfig(
                        shardMap.getPartitioning() != null
                                ? Map.of(
                                        "num_shards",
                                        String.valueOf(
                                                shardMap.getPartitioning().getNumShards()),
                                        "replication_factor",
                                        String.valueOf(
                                                shardMap.getPartitioning().getReplicationFactor()))
                                : Map.of())
                .nodes(nodes)
                .shards(shards)
                .build();
    }

    public ShardMapSnapshotDto getShardMap() {
        // Check cache first
        ShardMapSnapshotDto cached = shardMapCache.get();
        if (cached != null) {
            return cached;
        }

        // Cache miss or expired - fetch from coordinator
        ShardMapSnapshotDto shardMap = coordinatorReadClient.getShardMap();
        if (shardMap != null) {
            shardMapCache.put(shardMap);
        }
        return shardMap;
    }

    /**
     * Gets the shard map version, using cache if available.
     *
     * @return The map version number
     */
    public long getShardMapVersion() {
        // Check cache first
        Long cachedVersion = shardMapCache.getVersion();
        if (cachedVersion != null) {
            return cachedVersion;
        }

        // Cache miss or expired - fetch from coordinator
        ShardMapSnapshotDto shardMap = coordinatorReadClient.getShardMap();
        if (shardMap != null) {
            shardMapCache.put(shardMap);
            return shardMap.getMapVersion();
        }

        // Fallback: return 0 if coordinator unavailable
        return 0;
    }
}
