package com.danieljhkim.kvdb.kvadmin.service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;

import com.danieljhkim.kvdb.kvadmin.api.dto.ClusterSummaryDto;
import com.danieljhkim.kvdb.kvadmin.api.dto.NodeDto;
import com.danieljhkim.kvdb.kvadmin.api.dto.ShardDto;
import com.danieljhkim.kvdb.kvadmin.api.dto.ShardMapSnapshotDto;
import com.danieljhkim.kvdb.kvadmin.client.CoordinatorReadClient;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

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

	public ClusterSummaryDto getClusterSummary() {
		ShardMapSnapshotDto shardMap = getShardMap();

		List<NodeDto> nodes = nodeAdminService.listNodes();
		List<ShardDto> shards = shardAdminService.listShards();

		long aliveNodes = nodes.stream().filter(n -> "ALIVE".equals(n.getStatus())).count();
		long suspectNodes = nodes.stream().filter(n -> "SUSPECT".equals(n.getStatus())).count();
		long deadNodes = nodes.stream().filter(n -> "DEAD".equals(n.getStatus())).count();

		long stableShards = shards.stream().filter(s -> "STABLE".equals(s.getConfigState())).count();
		long movingShards = shards.stream().filter(s -> "MOVING".equals(s.getConfigState())).count();

		return ClusterSummaryDto.builder()
				.mapVersion(shardMap.getMapVersion())
				.totalNodes(nodes.size())
				.aliveNodes((int) aliveNodes)
				.suspectNodes((int) suspectNodes)
				.deadNodes((int) deadNodes)
				.totalShards(shards.size())
				.stableShards((int) stableShards)
				.movingShards((int) movingShards)
				.replicationFactor(shardMap.getPartitioning() != null
						? shardMap.getPartitioning().getReplicationFactor()
						: 0)
				.partitioningConfig(shardMap.getPartitioning() != null
						? Map.of(
								"num_shards", String.valueOf(shardMap.getPartitioning().getNumShards()),
								"replication_factor",
								String.valueOf(shardMap.getPartitioning().getReplicationFactor()))
						: Map.of())
				.nodes(nodes)
				.shards(shards)
				.build();
	}

	public ShardMapSnapshotDto getShardMap() {
		return coordinatorReadClient.getShardMap();
	}
}

