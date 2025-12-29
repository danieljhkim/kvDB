package com.danieljhkim.kvdb.kvadmin.service;

import java.util.List;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;

import com.danieljhkim.kvdb.kvadmin.api.dto.ShardDto;
import com.danieljhkim.kvdb.kvadmin.api.dto.TriggerRequestDto;
import com.danieljhkim.kvdb.kvadmin.client.CoordinatorAdminClient;
import com.danieljhkim.kvdb.kvadmin.client.CoordinatorReadClient;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Service for shard administration operations.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class ShardAdminService {

	private final CoordinatorAdminClient coordinatorAdminClient;
	private final CoordinatorReadClient coordinatorReadClient;

	public List<ShardDto> listShards() {
		com.danieljhkim.kvdb.kvadmin.api.dto.ShardMapSnapshotDto shardMap = coordinatorReadClient
				.getShardMap();
		return shardMap.getShards().values().stream()
				.collect(Collectors.toList());
	}

	public ShardDto getShard(String shardId) {
		com.danieljhkim.kvdb.kvadmin.api.dto.ShardMapSnapshotDto shardMap = coordinatorReadClient
				.getShardMap();
		ShardDto shard = shardMap.getShards().get(shardId);
		if (shard == null) {
			throw new IllegalArgumentException("Shard not found: " + shardId);
		}
		return shard;
	}

	public ShardDto setShardReplicas(String shardId, List<String> replicaNodeIds) {
		coordinatorAdminClient.setShardReplicas(shardId, replicaNodeIds);
		return getShard(shardId);
	}

	public ShardDto setShardLeader(String shardId, String leaderNodeId) {
		ShardDto currentShard = getShard(shardId);
		coordinatorAdminClient.setShardLeader(shardId, currentShard.getEpoch(), leaderNodeId);
		return getShard(shardId);
	}

	public TriggerRequestDto triggerRebalance(TriggerRequestDto request) {
		// TODO: Implement rebalancing logic
		log.info("Triggering rebalance: {}", request);
		return request;
	}
}

