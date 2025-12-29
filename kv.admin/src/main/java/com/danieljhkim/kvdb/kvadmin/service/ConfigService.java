package com.danieljhkim.kvdb.kvadmin.service;

import java.util.Map;

import org.springframework.stereotype.Service;

import com.danieljhkim.kvdb.kvadmin.client.CoordinatorAdminClient;
import com.danieljhkim.kvdb.kvadmin.client.CoordinatorReadClient;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Service for configuration operations.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class ConfigService {

	private final CoordinatorAdminClient coordinatorAdminClient;
	private final CoordinatorReadClient coordinatorReadClient;

	public Map<String, Object> getConfig() {
		com.danieljhkim.kvdb.kvadmin.api.dto.ShardMapSnapshotDto shardMap = coordinatorReadClient
				.getShardMap();
		if (shardMap == null) {
			throw new IllegalStateException("Shard map not available: cannot get config");
		}
		return Map.of(
				"map_version", shardMap.getMapVersion(),
				"num_shards", shardMap.getPartitioning() != null
						? shardMap.getPartitioning().getNumShards()
						: 0,
				"replication_factor", shardMap.getPartitioning() != null
						? shardMap.getPartitioning().getReplicationFactor()
						: 0);
	}

	public Map<String, Object> updateConfig(Map<String, Object> config) {
		// TODO: Implement config update logic
		log.info("Updating configuration: {}", config);
		return config;
	}

	public Map<String, Object> initShards(Map<String, Object> params) {
		int numShards = (Integer) params.getOrDefault("num_shards", 8);
		int replicationFactor = (Integer) params.getOrDefault("replication_factor", 2);

		coordinatorAdminClient.initShards(numShards, replicationFactor);

		return Map.of(
				"success", true,
				"num_shards", numShards,
				"replication_factor", replicationFactor);
	}
}
