package com.danieljhkim.kvdb.kvadmin.api.dto;

import java.util.List;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Summary of cluster state.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ClusterSummaryDto {

	private long mapVersion;
	private int totalNodes;
	private int aliveNodes;
	private int suspectNodes;
	private int deadNodes;
	private int totalShards;
	private int stableShards;
	private int movingShards;
	private int replicationFactor;
	private Map<String, String> partitioningConfig;
	private List<NodeDto> nodes;
	private List<ShardDto> shards;
}

