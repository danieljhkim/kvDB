package com.danieljhkim.kvdb.kvadmin.api.dto;

import java.util.List;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Full shard map snapshot.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ShardMapSnapshotDto {

	private long mapVersion;
	private Map<String, NodeDto> nodes;
	private Map<String, ShardDto> shards;
	private PartitioningConfigDto partitioning;
}
