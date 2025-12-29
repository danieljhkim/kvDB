package com.danieljhkim.kvdb.kvadmin.api.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Partitioning configuration.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PartitioningConfigDto {

	private int numShards;
	private int replicationFactor;
}

