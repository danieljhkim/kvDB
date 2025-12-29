package com.danieljhkim.kvdb.kvadmin.api.dto;

import java.util.List;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Request to trigger an operation (rebalance, compaction, etc.).
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TriggerRequestDto {

	private String operation; // REBALANCE, COMPACT, etc.
	private Map<String, String> parameters;
	private List<String> targetShards; // optional: specific shards
	private List<String> targetNodes; // optional: specific nodes
}
