package com.danieljhkim.kvdb.kvadmin.api.dto;

import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Node information.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NodeDto {

	private String nodeId;
	private String address;
	private String zone;
	private String rack;
	private String status; // ALIVE, SUSPECT, DEAD
	private long lastHeartbeatMs;
	private Map<String, String> capacityHints;
}
