package com.danieljhkim.kvdb.kvadmin.service;

import java.util.List;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;

import com.danieljhkim.kvdb.kvadmin.api.dto.HealthDto;
import com.danieljhkim.kvdb.kvadmin.api.dto.NodeDto;
import com.danieljhkim.kvdb.kvadmin.client.CoordinatorAdminClient;
import com.danieljhkim.kvdb.kvadmin.client.CoordinatorReadClient;
import com.danieljhkim.kvdb.kvadmin.client.NodeAdminClient;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Service for node administration operations.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class NodeAdminService {

	private final CoordinatorAdminClient coordinatorAdminClient;
	private final CoordinatorReadClient coordinatorReadClient;
	private final NodeAdminClient nodeAdminClient;

	public List<NodeDto> listNodes() {
		com.danieljhkim.kvdb.kvadmin.api.dto.ShardMapSnapshotDto shardMap = coordinatorReadClient
				.getShardMap();
		return shardMap.getNodes().values().stream()
				.collect(Collectors.toList());
	}

	public NodeDto getNode(String nodeId) {
		com.danieljhkim.kvdb.kvadmin.api.dto.ShardMapSnapshotDto shardMap = coordinatorReadClient
				.getShardMap();
		NodeDto node = shardMap.getNodes().get(nodeId);
		if (node == null) {
			throw new IllegalArgumentException("Node not found: " + nodeId);
		}
		return node;
	}

	public HealthDto getNodeHealth(String nodeId) {
		NodeDto node = getNode(nodeId);
		try {
			// Try to ping the node
			boolean healthy = nodeAdminClient.ping(node.getAddress());
			return HealthDto.builder()
					.status(healthy ? "UP" : "DOWN")
					.message(healthy ? "Node is healthy" : "Node is not responding")
					.timestampMs(System.currentTimeMillis())
					.build();
		} catch (Exception e) {
			log.warn("Failed to check node health: {}", nodeId, e);
			return HealthDto.builder()
					.status("DOWN")
					.message("Failed to connect to node: " + e.getMessage())
					.timestampMs(System.currentTimeMillis())
					.build();
		}
	}

	public NodeDto registerNode(NodeDto node) {
		coordinatorAdminClient.registerNode(
				node.getNodeId(),
				node.getAddress(),
				node.getZone());
		return getNode(node.getNodeId());
	}

	public NodeDto setNodeStatus(String nodeId, String status) {
		coordinatorAdminClient.setNodeStatus(nodeId, status);
		return getNode(nodeId);
	}
}

