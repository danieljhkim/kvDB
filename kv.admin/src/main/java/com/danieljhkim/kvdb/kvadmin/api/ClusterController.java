package com.danieljhkim.kvdb.kvadmin.api;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.danieljhkim.kvdb.kvadmin.api.dto.ClusterSummaryDto;
import com.danieljhkim.kvdb.kvadmin.api.dto.ShardMapSnapshotDto;
import com.danieljhkim.kvdb.kvadmin.service.ClusterAdminService;

import lombok.RequiredArgsConstructor;

/**
 * REST controller for cluster-level operations.
 * 
 * <p>
 * Endpoints:
 * - GET /admin/cluster/summary - Get cluster summary
 * - GET /admin/cluster/shard-map - Get full shard map
 */
@RestController
@RequestMapping("/admin/cluster")
@RequiredArgsConstructor
public class ClusterController {

	private final ClusterAdminService clusterAdminService;

	@GetMapping("/summary")
	public ResponseEntity<ClusterSummaryDto> getSummary() {
		ClusterSummaryDto summary = clusterAdminService.getClusterSummary();
		return ResponseEntity.ok(summary);
	}

	@GetMapping("/shard-map")
	public ResponseEntity<ShardMapSnapshotDto> getShardMap() {
		ShardMapSnapshotDto shardMap = clusterAdminService.getShardMap();
		return ResponseEntity.ok(shardMap);
	}
}

