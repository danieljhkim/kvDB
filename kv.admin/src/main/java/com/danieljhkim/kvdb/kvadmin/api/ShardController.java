package com.danieljhkim.kvdb.kvadmin.api;

import java.util.List;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.danieljhkim.kvdb.kvadmin.api.dto.ShardDto;
import com.danieljhkim.kvdb.kvadmin.api.dto.TriggerRequestDto;
import com.danieljhkim.kvdb.kvadmin.service.ShardAdminService;

import lombok.RequiredArgsConstructor;

/**
 * REST controller for shard operations.
 * 
 * <p>
 * Endpoints:
 * - GET /admin/shards - List all shards
 * - GET /admin/shards/{shardId} - Get shard details
 * - POST /admin/shards/{shardId}/replicas - Update shard replicas
 * - POST /admin/shards/{shardId}/leader - Update shard leader
 * - POST /admin/shards/rebalance - Trigger shard rebalancing
 */
@RestController
@RequestMapping("/admin/shards")
@RequiredArgsConstructor
public class ShardController {

	private final ShardAdminService shardAdminService;

	@GetMapping
	public ResponseEntity<List<ShardDto>> listShards() {
		List<ShardDto> shards = shardAdminService.listShards();
		return ResponseEntity.ok(shards);
	}

	@GetMapping("/{shardId}")
	public ResponseEntity<ShardDto> getShard(@PathVariable String shardId) {
		ShardDto shard = shardAdminService.getShard(shardId);
		return ResponseEntity.ok(shard);
	}

	@PostMapping("/{shardId}/replicas")
	public ResponseEntity<ShardDto> setShardReplicas(
			@PathVariable String shardId,
			@RequestBody List<String> replicaNodeIds) {
		ShardDto shard = shardAdminService.setShardReplicas(shardId, replicaNodeIds);
		return ResponseEntity.ok(shard);
	}

	@PostMapping("/{shardId}/leader")
	public ResponseEntity<ShardDto> setShardLeader(
			@PathVariable String shardId,
			@RequestBody String leaderNodeId) {
		ShardDto shard = shardAdminService.setShardLeader(shardId, leaderNodeId);
		return ResponseEntity.ok(shard);
	}

	@PostMapping("/rebalance")
	public ResponseEntity<TriggerRequestDto> triggerRebalance(@RequestBody TriggerRequestDto request) {
		TriggerRequestDto result = shardAdminService.triggerRebalance(request);
		return ResponseEntity.ok(result);
	}
}

