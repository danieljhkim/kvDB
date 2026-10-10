package com.danieljhkim.kvdb.kvadmin.api;

import com.danieljhkim.kvdb.kvadmin.api.dto.KeyPlacementDto;
import com.danieljhkim.kvdb.kvadmin.api.dto.ResolveKeyRequestDto;
import com.danieljhkim.kvdb.kvadmin.api.dto.SetShardLeaderRequestDto;
import com.danieljhkim.kvdb.kvadmin.api.dto.ShardDto;
import com.danieljhkim.kvdb.kvadmin.api.dto.TriggerRequestDto;
import com.danieljhkim.kvdb.kvadmin.service.ShardAdminService;
import jakarta.validation.Valid;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST controller for shard operations.
 *
 * <p>
 * Endpoints: - GET /admin/shards - List all shards - GET /admin/shards/{shardId} - Get shard details - POST
 * /admin/shards/resolve-key - Coordinator placement for a binary key - POST /admin/shards/{shardId}/replicas - Update
 * shard replicas - POST /admin/shards/{shardId}/leader - Update shard leader from JSON
 * {@code {"leader_node_id":"..."}} - POST /admin/shards/rebalance - Trigger shard rebalancing
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
    public ResponseEntity<ShardDto> getShard(@PathVariable("shardId") String shardId) {
        ShardDto shard = shardAdminService.getShard(shardId);
        return ResponseEntity.ok(shard);
    }

    /**
     * Return coordinator placement for a base64-encoded binary key at observation time. This is not
     * a value existence check and does not inspect the gateway cache.
     */
    @PostMapping("/resolve-key")
    public ResponseEntity<KeyPlacementDto> resolveKey(@RequestBody ResolveKeyRequestDto request) {
        return ResponseEntity.ok(shardAdminService.resolveKeyPlacement(request));
    }

    @PostMapping("/{shardId}/replicas")
    public ResponseEntity<ShardDto> setShardReplicas(
            @PathVariable("shardId") String shardId, @RequestBody List<String> replicaNodeIds) {
        ShardDto shard = shardAdminService.setShardReplicas(shardId, replicaNodeIds);
        return ResponseEntity.ok(shard);
    }

    /**
     * Set the shard leader. Requires {@code application/json} with a non-blank {@code leader_node_id}. The raw
     * request body is never stored as the leader id.
     */
    @PostMapping(value = "/{shardId}/leader", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<ShardDto> setShardLeader(
            @PathVariable("shardId") String shardId, @Valid @RequestBody SetShardLeaderRequestDto request) {
        ShardDto shard = shardAdminService.setShardLeader(shardId, request.getLeaderNodeId());
        return ResponseEntity.ok(shard);
    }

    @PostMapping("/rebalance")
    public ResponseEntity<TriggerRequestDto> triggerRebalance(@RequestBody TriggerRequestDto request) {
        TriggerRequestDto result = shardAdminService.triggerRebalance(request);
        return ResponseEntity.ok(result);
    }
}
