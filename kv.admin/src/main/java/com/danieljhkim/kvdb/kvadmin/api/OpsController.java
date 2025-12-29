package com.danieljhkim.kvdb.kvadmin.api;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.danieljhkim.kvdb.kvadmin.api.dto.TriggerRequestDto;
import com.danieljhkim.kvdb.kvadmin.service.OpsService;

import lombok.RequiredArgsConstructor;

/**
 * REST controller for operational tasks (rebalance, compaction, etc.).
 * 
 * <p>
 * Endpoints:
 * - POST /admin/ops/rebalance - Trigger cluster rebalancing
 * - POST /admin/ops/compact - Trigger compaction on nodes
 * - POST /admin/ops/trigger - Generic operation trigger
 */
@RestController
@RequestMapping("/admin/ops")
@RequiredArgsConstructor
public class OpsController {

	private final OpsService opsService;

	@PostMapping("/rebalance")
	public ResponseEntity<TriggerRequestDto> triggerRebalance(@RequestBody TriggerRequestDto request) {
		TriggerRequestDto result = opsService.triggerRebalance(request);
		return ResponseEntity.ok(result);
	}

	@PostMapping("/compact")
	public ResponseEntity<TriggerRequestDto> triggerCompaction(@RequestBody TriggerRequestDto request) {
		TriggerRequestDto result = opsService.triggerCompaction(request);
		return ResponseEntity.ok(result);
	}

	@PostMapping("/trigger")
	public ResponseEntity<TriggerRequestDto> triggerOperation(@RequestBody TriggerRequestDto request) {
		TriggerRequestDto result = opsService.triggerOperation(request);
		return ResponseEntity.ok(result);
	}
}

