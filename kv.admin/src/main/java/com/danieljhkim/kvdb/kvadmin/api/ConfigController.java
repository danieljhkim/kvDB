package com.danieljhkim.kvdb.kvadmin.api;

import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.danieljhkim.kvdb.kvadmin.service.ConfigService;

import lombok.RequiredArgsConstructor;

/**
 * REST controller for configuration operations.
 * 
 * <p>
 * Endpoints:
 * - GET /admin/config - Get current configuration
 * - POST /admin/config - Update configuration
 * - POST /admin/config/shard-init - Initialize shards
 */
@RestController
@RequestMapping("/admin/config")
@RequiredArgsConstructor
public class ConfigController {

	private final ConfigService configService;

	@GetMapping
	public ResponseEntity<Map<String, Object>> getConfig() {
		Map<String, Object> config = configService.getConfig();
		return ResponseEntity.ok(config);
	}

	@PostMapping
	public ResponseEntity<Map<String, Object>> updateConfig(@RequestBody Map<String, Object> config) {
		Map<String, Object> updated = configService.updateConfig(config);
		return ResponseEntity.ok(updated);
	}

	@PostMapping("/shard-init")
	public ResponseEntity<Map<String, Object>> initShards(@RequestBody Map<String, Object> params) {
		Map<String, Object> result = configService.initShards(params);
		return ResponseEntity.ok(result);
	}
}

