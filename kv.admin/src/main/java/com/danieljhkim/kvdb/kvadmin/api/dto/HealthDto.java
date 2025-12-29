package com.danieljhkim.kvdb.kvadmin.api.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Health check response.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class HealthDto {

	private String status; // UP, DOWN, DEGRADED
	private String message;
	private long timestampMs;
}
