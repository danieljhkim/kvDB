package com.danieljhkim.kvdb.kvadmin.health;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Liveness health indicator for Kubernetes/container orchestration.
 * 
 * <p>
 * Returns UP if the admin server is running and can respond.
 */
@Component
public class LivenessIndicator implements HealthIndicator {

	@Override
	public Health health() {
		// Admin server is alive if it can respond
		return Health.up()
				.withDetail("status", "alive")
				.build();
	}
}
