package com.danieljhkim.kvdb.kvadmin.health;

import com.danieljhkim.kvdb.kvadmin.client.CoordinatorReadClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Readiness health indicator for Kubernetes/container orchestration.
 *
 * <p>
 * Returns UP if the admin server can connect to the coordinator.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class ReadinessIndicator implements HealthIndicator {

    private final CoordinatorReadClient coordinatorReadClient;

    @Override
    public Health health() {
        try {
            // Try to fetch shard map to verify coordinator connectivity
            coordinatorReadClient.getShardMap();
            return Health.up().withDetail("coordinator", "reachable").build();
        } catch (Exception e) {
            log.warn("Coordinator not reachable", e);
            return Health.down()
                    .withDetail("coordinator", "unreachable")
                    .withDetail("error", e.getMessage())
                    .build();
        }
    }
}
