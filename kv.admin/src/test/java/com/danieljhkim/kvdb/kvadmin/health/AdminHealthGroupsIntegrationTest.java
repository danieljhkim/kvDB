package com.danieljhkim.kvdb.kvadmin.health;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.danieljhkim.kvdb.kvadmin.AdminApplication;
import com.danieljhkim.kvdb.kvadmin.api.dto.ShardMapSnapshotDto;
import com.danieljhkim.kvdb.kvadmin.client.CoordinatorReadClient;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.actuate.health.HealthContributorRegistry;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

class AdminHealthGroupsIntegrationTest {

    @Test
    void healthGroupsBootAndReadinessTracksCoordinatorAvailability() {
        try (ConfigurableApplicationContext context = new SpringApplicationBuilder(
                        AdminApplication.class, FakeCoordinatorConfiguration.class)
                .web(WebApplicationType.NONE)
                .run("--kvdb.admin.security.api-key=test-api-key")) {
            HealthContributorRegistry contributors = context.getBean(HealthContributorRegistry.class);
            assertNotNull(contributors.getContributor("readinessIndicator"));
            assertNotNull(contributors.getContributor("livenessIndicator"));

            CoordinatorAvailability availability = context.getBean(CoordinatorAvailability.class);
            ReadinessIndicator readiness = context.getBean(ReadinessIndicator.class);
            LivenessIndicator liveness = context.getBean(LivenessIndicator.class);

            assertEquals(Status.UP, readiness.health().getStatus());
            assertEquals(Status.UP, liveness.health().getStatus());

            availability.setReachable(false);

            assertEquals(Status.DOWN, readiness.health().getStatus());
            assertEquals(Status.UP, liveness.health().getStatus());
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class FakeCoordinatorConfiguration {

        @Bean
        CoordinatorAvailability coordinatorAvailability() {
            return new CoordinatorAvailability();
        }

        @Bean
        @Primary
        CoordinatorReadClient testCoordinatorReadClient(CoordinatorAvailability availability) {
            return new FakeCoordinatorReadClient(availability);
        }
    }

    static class CoordinatorAvailability {
        private final AtomicBoolean reachable = new AtomicBoolean(true);

        boolean isReachable() {
            return reachable.get();
        }

        void setReachable(boolean reachable) {
            this.reachable.set(reachable);
        }
    }

    static class FakeCoordinatorReadClient extends CoordinatorReadClient {
        private final CoordinatorAvailability availability;

        FakeCoordinatorReadClient(CoordinatorAvailability availability) {
            super(List.of("localhost:1"), 1, TimeUnit.MILLISECONDS);
            this.availability = availability;
        }

        @Override
        public ShardMapSnapshotDto getShardMap() {
            if (!availability.isReachable()) {
                throw new IllegalStateException("coordinator unavailable");
            }
            return null;
        }
    }
}
