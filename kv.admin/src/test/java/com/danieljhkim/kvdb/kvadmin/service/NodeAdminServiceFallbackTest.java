package com.danieljhkim.kvdb.kvadmin.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.danieljhkim.kvdb.kvadmin.AdminApplication;
import com.danieljhkim.kvdb.kvadmin.api.dto.NodeDto;
import com.danieljhkim.kvdb.kvadmin.api.dto.ShardMapSnapshotDto;
import com.danieljhkim.kvdb.kvadmin.client.CoordinatorReadClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.Status;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;

class NodeAdminServiceFallbackTest {

    private static final String API_KEY = "node-fallback-test-key";

    @Test
    void missingNodeReturnsNotFoundAndOtherRpcFailuresStillUseShardMap() throws Exception {
        try (ConfigurableApplicationContext context = startAdmin()) {
            int port = ((ServletWebServerApplicationContext) context)
                    .getWebServer()
                    .getPort();
            ObjectMapper mapper = context.getBean(ObjectMapper.class);
            StubCoordinatorReadClient coordinator = context.getBean(StubCoordinatorReadClient.class);
            HttpClient http = HttpClient.newHttpClient();

            coordinator.setStatus(Status.NOT_FOUND);
            coordinator.setNodes(Map.of());
            HttpResponse<String> missing = getNode(http, port, "node-missing");
            assertEquals(404, missing.statusCode());
            JsonNode missingBody = mapper.readTree(missing.body());
            assertEquals("NOT_FOUND", missingBody.get("error").asText());
            assertEquals("404", missingBody.get("code").asText());
            assertNotEquals("INVALID_ARGUMENT", missingBody.get("error").asText());

            NodeDto fallbackNode = NodeDto.builder()
                    .nodeId("node-from-map")
                    .address("localhost:9001")
                    .status("ALIVE")
                    .build();
            coordinator.setStatus(Status.UNAVAILABLE);
            coordinator.setNodes(Map.of(fallbackNode.getNodeId(), fallbackNode));
            HttpResponse<String> fallback = getNode(http, port, fallbackNode.getNodeId());
            assertEquals(200, fallback.statusCode(), fallback.body());
            assertEquals(
                    "node-from-map",
                    mapper.readTree(fallback.body()).get("node_id").asText());
        }
    }

    private static ConfigurableApplicationContext startAdmin() {
        return new SpringApplicationBuilder(AdminApplication.class, StubCoordinatorConfiguration.class)
                .web(WebApplicationType.SERVLET)
                .run(
                        "--server.port=0",
                        "--kvdb.admin.security.api-key=" + API_KEY,
                        "--kvdb.coordinator.grpc.address=localhost:1",
                        "--spring.profiles.active=node-fallback-test");
    }

    private static HttpResponse<String> getNode(HttpClient http, int port, String nodeId) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + port + "/admin/nodes/" + nodeId))
                .header("X-Admin-Api-Key", API_KEY)
                .GET()
                .build();
        return http.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    }

    @Configuration(proxyBeanMethods = false)
    @Profile("node-fallback-test")
    static class StubCoordinatorConfiguration {
        @Bean
        @Primary
        StubCoordinatorReadClient stubCoordinatorReadClient() {
            return new StubCoordinatorReadClient();
        }
    }

    static final class StubCoordinatorReadClient extends CoordinatorReadClient {
        private final AtomicReference<Status> status = new AtomicReference<>(Status.NOT_FOUND);
        private final AtomicReference<Map<String, NodeDto>> nodes = new AtomicReference<>(Map.of());

        StubCoordinatorReadClient() {
            super(List.of("localhost:1"), 1, TimeUnit.MILLISECONDS);
        }

        @Override
        public NodeDto getNode(String nodeId) {
            throw status.get().asRuntimeException();
        }

        @Override
        public ShardMapSnapshotDto getShardMap() {
            return ShardMapSnapshotDto.builder().nodes(nodes.get()).build();
        }

        void setStatus(Status next) {
            status.set(next);
        }

        void setNodes(Map<String, NodeDto> next) {
            nodes.set(next);
        }
    }
}
