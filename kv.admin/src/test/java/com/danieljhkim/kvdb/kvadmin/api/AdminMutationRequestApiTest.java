package com.danieljhkim.kvdb.kvadmin.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.danieljhkim.kvdb.kvadmin.AdminApplication;
import com.danieljhkim.kvdb.kvadmin.api.dto.NodeDto;
import com.danieljhkim.kvdb.kvadmin.api.dto.ShardDto;
import com.danieljhkim.kvdb.kvadmin.api.dto.TriggerRequestDto;
import com.danieljhkim.kvdb.kvadmin.client.CoordinatorAdminClient;
import com.danieljhkim.kvdb.kvadmin.client.CoordinatorReadClient;
import com.danieljhkim.kvdb.kvadmin.client.NodeAdminClient;
import com.danieljhkim.kvdb.kvadmin.config.AdminServerConfig;
import com.danieljhkim.kvdb.kvadmin.service.NodeAdminService;
import com.danieljhkim.kvdb.kvadmin.service.OpsService;
import com.danieljhkim.kvdb.kvadmin.service.ShardAdminService;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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

class AdminMutationRequestApiTest {

    private static final String API_KEY = "test-api-key";

    @Test
    void mutationEndpointsAcceptTypedJsonAndRejectBadInput() throws Exception {
        try (ConfigurableApplicationContext context = startAdmin()) {
            int port = ((ServletWebServerApplicationContext) context)
                    .getWebServer()
                    .getPort();
            ObjectMapper mapper = context.getBean(ObjectMapper.class);
            assertTrue(mapper.isEnabled(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES));
            RecordingShardAdminService shards = context.getBean(RecordingShardAdminService.class);
            RecordingNodeAdminService nodes = context.getBean(RecordingNodeAdminService.class);
            RecordingOpsService ops = context.getBean(RecordingOpsService.class);
            HttpClient http = HttpClient.newHttpClient();

            HttpResponse<String> leader =
                    post(http, port, "/admin/shards/shard-0/leader", json(), "{\"leader_node_id\":\"node-9\"}");
            assertEquals(200, leader.statusCode());
            assertEquals("node-9", mapper.readTree(leader.body()).get("leader").asText());
            assertEquals("node-9", shards.lastLeader.get());
            assertFalse(shards.lastLeader.get().contains("{"));
            assertEquals(1, shards.leaderCalls.get());

            HttpResponse<String> status =
                    post(http, port, "/admin/nodes/node-1/status", json(), "{\"status\":\"ALIVE\"}");
            assertEquals(200, status.statusCode());
            assertEquals("ALIVE", mapper.readTree(status.body()).get("status").asText());
            assertEquals("ALIVE", nodes.lastStatus.get());
            assertFalse(nodes.lastStatus.get().contains("{"));
            assertEquals(1, nodes.statusCalls.get());

            HttpResponse<String> triggered = post(
                    http,
                    port,
                    "/admin/ops/trigger",
                    json(),
                    "{\"operation\":\"REBALANCE\",\"target_nodes\":[\"node-1\"]}");
            assertEquals(200, triggered.statusCode());
            JsonNode triggeredBody = mapper.readTree(triggered.body());
            assertEquals("REBALANCE", triggeredBody.get("operation").asText());
            assertEquals("node-1", triggeredBody.get("target_nodes").get(0).asText());
            assertEquals(1, ops.triggerCalls.get());

            HttpResponse<String> rebalance = post(http, port, "/admin/ops/rebalance", json(), "{}");
            assertEquals(200, rebalance.statusCode());
            assertEquals(1, ops.triggerCalls.get());

            HttpResponse<String> banana =
                    post(http, port, "/admin/nodes/node-1/status", json(), "{\"status\":\"BANANA\"}");
            assertClientError(banana, 400, "VALIDATION_ERROR");
            assertTrue(mapper.readTree(banana.body()).get("message").asText().contains("status"));
            assertFalse(banana.body().contains("BANANA"));
            assertEquals(1, nodes.statusCalls.get());

            HttpResponse<String> missingStatus = post(http, port, "/admin/nodes/node-1/status", json(), "{}");
            assertClientError(missingStatus, 400, "VALIDATION_ERROR");
            assertTrue(mapper.readTree(missingStatus.body())
                    .get("message")
                    .asText()
                    .contains("status"));
            assertEquals(1, nodes.statusCalls.get());

            HttpResponse<String> missingLeader = post(http, port, "/admin/shards/shard-0/leader", json(), "{}");
            assertClientError(missingLeader, 400, "VALIDATION_ERROR");
            assertTrue(mapper.readTree(missingLeader.body())
                    .get("message")
                    .asText()
                    .contains("leader_node_id"));
            assertEquals(1, shards.leaderCalls.get());

            HttpResponse<String> blankLeader =
                    post(http, port, "/admin/shards/shard-0/leader", json(), "{\"leader_node_id\":\"\"}");
            assertClientError(blankLeader, 400, "VALIDATION_ERROR");
            assertEquals(1, shards.leaderCalls.get());

            HttpResponse<String> jsonString = post(http, port, "/admin/shards/shard-0/leader", json(), "\"node-9\"");
            assertClientError(jsonString, 400, "INVALID_REQUEST");
            assertFalse(jsonString.body().contains("node-9"));
            assertEquals(1, shards.leaderCalls.get());

            HttpResponse<String> verbatim = post(
                    http, port, "/admin/shards/shard-0/leader", json(), "\"{\\\"leader_node_id\\\":\\\"node-9\\\"}\"");
            assertClientError(verbatim, 400, "INVALID_REQUEST");
            assertFalse(verbatim.body().contains("leader_node_id"));
            assertEquals(1, shards.leaderCalls.get());

            HttpResponse<String> statusString = post(http, port, "/admin/nodes/node-1/status", json(), "\"ALIVE\"");
            assertClientError(statusString, 400, "INVALID_REQUEST");
            assertEquals(1, nodes.statusCalls.get());

            HttpResponse<String> textLeader = post(http, port, "/admin/shards/shard-0/leader", "text/plain", "node-9");
            assertClientError(textLeader, 415, "UNSUPPORTED_MEDIA_TYPE");
            assertFalse(textLeader.body().contains("node-9"));
            assertEquals(1, shards.leaderCalls.get());

            HttpResponse<String> camelLeader =
                    post(http, port, "/admin/shards/shard-0/leader", json(), "{\"leaderNodeId\":\"node-9\"}");
            assertClientError(camelLeader, 400, "UNKNOWN_FIELD");
            assertTrue(
                    mapper.readTree(camelLeader.body()).get("message").asText().contains("leaderNodeId"));
            assertEquals(1, shards.leaderCalls.get());

            HttpResponse<String> malformedLeader = post(http, port, "/admin/shards/shard-0/leader", json(), "{bad");
            assertClientError(malformedLeader, 400, "MALFORMED_JSON");
            assertEquals(1, shards.leaderCalls.get());

            HttpResponse<String> malformedTrigger = post(http, port, "/admin/ops/trigger", json(), "{bad");
            assertClientError(malformedTrigger, 400, "MALFORMED_JSON");
            assertEquals(
                    "Request body is not valid JSON",
                    mapper.readTree(malformedTrigger.body()).get("message").asText());
            assertEquals(1, ops.triggerCalls.get());

            HttpResponse<String> missingOperation = post(http, port, "/admin/ops/trigger", json(), "{}");
            assertClientError(missingOperation, 400, "VALIDATION_ERROR");
            assertTrue(mapper.readTree(missingOperation.body())
                    .get("message")
                    .asText()
                    .contains("operation"));
            assertEquals(1, ops.triggerCalls.get());

            HttpResponse<String> camelNodes = post(
                    http,
                    port,
                    "/admin/ops/trigger",
                    json(),
                    "{\"operation\":\"REBALANCE\",\"targetNodes\":[\"node-1\"]}");
            assertClientError(camelNodes, 400, "UNKNOWN_FIELD");
            assertTrue(
                    mapper.readTree(camelNodes.body()).get("message").asText().contains("targetNodes"));
            assertFalse(mapper.readTree(camelNodes.body()).has("target_nodes"));
            assertEquals(1, ops.triggerCalls.get());
        }
    }

    @Test
    void compactionEndpointsReturnNotImplemented() throws Exception {
        try (ConfigurableApplicationContext context = startAdmin()) {
            int port = ((ServletWebServerApplicationContext) context)
                    .getWebServer()
                    .getPort();
            HttpClient http = HttpClient.newHttpClient();
            RecordingOpsService ops = context.getBean(RecordingOpsService.class);

            HttpResponse<String> compact =
                    post(http, port, "/admin/ops/compact", json(), "{\"target_nodes\":[\"localhost:1\"]}");
            assertClientError(compact, 501, "NOT_IMPLEMENTED");
            assertTrue(compact.body().contains("Node compaction RPC is not implemented"));
            assertFalse(compact.body().contains("target_nodes"));

            HttpResponse<String> triggered = post(
                    http,
                    port,
                    "/admin/ops/trigger",
                    json(),
                    "{\"operation\":\"COMPACT\",\"target_nodes\":[\"localhost:1\"]}");
            assertClientError(triggered, 501, "NOT_IMPLEMENTED");
            assertTrue(triggered.body().contains("Node compaction RPC is not implemented"));
            assertFalse(triggered.body().contains("target_nodes"));
            assertEquals(1, ops.triggerCalls.get());
        }
    }

    private static void assertClientError(HttpResponse<String> response, int status, String errorCode)
            throws IOException {
        assertEquals(status, response.statusCode());
        JsonNode body = new ObjectMapper().readTree(response.body());
        assertEquals(errorCode, body.get("error").asText());
        assertEquals(String.valueOf(status), body.get("code").asText());
        assertFalse(body.get("message").asText().isBlank());
        String raw = response.body();
        assertFalse(raw.contains("Exception"));
        assertFalse(raw.contains("java."));
        assertFalse(raw.contains("com.fasterxml"));
        assertFalse(raw.contains("com.danieljhkim"));
        assertFalse(raw.toLowerCase().contains("json parse error"));
        assertFalse(raw.contains("toUpperCase"));
        assertFalse(raw.contains("NullPointer"));
        assertFalse(raw.contains("No enum constant"));
    }

    private static ConfigurableApplicationContext startAdmin() {
        return new SpringApplicationBuilder(AdminApplication.class, RecordingAdminConfiguration.class)
                .web(WebApplicationType.SERVLET)
                .run(
                        "--server.port=0",
                        "--server.shutdown=immediate",
                        "--kvdb.admin.security.api-key=" + API_KEY,
                        "--kvdb.coordinator.grpc.address=localhost:1",
                        "--kvdb.coordinator.grpc.deadline-ms=50",
                        "--spring.profiles.active=admin-mutation-api-test");
    }

    private static String json() {
        return "application/json";
    }

    private static HttpResponse<String> post(HttpClient http, int port, String path, String contentType, String body)
            throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + port + path))
                .header("Content-Type", contentType)
                .header("X-Admin-Api-Key", API_KEY)
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .build();
        return http.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    }

    @Configuration(proxyBeanMethods = false)
    @Profile("admin-mutation-api-test")
    static class RecordingAdminConfiguration {
        @Bean
        @Primary
        RecordingShardAdminService recordingShardAdminService() {
            return new RecordingShardAdminService();
        }

        @Bean
        @Primary
        RecordingNodeAdminService recordingNodeAdminService() {
            return new RecordingNodeAdminService();
        }

        @Bean
        @Primary
        RecordingOpsService recordingOpsService(RecordingShardAdminService shards) {
            return new RecordingOpsService(shards);
        }
    }

    static final class RecordingShardAdminService extends ShardAdminService {
        private final AtomicReference<String> lastLeader = new AtomicReference<>();
        private final AtomicInteger leaderCalls = new AtomicInteger();

        RecordingShardAdminService() {
            super(
                    new CoordinatorAdminClient(List.of("localhost:1"), 1, TimeUnit.MILLISECONDS),
                    new CoordinatorReadClient(List.of("localhost:1"), 1, TimeUnit.MILLISECONDS),
                    new AdminServerConfig());
        }

        @Override
        public ShardDto setShardLeader(String shardId, String leaderNodeId) {
            leaderCalls.incrementAndGet();
            lastLeader.set(leaderNodeId);
            return ShardDto.builder()
                    .shardId(shardId)
                    .leader(leaderNodeId)
                    .epoch(1)
                    .build();
        }
    }

    static final class RecordingNodeAdminService extends NodeAdminService {
        private final AtomicReference<String> lastStatus = new AtomicReference<>();
        private final AtomicInteger statusCalls = new AtomicInteger();

        RecordingNodeAdminService() {
            super(
                    new CoordinatorAdminClient(List.of("localhost:1"), 1, TimeUnit.MILLISECONDS),
                    new CoordinatorReadClient(List.of("localhost:1"), 1, TimeUnit.MILLISECONDS),
                    new NodeAdminClient(1, TimeUnit.MILLISECONDS));
        }

        @Override
        public NodeDto setNodeStatus(String nodeId, String status) {
            statusCalls.incrementAndGet();
            lastStatus.set(status);
            return NodeDto.builder().nodeId(nodeId).status(status).build();
        }
    }

    static final class RecordingOpsService extends OpsService {
        private final AtomicInteger triggerCalls = new AtomicInteger();

        RecordingOpsService(ShardAdminService shards) {
            super(shards);
        }

        @Override
        public TriggerRequestDto triggerOperation(TriggerRequestDto request) {
            triggerCalls.incrementAndGet();
            return super.triggerOperation(request);
        }
    }
}
