package com.danieljhkim.kvdb.kvadmin.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.danieljhkim.kvdb.kvadmin.AdminApplication;
import com.danieljhkim.kvdb.kvadmin.api.dto.NodeDto;
import com.danieljhkim.kvdb.kvadmin.api.dto.ShardDto;
import com.danieljhkim.kvdb.kvadmin.api.dto.ShardMapSnapshotDto;
import com.danieljhkim.kvdb.kvadmin.client.CoordinatorAdminClient;
import com.danieljhkim.kvdb.kvadmin.client.CoordinatorReadClient;
import com.danieljhkim.kvdb.proto.coordinator.InitShardsResponse;
import com.danieljhkim.kvdb.proto.coordinator.RegisterNodeResponse;
import com.danieljhkim.kvdb.proto.coordinator.SetNodeStatusResponse;
import com.danieljhkim.kvdb.proto.coordinator.SetShardLeaderResponse;
import com.danieljhkim.kvdb.proto.coordinator.SetShardReplicasResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;

class AdminMutationOutcomeTest {

    private static final String API_KEY = "mutation-outcome-test-key";
    private static final String UNSAFE_MESSAGE = "java.lang.IllegalStateException: internal coordinator failure";
    private static final List<Mutation> MUTATIONS = List.of(
            new Mutation("/admin/config/shard-init", "{\"num_shards\":4,\"replication_factor\":2}"),
            new Mutation("/admin/nodes", "{\"node_id\":\"node-1\",\"address\":\"localhost:9002\",\"zone\":\"zone-a\"}"),
            new Mutation("/admin/nodes/node-1/status", "{\"status\":\"ALIVE\"}"),
            new Mutation("/admin/shards/shard-0/replicas", "[\"node-new\"]"),
            new Mutation("/admin/shards/shard-0/leader", "{\"leader_node_id\":\"node-new\"}"));

    private static ConfigurableApplicationContext context;
    private static StubAdminClient admin;
    private static int port;
    private static final HttpClient HTTP = HttpClient.newHttpClient();
    private static final ObjectMapper JSON = new ObjectMapper();

    @BeforeAll
    static void startAdmin() {
        context = new SpringApplicationBuilder(AdminApplication.class, StubConfiguration.class)
                .web(WebApplicationType.SERVLET)
                .run(
                        "--server.port=0",
                        "--server.shutdown=immediate",
                        "--kvdb.admin.security.api-key=" + API_KEY,
                        "--spring.profiles.active=admin-mutation-outcome-test");
        port = ((ServletWebServerApplicationContext) context).getWebServer().getPort();
        admin = context.getBean(StubAdminClient.class);
    }

    @AfterAll
    static void stopAdmin() {
        if (context != null) {
            context.close();
        }
        HTTP.close();
    }

    @BeforeEach
    void resetResponses() {
        admin.success = false;
        admin.mutationCalls.set(0);
    }

    @Test
    void failedCoordinatorMutationsReturnServerErrorsWithoutAppliedRecords() throws Exception {
        for (Mutation mutation : MUTATIONS) {
            HttpResponse<String> response = post(mutation.path(), mutation.body());
            assertSafeError(response, 500, "INTERNAL_ERROR");
            JsonNode body = JSON.readTree(response.body());
            assertTrue(body.get("message").asText().startsWith("Coordinator failed to "));
            for (String field : List.of("success", "node_id", "status", "shard_id", "leader", "replicas")) {
                assertFalse(body.hasNonNull(field), mutation.path() + " must not return an applied record");
            }
            assertFalse(response.body().contains(UNSAFE_MESSAGE));
        }
        assertEquals(5, admin.mutationCalls.get());
    }

    @Test
    void invalidShardInitValuesReturnSafeClientErrorsBeforeCallingCoordinator() throws Exception {
        for (String field : List.of("num_shards", "replication_factor")) {
            for (String value : List.of("null", "1.5", "2.0", "\"2\"", "true", "[]", "{}", "2147483648", "0", "-1")) {
                HttpResponse<String> response = post("/admin/config/shard-init", "{\"" + field + "\":" + value + "}");
                assertSafeError(response, 400, "INVALID_ARGUMENT");
                assertEquals(
                        field + " must be a positive 32-bit integer",
                        JSON.readTree(response.body()).get("message").asText());
            }
        }
        assertEquals(0, admin.mutationCalls.get());
    }

    @Test
    void successfulMutationsPreserveResponsesAndShardInitDefaults() throws Exception {
        admin.success = true;
        for (Mutation mutation : MUTATIONS) {
            HttpResponse<String> response = post(mutation.path(), mutation.body());
            assertEquals(200, response.statusCode(), mutation.path() + ": " + response.body());
            JsonNode body = JSON.readTree(response.body());
            switch (mutation.path()) {
                case "/admin/config/shard-init" -> {
                    assertTrue(body.get("success").asBoolean());
                    assertEquals(4, body.get("num_shards").asInt());
                    assertEquals(2, body.get("replication_factor").asInt());
                    assertEquals(4, admin.numShards);
                    assertEquals(2, admin.replicationFactor);
                }
                case "/admin/nodes", "/admin/nodes/node-1/status" -> {
                    assertEquals("node-1", body.get("node_id").asText());
                    assertEquals("ALIVE", body.get("status").asText());
                }
                default -> {
                    assertEquals("shard-0", body.get("shard_id").asText());
                    assertEquals("node-new", body.get("leader").asText());
                    assertEquals("node-new", body.get("replicas").get(0).asText());
                }
            }
        }
        HttpResponse<String> defaults = post("/admin/config/shard-init", "{}");
        assertEquals(200, defaults.statusCode());
        assertEquals(8, JSON.readTree(defaults.body()).get("num_shards").asInt());
        assertEquals(2, JSON.readTree(defaults.body()).get("replication_factor").asInt());
        assertEquals(8, admin.numShards);
        assertEquals(2, admin.replicationFactor);
        assertEquals(6, admin.mutationCalls.get());
    }

    private static void assertSafeError(HttpResponse<String> response, int status, String error) throws Exception {
        assertEquals(status, response.statusCode(), response.body());
        JsonNode body = JSON.readTree(response.body());
        assertEquals(error, body.get("error").asText());
        assertEquals(String.valueOf(status), body.get("code").asText());
        assertFalse(response.body().contains("java."));
        assertFalse(response.body().contains("Exception"));
        assertFalse(response.body().contains("com.danieljhkim"));
    }

    private static HttpResponse<String> post(String path, String body) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json")
                .header("X-Admin-Api-Key", API_KEY)
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private record Mutation(String path, String body) {}

    @Configuration(proxyBeanMethods = false)
    @Profile("admin-mutation-outcome-test")
    static class StubConfiguration {
        @Bean
        @Primary
        StubAdminClient stubAdminClient() {
            return new StubAdminClient();
        }

        @Bean
        @Primary
        CoordinatorReadClient stubReadClient(StubAdminClient admin) {
            return new CoordinatorReadClient(List.of(), 1, TimeUnit.SECONDS) {
                @Override
                public NodeDto getNode(String nodeId) {
                    return NodeDto.builder()
                            .nodeId(nodeId)
                            .address(admin.success ? "localhost:9002" : "localhost:9001")
                            .status(admin.success ? "ALIVE" : "DEAD")
                            .build();
                }

                @Override
                public ShardMapSnapshotDto getShardMap() {
                    String leader = admin.success ? "node-new" : "node-old";
                    ShardDto shard = ShardDto.builder()
                            .shardId("shard-0")
                            .epoch(7)
                            .leader(leader)
                            .replicas(List.of(leader))
                            .build();
                    return ShardMapSnapshotDto.builder()
                            .shards(Map.of("shard-0", shard))
                            .build();
                }
            };
        }
    }

    static final class StubAdminClient extends CoordinatorAdminClient {
        private volatile boolean success;
        private volatile int numShards;
        private volatile int replicationFactor;
        private final AtomicInteger mutationCalls = new AtomicInteger();

        StubAdminClient() {
            super(List.of(), 1, TimeUnit.SECONDS);
        }

        @Override
        public InitShardsResponse initShards(int numShards, int replicationFactor) {
            mutationCalls.incrementAndGet();
            this.numShards = numShards;
            this.replicationFactor = replicationFactor;
            return InitShardsResponse.newBuilder()
                    .setSuccess(success)
                    .setMessage(UNSAFE_MESSAGE)
                    .build();
        }

        @Override
        public RegisterNodeResponse registerNode(String nodeId, String address, String zone) {
            mutationCalls.incrementAndGet();
            return RegisterNodeResponse.newBuilder()
                    .setSuccess(success)
                    .setMessage(UNSAFE_MESSAGE)
                    .build();
        }

        @Override
        public SetNodeStatusResponse setNodeStatus(String nodeId, String status) {
            mutationCalls.incrementAndGet();
            return SetNodeStatusResponse.newBuilder()
                    .setSuccess(success)
                    .setMessage(UNSAFE_MESSAGE)
                    .build();
        }

        @Override
        public SetShardReplicasResponse setShardReplicas(String shardId, List<String> replicaNodeIds) {
            mutationCalls.incrementAndGet();
            return SetShardReplicasResponse.newBuilder()
                    .setSuccess(success)
                    .setMessage(UNSAFE_MESSAGE)
                    .build();
        }

        @Override
        public SetShardLeaderResponse setShardLeader(String shardId, long epoch, String leaderNodeId) {
            mutationCalls.incrementAndGet();
            assertEquals(7, epoch);
            return SetShardLeaderResponse.newBuilder()
                    .setSuccess(success)
                    .setMessage(UNSAFE_MESSAGE)
                    .build();
        }
    }
}
