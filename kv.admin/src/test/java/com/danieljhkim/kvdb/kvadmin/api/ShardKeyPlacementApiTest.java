package com.danieljhkim.kvdb.kvadmin.api;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.danieljhkim.kvdb.kvadmin.AdminApplication;
import com.danieljhkim.kvdb.kvadmin.api.dto.ShardDto;
import com.danieljhkim.kvdb.kvadmin.client.CoordinatorReadClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.Status;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
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

class ShardKeyPlacementApiTest {

    private static final String API_KEY = "test-api-key";
    private static final ShardDto PLACEMENT = ShardDto.builder()
            .shardId("shard-7")
            .epoch(42)
            .replicas(List.of("node-a", "node-b"))
            .leader("node-a")
            .configState("STABLE")
            .build();

    @Test
    void httpResolveKeyReturnsCoordinatorPlacementAndDocumentedErrors() throws Exception {
        try (ConfigurableApplicationContext context = startAdmin()) {
            RecordingCoordinatorReadClient coordinator = context.getBean(RecordingCoordinatorReadClient.class);
            int port = ((ServletWebServerApplicationContext) context)
                    .getWebServer()
                    .getPort();
            HttpClient http = HttpClient.newHttpClient();
            ObjectMapper mapper = context.getBean(ObjectMapper.class);

            byte[] binaryKey = new byte[] {0x00, (byte) 0xFF, (byte) 0xFE, 'k'};
            HttpResponse<String> ok = post(http, port, API_KEY, jsonKey(binaryKey));
            assertEquals(200, ok.statusCode());
            JsonNode body = mapper.readTree(ok.body());
            assertEquals("shard-7", body.get("shard_id").asText());
            assertEquals(42, body.get("epoch").asLong());
            assertEquals("node-a", body.get("leader").asText());
            assertEquals("node-a", body.get("replicas").get(0).asText());
            assertEquals("node-b", body.get("replicas").get(1).asText());
            assertEquals("STABLE", body.get("config_state").asText());
            assertFalse(ok.body().contains(Base64.getEncoder().encodeToString(binaryKey)));
            assertFalse(ok.body().contains("key_base64"));
            assertArrayEquals(binaryKey, coordinator.lastKey());

            HttpResponse<String> malformed = post(http, port, API_KEY, "{\"key_base64\":\"%%%\"}");
            assertEquals(400, malformed.statusCode());
            assertEquals(
                    "INVALID_ARGUMENT",
                    mapper.readTree(malformed.body()).get("error").asText());
            assertFalse(malformed.body().contains("%%%"));

            HttpResponse<String> empty = post(http, port, API_KEY, jsonKey(new byte[0]));
            assertEquals(400, empty.statusCode());
            assertEquals(
                    "InvalidRequestException",
                    mapper.readTree(empty.body()).get("error").asText());

            HttpResponse<String> oversizedEncoded =
                    post(http, port, API_KEY, "{\"key_base64\":\"" + "A".repeat(25) + "\"}");
            assertEquals(429, oversizedEncoded.statusCode());
            assertEquals(
                    "PayloadTooLargeException",
                    mapper.readTree(oversizedEncoded.body()).get("error").asText());

            HttpResponse<String> oversizedDecoded = post(http, port, API_KEY, jsonKey(new byte[17]));
            assertEquals(429, oversizedDecoded.statusCode());
            assertEquals(
                    "PayloadTooLargeException",
                    mapper.readTree(oversizedDecoded.body()).get("error").asText());

            HttpResponse<String> unauthenticated = post(http, port, null, jsonKey(binaryKey));
            assertEquals(401, unauthenticated.statusCode());
            assertEquals(
                    "invalid_api_key",
                    mapper.readTree(unauthenticated.body()).get("error").asText());

            coordinator.setStatus(Status.UNAVAILABLE.withDescription("coordinator down"));
            HttpResponse<String> unavailable = post(http, port, API_KEY, jsonKey(binaryKey));
            assertEquals(503, unavailable.statusCode());
            assertEquals(
                    "GRPC_ERROR",
                    mapper.readTree(unavailable.body()).get("error").asText());

            coordinator.setStatus(Status.DEADLINE_EXCEEDED.withDescription("deadline"));
            HttpResponse<String> timeout = post(http, port, API_KEY, jsonKey(binaryKey));
            assertEquals(504, timeout.statusCode());
            assertEquals(
                    "GRPC_ERROR", mapper.readTree(timeout.body()).get("error").asText());
        }
    }

    @Test
    void httpResolveKeyEnforcesIpAllowlist() throws Exception {
        try (ConfigurableApplicationContext context = startAdmin("--kvdb.admin.security.allowed-ips=10.0.0.0/32")) {
            int port = ((ServletWebServerApplicationContext) context)
                    .getWebServer()
                    .getPort();
            HttpResponse<String> forbidden =
                    post(HttpClient.newHttpClient(), port, API_KEY, jsonKey(new byte[] {0x01}));
            assertEquals(403, forbidden.statusCode());
            assertEquals(
                    "ip_not_allowed",
                    new ObjectMapper().readTree(forbidden.body()).get("error").asText());
        }
    }

    @Test
    void routingDiagnosticsDoNotHashOrAccessValues() throws IOException {
        String service =
                Files.readString(Path.of("src/main/java/com/danieljhkim/kvdb/kvadmin/service/ShardAdminService.java"));
        String client = Files.readString(
                Path.of("src/main/java/com/danieljhkim/kvdb/kvadmin/client/CoordinatorReadClient.java"));
        String controller =
                Files.readString(Path.of("src/main/java/com/danieljhkim/kvdb/kvadmin/api/ShardController.java"));
        String combined = service + client + controller;
        assertTrue(client.contains("stub.resolveShard"));
        assertFalse(service.contains("MessageDigest"));
        assertFalse(service.contains("Murmur"));
        assertFalse(service.contains("Hashing"));
        assertFalse(controller.contains("GetRequest"));
        assertFalse(controller.contains("SetRequest"));
        assertFalse(combined.contains("KVServiceGrpc"));
    }

    @Test
    void requestToStringOmitsKey() {
        String rendered = com.danieljhkim.kvdb.kvadmin.api.dto.ResolveKeyRequestDto.builder()
                .keyBase64("c2VjcmV0")
                .build()
                .toString();
        assertFalse(rendered.contains("c2VjcmV0"));
    }

    private static ConfigurableApplicationContext startAdmin(String... extra) {
        String[] args = new String[extra.length + 5];
        args[0] = "--server.port=0";
        args[1] = "--kvdb.admin.security.api-key=" + API_KEY;
        args[2] = "--kvdb.admin.max-key-bytes=16";
        args[3] = "--kvdb.coordinator.grpc.address=localhost:1";
        args[4] = "--spring.profiles.active=key-placement-api-test";
        System.arraycopy(extra, 0, args, 5, extra.length);
        return new SpringApplicationBuilder(AdminApplication.class, FakeCoordinatorConfiguration.class)
                .web(WebApplicationType.SERVLET)
                .run(args);
    }

    private static HttpResponse<String> post(HttpClient http, int port, String apiKey, String json)
            throws IOException, InterruptedException {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + port + "/admin/shards/resolve-key"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json, StandardCharsets.UTF_8));
        if (apiKey != null) {
            builder.header("X-Admin-Api-Key", apiKey);
        }
        return http.send(builder.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    }

    private static String jsonKey(byte[] key) {
        return "{\"key_base64\":\"" + Base64.getEncoder().encodeToString(key) + "\"}";
    }

    @Configuration(proxyBeanMethods = false)
    @Profile("key-placement-api-test")
    static class FakeCoordinatorConfiguration {
        @Bean
        @Primary
        RecordingCoordinatorReadClient recordingCoordinatorReadClient() {
            return new RecordingCoordinatorReadClient();
        }
    }

    static final class RecordingCoordinatorReadClient extends CoordinatorReadClient {
        private final AtomicReference<byte[]> lastKey = new AtomicReference<>();
        private final AtomicReference<Status> status = new AtomicReference<>(Status.OK);

        RecordingCoordinatorReadClient() {
            super(List.of("localhost:1"), 1, TimeUnit.MILLISECONDS);
        }

        @Override
        public ShardDto resolveShard(byte[] key) {
            lastKey.set(key == null ? null : Arrays.copyOf(key, key.length));
            Status current = status.get();
            if (!current.isOk()) {
                throw current.asRuntimeException();
            }
            return PLACEMENT;
        }

        byte[] lastKey() {
            return lastKey.get();
        }

        void setStatus(Status next) {
            status.set(next);
        }
    }
}
