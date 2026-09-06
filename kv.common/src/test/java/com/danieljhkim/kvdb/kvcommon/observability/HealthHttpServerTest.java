package com.danieljhkim.kvdb.kvcommon.observability;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.kvdb.proto.kvstore.KVServiceGrpc;
import com.kvdb.proto.kvstore.PingRequest;
import com.kvdb.proto.kvstore.PingResponse;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

class HealthHttpServerTest {

    private static final Pattern PROMETHEUS_SAMPLE =
            Pattern.compile("[a-zA-Z_:][a-zA-Z0-9_:]*(?:\\{[a-zA-Z_][a-zA-Z0-9_]*=\"(?:[^\"\\\\]|\\\\.)*\""
                    + "(?:,[a-zA-Z_][a-zA-Z0-9_]*=\"(?:[^\"\\\\]|\\\\.)*\")*\\})?"
                    + "\\s+[-+]?(?:[0-9]+(?:\\.[0-9]*)?|\\.[0-9]+)(?:[eE][-+]?[0-9]+)?(?:\\s+[0-9]+)?");

    @Test
    void readinessReflectsDependencyFailureAndDrain() throws Exception {
        ServiceLifecycle lifecycle = new ServiceLifecycle();
        AtomicBoolean dependencyReady = new AtomicBoolean(false);
        try (HealthHttpServer server = new HealthHttpServer(0, lifecycle, dependencyReady::get)) {
            server.start();
            HttpClient client = HttpClient.newHttpClient();
            URI readyUri = URI.create("http://localhost:" + server.getPort() + "/health/ready");

            assertEquals(503, status(client, readyUri));
            dependencyReady.set(true);
            assertEquals(200, status(client, readyUri));

            lifecycle.beginDrain();
            assertEquals(503, status(client, readyUri));
        }
    }

    @Test
    void drainWaitsForAdmittedRequests() throws Exception {
        ServiceLifecycle lifecycle = new ServiceLifecycle();
        assertTrue(lifecycle.tryAdmit());
        lifecycle.beginDrain();
        assertFalse(lifecycle.tryAdmit());
        assertFalse(lifecycle.awaitDrain(Duration.ofMillis(1)));
        lifecycle.complete();
        assertTrue(lifecycle.awaitDrain(Duration.ofMillis(1)));
    }

    @Test
    void metricsEndpointUsesValidLatencyNamesAfterCompletedRpcOutcomes() throws Exception {
        ServiceLifecycle lifecycle = new ServiceLifecycle();
        RequestMetricsInterceptor interceptor = new RequestMetricsInterceptor("metrics-test", lifecycle);
        completeRpc(interceptor, lifecycle, Status.OK);
        completeRpc(interceptor, lifecycle, Status.INVALID_ARGUMENT);

        try (HealthHttpServer server = new HealthHttpServer(0, lifecycle, () -> true)) {
            server.start();
            HttpResponse<String> response = HttpClient.newHttpClient()
                    .send(
                            HttpRequest.newBuilder(metricsUri(server)).GET().build(),
                            HttpResponse.BodyHandlers.ofString());

            assertEquals(200, response.statusCode());
            String metrics = response.body();
            assertPrometheusTextFormat(metrics);
            assertTrue(metrics.contains("kvdb_rpc_duration_seconds_sum{service=\"metrics-test\",method=\"Ping\"}"));
            assertTrue(metrics.contains("kvdb_rpc_duration_seconds_count{service=\"metrics-test\",method=\"Ping\"} 2"));
            assertTrue(metrics.contains(
                    "kvdb_rpc_requests_total{service=\"metrics-test\",method=\"Ping\",outcome=\"ok\"}"));
            assertTrue(metrics.contains(
                    "kvdb_rpc_requests_total{service=\"metrics-test\",method=\"Ping\",outcome=\"invalid_argument\"}"));
            assertFalse(metrics.contains("}_sum"));
            assertFalse(metrics.contains("}_count"));
            assertFalse(metrics.contains("payload"));
        }
    }

    @Test
    void concurrentObservationsAndScrapesRemainValidPrometheusText() throws Exception {
        try (ExecutorService executor = Executors.newFixedThreadPool(4)) {
            List<Callable<Void>> operations = new ArrayList<>();
            for (int index = 0; index < 2; index++) {
                operations.add(() -> {
                    for (int observation = 0; observation < 100; observation++) {
                        Metrics.observe("kvdb_rpc_duration_seconds", "concurrent-test", "Get", 0.01);
                    }
                    return null;
                });
                operations.add(() -> {
                    for (int scrape = 0; scrape < 100; scrape++) {
                        assertPrometheusTextFormat(Metrics.scrape());
                    }
                    return null;
                });
            }
            for (Future<Void> operation : executor.invokeAll(operations)) {
                operation.get();
            }
        }
    }

    private static int status(HttpClient client, URI uri) throws Exception {
        return client.send(HttpRequest.newBuilder(uri).GET().build(), HttpResponse.BodyHandlers.discarding())
                .statusCode();
    }

    private static URI metricsUri(HealthHttpServer server) {
        return URI.create("http://localhost:" + server.getPort() + "/metrics");
    }

    private static void completeRpc(RequestMetricsInterceptor interceptor, ServiceLifecycle lifecycle, Status status) {
        assertTrue(lifecycle.tryAdmit());
        RecordingCall<PingRequest, PingResponse> call = new RecordingCall<>(KVServiceGrpc.getPingMethod());
        AtomicReference<ServerCall<PingRequest, PingResponse>> measuredCall = new AtomicReference<>();
        interceptor.interceptCall(call, new Metadata(), capture(measuredCall));
        measuredCall.get().close(status, new Metadata());
    }

    private static ServerCallHandler<PingRequest, PingResponse> capture(
            AtomicReference<ServerCall<PingRequest, PingResponse>> measuredCall) {
        return (call, headers) -> {
            measuredCall.set(call);
            return new ServerCall.Listener<>() {};
        };
    }

    private static void assertPrometheusTextFormat(String metrics) {
        for (String sample : metrics.split("\\n")) {
            if (!sample.isBlank()) {
                assertTrue(PROMETHEUS_SAMPLE.matcher(sample).matches(), () -> "Invalid sample: " + sample);
            }
        }
    }

    private static final class RecordingCall<ReqT, RespT> extends ServerCall<ReqT, RespT> {
        private final MethodDescriptor<ReqT, RespT> method;

        private RecordingCall(MethodDescriptor<ReqT, RespT> method) {
            this.method = method;
        }

        @Override
        public void request(int numMessages) {}

        @Override
        public void sendHeaders(Metadata headers) {}

        @Override
        public void sendMessage(RespT message) {}

        @Override
        public void close(Status status, Metadata trailers) {}

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public MethodDescriptor<ReqT, RespT> getMethodDescriptor() {
            return method;
        }
    }
}
