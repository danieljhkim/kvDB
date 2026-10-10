package com.danieljhkim.kvdb.kvcommon.observability;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.danieljhkim.kvdb.kvcommon.grpc.GrpcIdentity;
import com.danieljhkim.kvdb.kvcommon.grpc.GrpcSecurityConfig;
import com.danieljhkim.kvdb.proto.coordinator.CoordinatorGrpc;
import com.danieljhkim.kvdb.proto.coordinator.ShardMapDelta;
import com.danieljhkim.kvdb.proto.coordinator.WatchShardMapRequest;
import com.kvdb.proto.kvstore.KVServiceGrpc;
import com.kvdb.proto.kvstore.PingRequest;
import com.kvdb.proto.kvstore.PingResponse;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerMethodDefinition;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

class HealthHttpServerTest {

    @Test
    void defaultHealthListenerBindsLoopback() throws Exception {
        try (HealthHttpServer server = new HealthHttpServer(0, new ServiceLifecycle(), () -> true)) {
            server.start();
            assertEquals("127.0.0.1", server.getAddress().getAddress().getHostAddress());
            assertEquals(
                    200,
                    status(
                            HttpClient.newHttpClient(),
                            URI.create("http://127.0.0.1:" + server.getPort() + "/health/live")));
        }
    }

    @Test
    void healthListenerUsesSecurityBindAndAllowsExplicitWildcard() throws Exception {
        GrpcSecurityConfig config = GrpcSecurityConfig.development(GrpcIdentity.Role.GATEWAY, "gateway-test");
        try (HealthHttpServer server =
                new HealthHttpServer(config.serverAddress(0), new ServiceLifecycle(), () -> true)) {
            server.start();
            assertEquals(config.bindAddress(), server.getAddress().getAddress());
        }
        try (HealthHttpServer server =
                new HealthHttpServer(new InetSocketAddress("0.0.0.0", 0), new ServiceLifecycle(), () -> true)) {
            server.start();
            assertTrue(server.getAddress().getAddress().isAnyLocalAddress());
            assertEquals(
                    200,
                    status(
                            HttpClient.newHttpClient(),
                            URI.create("http://127.0.0.1:" + server.getPort() + "/health/ready")));
        }
    }

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
        completeRpc("metrics-test", lifecycle, Status.OK);
        completeRpc("metrics-test", lifecycle, Status.INVALID_ARGUMENT);

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
    void cancelledUnaryRpcRecordsOutcomeAndAllowsImmediateDrain() throws Exception {
        String service = "cancelled-unary-test";
        ServiceLifecycle lifecycle = new ServiceLifecycle();
        CapturedRpc<PingRequest, PingResponse> rpc = startRpc(service, lifecycle, KVServiceGrpc.getPingMethod());

        rpc.listener().onCancel();

        assertEquals(0, lifecycle.inFlight());
        lifecycle.beginDrain();
        assertTrue(lifecycle.awaitDrain(Duration.ZERO));
        assertEquals(1, rpcOutcomeCount(service, "Ping", "cancelled"));
        assertEquals(1, rpcDurationCount(service, "Ping"));
    }

    @Test
    void cancelledStreamingRpcRecordsOutcomeAndAllowsImmediateDrain() throws Exception {
        String service = "cancelled-streaming-test";
        ServiceLifecycle lifecycle = new ServiceLifecycle();
        CapturedRpc<WatchShardMapRequest, ShardMapDelta> rpc =
                startRpc(service, lifecycle, CoordinatorGrpc.getWatchShardMapMethod());

        rpc.listener().onCancel();

        assertEquals(0, lifecycle.inFlight());
        lifecycle.beginDrain();
        assertTrue(lifecycle.awaitDrain(Duration.ZERO));
        assertEquals(1, rpcOutcomeCount(service, "WatchShardMap", "cancelled"));
        assertEquals(1, rpcDurationCount(service, "WatchShardMap"));
    }

    @Test
    void cancellationRacingNormalCloseCompletesExactlyOnce() throws Exception {
        String service = "cancel-close-race-test";
        ServiceLifecycle lifecycle = new ServiceLifecycle();
        CapturedRpc<PingRequest, PingResponse> rpc = startRpc(service, lifecycle, KVServiceGrpc.getPingMethod());
        CyclicBarrier start = new CyclicBarrier(3);

        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            Future<?> cancellation = executor.submit(() -> {
                start.await();
                rpc.listener().onCancel();
                return null;
            });
            Future<?> close = executor.submit(() -> {
                start.await();
                rpc.call().close(Status.OK, new Metadata());
                return null;
            });
            start.await();
            cancellation.get();
            close.get();
        }

        assertEquals(0, lifecycle.inFlight());
        assertEquals(1, rpcOutcomeCount(service, "Ping", "cancelled") + rpcOutcomeCount(service, "Ping", "ok"));
        assertEquals(1, rpcDurationCount(service, "Ping"));
    }

    @Test
    void rejectedCallLeavesInFlightAtZero() throws Exception {
        ServiceLifecycle lifecycle = new ServiceLifecycle();
        lifecycle.beginDrain();

        CapturedRpc<PingRequest, PingResponse> rpc =
                startRpc("rejected-test", lifecycle, KVServiceGrpc.getPingMethod());

        assertNull(rpc.call());
        assertEquals(Status.Code.UNAVAILABLE, rpc.transport().closedStatus.getCode());
        assertEquals(0, lifecycle.inFlight());
        assertTrue(lifecycle.awaitDrain(Duration.ZERO));
        assertEquals(1, rpcOutcomeCount("rejected-test", "Ping", "unavailable"));
    }

    @Test
    void unadmittedRaftCompletionDoesNotReleaseAdmittedCall() throws Exception {
        ServiceLifecycle lifecycle = new ServiceLifecycle();
        CapturedRpc<PingRequest, PingResponse> admitted =
                startRpc("admitted-app-test", lifecycle, KVServiceGrpc.getPingMethod());
        assertEquals(1, lifecycle.inFlight());

        for (int index = 0; index < 3; index++) {
            CapturedRpc<PingRequest, PingResponse> raft =
                    startRpc(KVServiceGrpc.getPingMethod(), new RequestMetricsInterceptor("raft-test"));
            raft.call().close(Status.OK, new Metadata());
        }

        assertEquals(1, lifecycle.inFlight());
        lifecycle.beginDrain();
        assertFalse(lifecycle.awaitDrain(Duration.ofMillis(1)));
        admitted.call().close(Status.OK, new Metadata());
        assertEquals(0, lifecycle.inFlight());
        assertTrue(lifecycle.awaitDrain(Duration.ofMillis(1)));
        assertEquals(3, rpcOutcomeCount("raft-test", "Ping", "ok"));
    }

    @Test
    void admittedCallReleasesExactlyOnceAcrossCloseAndCancel() {
        ServiceLifecycle lifecycle = new ServiceLifecycle();
        CapturedRpc<PingRequest, PingResponse> first =
                startRpc("release-once-test", lifecycle, KVServiceGrpc.getPingMethod());
        CapturedRpc<PingRequest, PingResponse> second =
                startRpc("release-once-test", lifecycle, KVServiceGrpc.getPingMethod());
        assertEquals(2, lifecycle.inFlight());

        first.call().close(Status.OK, new Metadata());
        first.listener().onCancel();
        first.call().close(Status.OK, new Metadata());

        assertEquals(1, lifecycle.inFlight());
        second.listener().onCancel();
        assertEquals(0, lifecycle.inFlight());
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

    private static void completeRpc(String service, ServiceLifecycle lifecycle, Status status) {
        CapturedRpc<PingRequest, PingResponse> rpc = startRpc(service, lifecycle, KVServiceGrpc.getPingMethod());
        rpc.call().close(status, new Metadata());
    }

    /** Starts a call through the production chain: admission and metrics listed in {@code GatewayServer} order. */
    private static <ReqT, RespT> CapturedRpc<ReqT, RespT> startRpc(
            String service, ServiceLifecycle lifecycle, MethodDescriptor<ReqT, RespT> method) {
        return startRpc(method, new AdmissionControlInterceptor(lifecycle), new RequestMetricsInterceptor(service));
    }

    private static <ReqT, RespT> CapturedRpc<ReqT, RespT> startRpc(
            MethodDescriptor<ReqT, RespT> method, ServerInterceptor... interceptors) {
        AtomicReference<ServerCall<ReqT, RespT>> handledCall = new AtomicReference<>();
        ServerServiceDefinition definition = ServerServiceDefinition.builder(method.getServiceName())
                .addMethod(method, (ServerCallHandler<ReqT, RespT>) (call, headers) -> {
                    handledCall.set(call);
                    return new ServerCall.Listener<>() {};
                })
                .build();
        @SuppressWarnings("unchecked")
        ServerMethodDefinition<ReqT, RespT> intercepted = (ServerMethodDefinition<ReqT, RespT>)
                ServerInterceptors.intercept(definition, interceptors).getMethod(method.getFullMethodName());
        RecordingCall<ReqT, RespT> recording = new RecordingCall<>(method);
        ServerCall.Listener<ReqT> listener = intercepted.getServerCallHandler().startCall(recording, new Metadata());
        return new CapturedRpc<>(handledCall.get(), listener, recording);
    }

    private static long rpcOutcomeCount(String service, String method, String outcome) {
        String prefix = "kvdb_rpc_requests_total{service=\"" + service + "\",method=\"" + method + "\",outcome=\""
                + outcome + "\"} ";
        return metricValue(prefix);
    }

    private static long rpcDurationCount(String service, String method) {
        return metricValue("kvdb_rpc_duration_seconds_count{service=\"" + service + "\",method=\"" + method + "\"} ");
    }

    private static long metricValue(String prefix) {
        return Metrics.scrape()
                .lines()
                .filter(line -> line.startsWith(prefix))
                .mapToLong(line -> Long.parseLong(line.substring(prefix.length())))
                .findFirst()
                .orElse(0);
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
        private Status closedStatus;

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
        public void close(Status status, Metadata trailers) {
            closedStatus = status;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public MethodDescriptor<ReqT, RespT> getMethodDescriptor() {
            return method;
        }
    }

    private record CapturedRpc<ReqT, RespT>(
            ServerCall<ReqT, RespT> call, ServerCall.Listener<ReqT> listener, RecordingCall<ReqT, RespT> transport) {}
}
