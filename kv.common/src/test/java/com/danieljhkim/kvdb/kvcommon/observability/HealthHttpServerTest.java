package com.danieljhkim.kvdb.kvcommon.observability;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class HealthHttpServerTest {

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

    private static int status(HttpClient client, URI uri) throws Exception {
        return client.send(HttpRequest.newBuilder(uri).GET().build(), HttpResponse.BodyHandlers.discarding())
                .statusCode();
    }
}
