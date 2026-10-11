package com.danieljhkim.kvdb.kvadmin.middleware;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.servlet.DispatcherType;
import jakarta.servlet.FilterChain;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class RateLimitFilterTest {

    @Test
    void enforcesTheCapSeparatelyForEachClient() throws Exception {
        ControlledClock clock = new ControlledClock();
        RateLimitFilter filter = new RateLimitFilter(clock);
        admit(filter, "10.0.0.1", 100);

        FilterResult rejected = request(filter, "10.0.0.1");
        assertEquals(429, rejected.status());
        assertEquals("Rate limit exceeded", rejected.body());
        assertFalse(rejected.continued());
        assertTrue(request(filter, "10.0.0.2").continued());
    }

    @Test
    void resetsAtTheExactWindowBoundary() throws Exception {
        ControlledClock clock = new ControlledClock();
        RateLimitFilter filter = new RateLimitFilter(clock);
        admit(filter, "client", 100);

        clock.now.set(59999);
        assertEquals(429, request(filter, "client").status());
        clock.now.set(60000);
        admit(filter, "client", 100);
        assertEquals(429, request(filter, "client").status());
    }

    @Test
    void sweepsIdleClientsAndKeepsClientsWithRecentRejectedRequests() throws Exception {
        ControlledClock clock = new ControlledClock();
        RateLimitFilter filter = new RateLimitFilter(clock);
        admit(filter, "idle", 1);
        admit(filter, "active", 100);

        clock.now.set(59999);
        assertEquals(429, request(filter, "active").status());
        clock.now.set(60000);
        admit(filter, "new-client", 1);
        assertFalse(clients(filter).containsKey("idle"));
        assertTrue(clients(filter).containsKey("active"));
        assertEquals(2, clients(filter).size());

        clock.now.set(120000);
        admit(filter, "another-client", 1);
        assertEquals(1, clients(filter).size());
        assertTrue(clients(filter).containsKey("another-client"));
    }

    @Test
    @Timeout(15)
    void concurrentRolloverRequestsShareOneNewWindow() throws Exception {
        ControlledClock clock = new ControlledClock();
        RateLimitFilter filter = new RateLimitFilter(clock);
        admit(filter, "client", 100);
        clock.now.set(60000);

        FilterResult[] results = overlappingRequests(filter, clock);
        assertTrue(results[0].continued());
        assertTrue(results[1].continued());
        admit(filter, "client", 98);
        assertEquals(429, request(filter, "client").status());
    }

    @Test
    @Timeout(15)
    void concurrentRequestsCannotBothTakeTheLastSlot() throws Exception {
        ControlledClock clock = new ControlledClock();
        RateLimitFilter filter = new RateLimitFilter(clock);
        admit(filter, "client", 99);

        FilterResult[] results = overlappingRequests(filter, clock);
        assertTrue(results[0].continued());
        assertEquals(429, results[1].status());
        assertFalse(results[1].continued());
        assertEquals(429, request(filter, "client").status());
    }

    @Test
    @Timeout(15)
    void sweepCannotDiscardAClientBeingRefreshed() throws Exception {
        ControlledClock clock = new ControlledClock();
        RateLimitFilter filter = new RateLimitFilter(clock);
        admit(filter, "client", 100);
        clock.now.set(60000);
        Pause pause = new Pause();
        CountDownLatch sweepStarted = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<FilterResult> refresh = executor.submit(() -> {
                clock.pauseNextRead.set(pause);
                return request(filter, "client");
            });
            await(pause.entered);
            Future<FilterResult> sweep = executor.submit(() -> {
                // The second clock read is the sweep's timestamp. The refresh
                // is still paused inside the target client's atomic update.
                clock.signalRead.set(new ReadSignal(2, sweepStarted));
                return request(filter, "sweeper");
            });
            await(sweepStarted);
            pause.release.countDown();
            assertTrue(refresh.get(5, TimeUnit.SECONDS).continued());
            assertTrue(sweep.get(5, TimeUnit.SECONDS).continued());
            assertTrue(clients(filter).containsKey("client"));
            admit(filter, "client", 99);
            assertEquals(429, request(filter, "client").status());
        } finally {
            pause.release.countDown();
            shutdown(executor);
        }
    }

    private static FilterResult[] overlappingRequests(RateLimitFilter filter, ControlledClock clock) throws Exception {
        Pause pause = new Pause();
        CountDownLatch secondArrived = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<FilterResult> first = executor.submit(() -> {
                clock.pauseNextRead.set(pause);
                return request(filter, "client");
            });
            await(pause.entered);
            Future<FilterResult> second = executor.submit(() -> request(filter, "client", secondArrived::countDown));
            await(secondArrived);
            // Both requests are in the filter while the first is paused at
            // the window update. No timing sleeps or repeated races are needed.
            pause.release.countDown();
            return new FilterResult[] {first.get(5, TimeUnit.SECONDS), second.get(5, TimeUnit.SECONDS)};
        } finally {
            pause.release.countDown();
            shutdown(executor);
        }
    }

    private static void admit(RateLimitFilter filter, String client, int count) throws Exception {
        for (int i = 0; i < count; i++) {
            FilterResult result = request(filter, client);
            assertEquals(HttpServletResponse.SC_OK, result.status());
            assertTrue(result.continued());
        }
    }

    private static Map<?, ?> clients(RateLimitFilter filter) throws Exception {
        Field field = RateLimitFilter.class.getDeclaredField("rateLimitMap");
        field.setAccessible(true);
        return (Map<?, ?>) field.get(filter);
    }

    private static FilterResult request(RateLimitFilter filter, String client) throws Exception {
        return request(filter, client, () -> {});
    }

    private static FilterResult request(RateLimitFilter filter, String client, Runnable onClientLookup)
            throws Exception {
        AtomicInteger status = new AtomicInteger(HttpServletResponse.SC_OK);
        StringWriter body = new StringWriter();
        AtomicBoolean continued = new AtomicBoolean();
        HttpServletRequest request = (HttpServletRequest) Proxy.newProxyInstance(
                HttpServletRequest.class.getClassLoader(),
                new Class<?>[] {HttpServletRequest.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getRemoteAddr" -> {
                        onClientLookup.run();
                        yield client;
                    }
                    case "getRequestURI" -> "/admin/test";
                    case "getDispatcherType" -> DispatcherType.REQUEST;
                    case "getAttribute", "setAttribute", "removeAttribute" -> null;
                    default -> null;
                });
        HttpServletResponse response = (HttpServletResponse) Proxy.newProxyInstance(
                HttpServletResponse.class.getClassLoader(),
                new Class<?>[] {HttpServletResponse.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "setStatus" -> {
                        status.set((Integer) args[0]);
                        yield null;
                    }
                    case "getWriter" -> new PrintWriter(body);
                    default -> null;
                });
        FilterChain chain = (requestArg, responseArg) -> continued.set(true);
        filter.doFilter(request, response, chain);
        return new FilterResult(status.get(), body.toString(), continued.get());
    }

    private static void await(CountDownLatch latch) {
        try {
            assertTrue(latch.await(5, TimeUnit.SECONDS), "Timed out waiting for the deterministic interleaving");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private static void shutdown(ExecutorService executor) throws InterruptedException {
        executor.shutdownNow();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }

    private record FilterResult(int status, String body, boolean continued) {}

    private static final class Pause {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
    }

    private static final class ReadSignal {
        int remaining;
        final CountDownLatch reached;

        ReadSignal(int remaining, CountDownLatch reached) {
            this.remaining = remaining;
            this.reached = reached;
        }
    }

    private static final class ControlledClock implements LongSupplier {
        final AtomicLong now = new AtomicLong();
        final ThreadLocal<Pause> pauseNextRead = new ThreadLocal<>();
        final ThreadLocal<ReadSignal> signalRead = new ThreadLocal<>();

        @Override
        public long getAsLong() {
            Pause pause = pauseNextRead.get();
            if (pause != null) {
                pauseNextRead.remove();
                pause.entered.countDown();
                await(pause.release);
            }
            ReadSignal signal = signalRead.get();
            if (signal != null && --signal.remaining == 0) {
                signalRead.remove();
                signal.reached.countDown();
            }
            return now.get();
        }
    }
}
