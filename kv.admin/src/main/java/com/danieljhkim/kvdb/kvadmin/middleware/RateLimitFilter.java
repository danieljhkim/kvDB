package com.danieljhkim.kvdb.kvadmin.middleware;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Optional rate limiting filter.
 *
 * <p>
 * Enable with: kvdb.admin.security.rate-limit.enabled=true
 */
@Component
@ConditionalOnProperty(name = "kvdb.admin.security.rate-limit.enabled", havingValue = "true")
public class RateLimitFilter extends OncePerRequestFilter {

    private static final int DEFAULT_RATE_LIMIT = 100; // requests per minute
    private static final long WINDOW_MILLIS = 60000;
    private final ConcurrentHashMap<String, RateLimitEntry> rateLimitMap = new ConcurrentHashMap<>();
    private final int rateLimit;
    private final LongSupplier clock;
    private final AtomicLong lastCleanup;

    public RateLimitFilter() {
        this(() -> TimeUnit.NANOSECONDS.toMillis(System.nanoTime()));
    }

    RateLimitFilter(LongSupplier clock) {
        this.rateLimit = DEFAULT_RATE_LIMIT;
        this.clock = Objects.requireNonNull(clock);
        this.lastCleanup = new AtomicLong(clock.getAsLong());
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {

        String clientId = getClientId(request);
        AtomicBoolean allowed = new AtomicBoolean();
        // Sample time and update the whole window under the same per-client lock.
        rateLimitMap.compute(clientId, (key, entry) -> {
            long now = clock.getAsLong();
            if (entry == null || now - entry.windowStart >= WINDOW_MILLIS) {
                entry = new RateLimitEntry(now);
            }
            entry.lastSeen = now;
            if (entry.count < rateLimit) {
                entry.count++;
                allowed.set(true);
            }
            return entry;
        });
        removeIdleClients();

        if (!allowed.get()) {
            response.setStatus(429); // Too Many Requests
            response.getWriter().write("Rate limit exceeded");
            return;
        }

        filterChain.doFilter(request, response);
    }

    private void removeIdleClients() {
        long now = clock.getAsLong();
        long previous = lastCleanup.get();
        if (now - previous < WINDOW_MILLIS || !lastCleanup.compareAndSet(previous, now)) {
            return;
        }
        // Recheck idleness inside computeIfPresent: a concurrent request may
        // have refreshed the entry since the sweep started iterating keys.
        for (String clientId : rateLimitMap.keySet()) {
            rateLimitMap.computeIfPresent(
                    clientId, (key, entry) -> now - entry.lastSeen >= WINDOW_MILLIS ? null : entry);
        }
    }

    private String getClientId(HttpServletRequest request) {
        // Use IP address as client identifier
        return request.getRemoteAddr();
    }

    private static class RateLimitEntry {
        // Accessed only by the map's atomic operations for this client.
        int count;
        final long windowStart;
        long lastSeen;

        RateLimitEntry(long now) {
            windowStart = now;
            lastSeen = now;
        }
    }
}
