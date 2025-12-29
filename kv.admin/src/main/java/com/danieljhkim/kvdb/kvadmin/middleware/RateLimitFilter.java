package com.danieljhkim.kvdb.kvadmin.middleware;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

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
	private final ConcurrentHashMap<String, RateLimitEntry> rateLimitMap = new ConcurrentHashMap<>();
	private final int rateLimit;

	public RateLimitFilter() {
		this.rateLimit = DEFAULT_RATE_LIMIT;
	}

	@Override
	protected void doFilterInternal(
			HttpServletRequest request,
			HttpServletResponse response,
			FilterChain filterChain) throws ServletException, IOException {

		String clientId = getClientId(request);
		RateLimitEntry entry = rateLimitMap.computeIfAbsent(clientId, k -> new RateLimitEntry());

		long now = System.currentTimeMillis();
		if (now - entry.windowStart > 60000) { // Reset window every minute
			entry.count.set(0);
			entry.windowStart = now;
		}

		if (entry.count.incrementAndGet() > rateLimit) {
			response.setStatus(429); // Too Many Requests
			response.getWriter().write("Rate limit exceeded");
			return;
		}

		filterChain.doFilter(request, response);
	}

	private String getClientId(HttpServletRequest request) {
		// Use IP address as client identifier
		return request.getRemoteAddr();
	}

	private static class RateLimitEntry {
		AtomicInteger count = new AtomicInteger(0);
		long windowStart = System.currentTimeMillis();
	}
}

