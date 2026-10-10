package com.danieljhkim.kvdb.kvadmin.security;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

public final class AdminApiKeyFilter extends org.springframework.web.filter.OncePerRequestFilter {

    private static final String HEADER = "X-Admin-Api-Key";
    private final byte[] expectedDigest;

    public AdminApiKeyFilter(String expected) {
        this.expectedDigest = digest(Objects.requireNonNull(expected, "apiKey"));
    }

    @Override
    protected boolean shouldNotFilter(jakarta.servlet.http.HttpServletRequest request) {
        String path = request.getRequestURI();
        return path != null && (path.startsWith("/admin/actuator/health") || path.startsWith("/admin/actuator/info"));
    }

    @Override
    protected void doFilterInternal(
            jakarta.servlet.http.HttpServletRequest request,
            jakarta.servlet.http.HttpServletResponse response,
            jakarta.servlet.FilterChain filterChain)
            throws jakarta.servlet.ServletException, IOException {

        String provided = request.getHeader(HEADER);
        if (provided == null || provided.isBlank() || !MessageDigest.isEqual(digest(provided), expectedDigest)) {
            JsonError.write(
                    response,
                    jakarta.servlet.http.HttpServletResponse.SC_UNAUTHORIZED,
                    "invalid_api_key",
                    request.getRequestURI());
            return;
        }
        filterChain.doFilter(request, response);
    }

    private static byte[] digest(String value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }
}
