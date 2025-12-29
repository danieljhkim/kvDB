package com.danieljhkim.kvdb.kvadmin.security;

import java.io.IOException;
import java.util.Objects;

public final class AdminApiKeyFilter extends org.springframework.web.filter.OncePerRequestFilter {

	private static final String HEADER = "X-Admin-Api-Key";
	private final String expected;

	public AdminApiKeyFilter(String expected) {
		this.expected = Objects.requireNonNull(expected, "apiKey");
	}

	@Override
	protected boolean shouldNotFilter(jakarta.servlet.http.HttpServletRequest request) {
		String path = request.getRequestURI();
		return path != null
				&& (path.startsWith("/admin/actuator/health")
						|| path.startsWith("/admin/actuator/info"));
	}

	@Override
	protected void doFilterInternal(
			jakarta.servlet.http.HttpServletRequest request,
			jakarta.servlet.http.HttpServletResponse response,
			jakarta.servlet.FilterChain filterChain) throws jakarta.servlet.ServletException, IOException {

		String provided = request.getHeader(HEADER);
		if (provided == null || provided.isBlank() || !provided.equals(expected)) {
			JsonError.write(response, jakarta.servlet.http.HttpServletResponse.SC_UNAUTHORIZED, "invalid_api_key",
					request.getRequestURI());
			return;
		}
		filterChain.doFilter(request, response);
	}
}
