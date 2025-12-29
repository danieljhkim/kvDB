package com.danieljhkim.kvdb.kvadmin.security;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public final class AdminIpAllowlistFilter extends org.springframework.web.filter.OncePerRequestFilter {

	private final List<Cidr> allow;

	public AdminIpAllowlistFilter(List<String> allowedIps) {
		if (allowedIps == null || allowedIps.isEmpty()) {
			throw new IllegalArgumentException(
					"kvdb.admin.security.allowed-ips must not be empty when security is enabled");
		}
		List<Cidr> parsed = new ArrayList<>();
		for (String entry : allowedIps) {
			if (entry == null || entry.isBlank()) {
				continue;
			}
			parsed.add(Cidr.parse(entry.trim()));
		}
		if (parsed.isEmpty()) {
			throw new IllegalArgumentException("kvdb.admin.security.allowed-ips must contain at least one valid entry");
		}
		this.allow = List.copyOf(parsed);
	}

	@Override
	protected void doFilterInternal(
			jakarta.servlet.http.HttpServletRequest request,
			jakarta.servlet.http.HttpServletResponse response,
			jakarta.servlet.FilterChain filterChain) throws jakarta.servlet.ServletException, IOException {

		String clientIp = IpUtil.clientIp(request);
		if (clientIp == null) {
			JsonError.write(response, jakarta.servlet.http.HttpServletResponse.SC_FORBIDDEN, "ip_not_allowed",
					request.getRequestURI());
			return;
		}

		InetAddress addr;
		try {
			addr = InetAddress.getByName(clientIp);
		} catch (UnknownHostException e) {
			JsonError.write(response, jakarta.servlet.http.HttpServletResponse.SC_FORBIDDEN, "ip_not_allowed",
					request.getRequestURI());
			return;
		}

		for (Cidr cidr : allow) {
			if (cidr.contains(addr)) {
				filterChain.doFilter(request, response);
				return;
			}
		}
		JsonError.write(response, jakarta.servlet.http.HttpServletResponse.SC_FORBIDDEN, "ip_not_allowed",
				request.getRequestURI());
	}
}
