package com.danieljhkim.kvdb.kvadmin.security;

final class IpUtil {
	private IpUtil() {
	}

	static String clientIp(jakarta.servlet.http.HttpServletRequest req) {
		// NOTE: If you deploy behind a proxy/LB, configure and trust X-Forwarded-For
		// appropriately.
		// For now, prefer direct remote address.
		String remote = req.getRemoteAddr();
		if (remote != null && !remote.isBlank()) {
			return remote;
		}
		String xff = req.getHeader("X-Forwarded-For");
		if (xff == null || xff.isBlank()) {
			return null;
		}
		int comma = xff.indexOf(',');
		return (comma >= 0 ? xff.substring(0, comma) : xff).trim();
	}
}
