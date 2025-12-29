package com.danieljhkim.kvdb.kvadmin.config;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import lombok.Data;

/**
 * Security configuration for admin API (authn/authz, IP allowlist, mTLS).
 * 
 * <p>
 * This is a placeholder configuration. In production, implement:
 * - Authentication (JWT, OAuth2, mTLS)
 * - Authorization (RBAC)
 * - IP allowlist/denylist
 * - Rate limiting
 */
@Configuration
@ConfigurationProperties(prefix = "kvdb.admin.security")
@Data
public class SecurityConfig {

	/**
	 * Enable security features.
	 */
	private boolean enabled = false;

	/**
	 * Allowed IP addresses (CIDR notation).
	 */
	private List<String> allowedIps = List.of("127.0.0.1/32", "::1/128");

	/**
	 * Require mTLS for admin operations.
	 */
	private boolean requireMtls = false;

	/**
	 * JWT issuer (if using JWT auth).
	 */
	private String jwtIssuer;

	/**
	 * JWT audience (if using JWT auth).
	 */
	private String jwtAudience;

	/**
	 * API key for simple auth (not recommended for production).
	 */
	@Profile("dev")
	private String apiKey;
}

