package com.danieljhkim.kvdb.kvadmin.config;

import com.danieljhkim.kvdb.kvadmin.security.AdminApiKeyFilter;
import com.danieljhkim.kvdb.kvadmin.security.AdminIpAllowlistFilter;
import lombok.Data;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.util.List;

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
	 * Only used in dev profile.
	 */
	private String apiKey;

	/**
	 * IP allowlist filter. Enforces {@code allowedIps} when {@code enabled=true}.
	 *
	 * <p>
	 * Note: JWT/mTLS are not implemented yet. This module currently supports:
	 * <ul>
	 * <li>IP allowlist (CIDR)</li>
	 * <li>Dev-only API key (X-Admin-Api-Key)</li>
	 * </ul>
	 * </p>
	 */
	@Bean
	@ConditionalOnProperty(prefix = "kvdb.admin.security", name = "enabled", havingValue = "true")
	public FilterRegistrationBean<jakarta.servlet.Filter> adminIpAllowlistFilter() {
		FilterRegistrationBean<jakarta.servlet.Filter> bean = new FilterRegistrationBean<>();
		bean.setFilter(new AdminIpAllowlistFilter(allowedIps));
		bean.addUrlPatterns("/*");
		bean.setOrder(10);
		return bean;
	}

	/**
	 * Dev-only API key filter. Enforces X-Admin-Api-Key when configured.
	 */
	@Bean
	@Profile("dev")
	@ConditionalOnProperty(prefix = "kvdb.admin.security", name = "api-key")
	public FilterRegistrationBean<jakarta.servlet.Filter> adminApiKeyFilter() {
		if (apiKey == null || apiKey.isBlank()) {
			throw new IllegalArgumentException(
					"kvdb.admin.security.api-key must not be blank when security is enabled (dev profile)");
		}
		FilterRegistrationBean<jakarta.servlet.Filter> bean = new FilterRegistrationBean<>();
		bean.setFilter(new AdminApiKeyFilter(apiKey));
		bean.addUrlPatterns("/*");
		bean.setOrder(20);
		return bean;
	}
}
