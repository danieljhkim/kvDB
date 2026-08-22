package com.danieljhkim.kvdb.kvadmin.config;

import com.danieljhkim.kvdb.kvadmin.security.AdminApiKeyFilter;
import com.danieljhkim.kvdb.kvadmin.security.AdminIpAllowlistFilter;
import java.util.List;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Security configuration for admin API (authn/authz, IP allowlist, mTLS).
 *
 * <p>
 * The admin API always requires an API key and an IP allowlist. Future production authentication
 * mechanisms may replace the API key, but must not make the control plane unauthenticated.
 */
@Configuration
@ConfigurationProperties(prefix = "kvdb.admin.security")
@Data
public class SecurityConfig {

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
     * API key for simple auth (not recommended for production). Only used in dev profile.
     */
    private String apiKey;

    /**
     * IP allowlist filter for every admin request.
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
    public FilterRegistrationBean<jakarta.servlet.Filter> adminIpAllowlistFilter() {
        FilterRegistrationBean<jakarta.servlet.Filter> bean = new FilterRegistrationBean<>();
        bean.setFilter(new AdminIpAllowlistFilter(allowedIps));
        bean.addUrlPatterns("/*");
        bean.setOrder(10);
        return bean;
    }

    /**
     * API key filter for every admin request. A missing key fails startup rather than exposing the
     * control plane without authentication.
     */
    @Bean
    public FilterRegistrationBean<jakarta.servlet.Filter> adminApiKeyFilter() {
        if (apiKey == null || apiKey.isBlank()) {
            throw new IllegalArgumentException("kvdb.admin.security.api-key must not be blank");
        }
        FilterRegistrationBean<jakarta.servlet.Filter> bean = new FilterRegistrationBean<>();
        bean.setFilter(new AdminApiKeyFilter(apiKey));
        bean.addUrlPatterns("/*");
        bean.setOrder(20);
        return bean;
    }
}
