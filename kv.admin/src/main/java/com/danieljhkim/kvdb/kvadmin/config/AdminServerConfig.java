package com.danieljhkim.kvdb.kvadmin.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.Data;

/**
 * Configuration for the admin server (ports, timeouts, etc.).
 */
@Configuration
@ConfigurationProperties(prefix = "kvdb.admin.server")
@Data
public class AdminServerConfig {

	/**
	 * HTTP server port for admin API.
	 */
	private int port = 8081;

	/**
	 * Request timeout in milliseconds.
	 */
	private long requestTimeoutMs = 30000;

	/**
	 * Maximum request body size in bytes.
	 */
	private long maxRequestBodySize = 10485760; // 10MB
}

