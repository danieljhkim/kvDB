package com.danieljhkim.kvdb.kvadmin.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.Data;

/**
 * Configuration for the admin server (ports, timeouts, etc.).
 */
@Configuration
@ConfigurationProperties(prefix = "kvdb.admin")
@Data
public class AdminServerConfig {

	/**
	 * Overall timeout budget for a single admin request.
	 */
	private long requestTimeoutMs = 3000;

	/**
	 * Maximum number of nodes queried in parallel.
	 */
	private int maxNodeFanout = 8;
}