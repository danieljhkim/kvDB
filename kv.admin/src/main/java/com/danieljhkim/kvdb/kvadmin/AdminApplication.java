package com.danieljhkim.kvdb.kvadmin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * Main entry point for the KvDB Admin API server.
 * 
 * <p>
 * Provides REST APIs for cluster administration, monitoring, and operations.
 * 
 * <p>
 * Usage: java -jar kv-admin.jar
 * 
 * <p>
 * Configuration via application.properties or environment variables:
 * - kvdb.admin.server.port (default: 8081)
 * - kvdb.admin.coordinator.host (default: localhost)
 * - kvdb.admin.coordinator.port (default: 9000)
 * - kvdb.admin.client.default-timeout-seconds (default: 10)
 * - kvdb.admin.client.max-parallelism (default: 10)
 * - kvdb.admin.cache.shard-map-ttl-seconds (default: 5)
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class AdminApplication {

	private static final Logger logger = LoggerFactory.getLogger(AdminApplication.class);

	public static void main(String[] args) {
		logger.info("Starting KvDB Admin API server...");
		SpringApplication app = new SpringApplication(AdminApplication.class);
		app.run(args);
		logger.info("KvDB Admin API server started");
	}
}
