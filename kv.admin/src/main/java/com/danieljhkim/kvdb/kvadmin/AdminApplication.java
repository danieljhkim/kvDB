package com.danieljhkim.kvdb.kvadmin;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

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
 */
@SpringBootApplication
public class AdminApplication {

	public static void main(String[] args) {
		SpringApplication.run(AdminApplication.class, args);
	}
}

