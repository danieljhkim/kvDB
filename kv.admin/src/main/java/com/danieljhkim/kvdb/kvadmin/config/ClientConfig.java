package com.danieljhkim.kvdb.kvadmin.config;

import java.util.concurrent.TimeUnit;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.danieljhkim.kvdb.kvadmin.client.CoordinatorAdminClient;
import com.danieljhkim.kvdb.kvadmin.client.CoordinatorReadClient;
import com.danieljhkim.kvdb.kvadmin.client.GatewayAdminClient;
import com.danieljhkim.kvdb.kvadmin.client.NodeAdminClient;

import lombok.Data;

/**
 * Configuration for gRPC clients (Coordinator, Nodes, Gateway).
 */
@Configuration
@ConfigurationProperties(prefix = "kvdb.admin")
@Data
public class ClientConfig {

	/**
	 * Coordinator connection settings.
	 */
	private CoordinatorSettings coordinator = new CoordinatorSettings();

	/**
	 * Gateway connection settings (optional).
	 */
	private GatewaySettings gateway = new GatewaySettings();

	/**
	 * Default gRPC call timeout in seconds.
	 */
	private int defaultTimeoutSeconds = 10;

	@Data
	public static class CoordinatorSettings {
		private String host = "localhost";
		private int port = 9000;
	}

	@Data
	public static class GatewaySettings {
		private String host = "localhost";
		private int port = 7000;
		private boolean enabled = false;
	}

	@Bean
	public CoordinatorAdminClient coordinatorAdminClient() {
		return new CoordinatorAdminClient(
				coordinator.getHost(),
				coordinator.getPort(),
				defaultTimeoutSeconds,
				TimeUnit.SECONDS);
	}

	@Bean
	public CoordinatorReadClient coordinatorReadClient() {
		return new CoordinatorReadClient(
				coordinator.getHost(),
				coordinator.getPort(),
				defaultTimeoutSeconds,
				TimeUnit.SECONDS);
	}

	@Bean
	public NodeAdminClient nodeAdminClient() {
		return new NodeAdminClient(defaultTimeoutSeconds, TimeUnit.SECONDS);
	}

	@Bean
	public GatewayAdminClient gatewayAdminClient() {
		if (gateway.isEnabled()) {
			return new GatewayAdminClient(
					gateway.getHost(),
					gateway.getPort(),
					defaultTimeoutSeconds,
					TimeUnit.SECONDS);
		}
		return null;
	}
}

