package com.danieljhkim.kvdb.kvadmin.config;

import com.danieljhkim.kvdb.kvadmin.client.*;
import lombok.Data;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Configuration for KvDB Admin gRPC clients.
 *
 * <p>
 * This config is intentionally thin: it binds to application.properties and
 * wires client wrappers that
 * create/manage gRPC channels internally.
 * </p>
 *
 * <p>
 * Authoritative properties (see kv.admin application.properties):
 * <ul>
 * <li>kvdb.coordinator.grpc.address</li>
 * <li>kvdb.coordinator.grpc.deadline-ms</li>
 * <li>kvdb.node.grpc.deadline-ms</li>
 * <li>kvdb.admin.max-node-fanout</li>
 * <li>kvdb.admin.request-timeout-ms</li>
 * </ul>
 * </p>
 */
@Configuration
@ConfigurationProperties(prefix = "kvdb")
@Data
public class ClientConfig {

	private final Coordinator coordinator = new Coordinator();
	private final Node node = new Node();
	private final Admin admin = new Admin();
	private final Gateway gateway = new Gateway();

	@Data
	public static class Coordinator {
		private final Grpc grpc = new Grpc();

		@Data
		public static class Grpc {
			/**
			 * Coordinator endpoint in host:port form.
			 */
			private String address = "localhost:9090";

			/**
			 * Per-RPC deadline for coordinator calls in milliseconds.
			 */
			private long deadlineMs = 1500;
		}
	}

	@Data
	public static class Node {
		private final Grpc grpc = new Grpc();

		@Data
		public static class Grpc {
			/**
			 * Per-RPC deadline for node calls in milliseconds.
			 */
			private long deadlineMs = 1500;
		}
	}

	@Data
	public static class Admin {
		/**
		 * Maximum parallel fanout when querying many nodes.
		 */
		private int maxNodeFanout = 8;

		/**
		 * Overall timeout budget for an admin HTTP request.
		 */
		private long requestTimeoutMs = 3000;
	}

	/**
	 * Optional Gateway client configuration. Disabled by default.
	 *
	 * <p>
	 * Properties:
	 * <ul>
	 * <li>kvdb.gateway.grpc.enabled</li>
	 * <li>kvdb.gateway.grpc.address</li>
	 * <li>kvdb.gateway.grpc.deadline-ms</li>
	 * </ul>
	 * </p>
	 */
	@Data
	public static class Gateway {
		private final Grpc grpc = new Grpc();

		@Data
		public static class Grpc {
			private boolean enabled = false;
			private String address = "localhost:8080";
			private long deadlineMs = 1500;
		}
	}

	@Bean
	public GrpcClientInterceptor grpcClientInterceptor() {
		// This interceptor should set per-call deadlines consistently across all admin
		// client calls.
		long ms = coordinator.getGrpc().getDeadlineMs();
		long seconds = Math.max(1, (ms + 999) / 1000);
		return new GrpcClientInterceptor((int) seconds);
	}

	@Bean
	public CoordinatorAdminClient coordinatorAdminClient() {
		HostPort hp = HostPort.parse(coordinator.getGrpc().getAddress(), "kvdb.coordinator.grpc.address");
		long deadlineMs = coordinator.getGrpc().getDeadlineMs();
		return new CoordinatorAdminClient(hp.host(), hp.port(), deadlineMs, TimeUnit.MILLISECONDS);
	}

	@Bean
	public CoordinatorReadClient coordinatorReadClient() {
		HostPort hp = HostPort.parse(coordinator.getGrpc().getAddress(), "kvdb.coordinator.grpc.address");
		long deadlineMs = coordinator.getGrpc().getDeadlineMs();
		return new CoordinatorReadClient(hp.host(), hp.port(), deadlineMs, TimeUnit.MILLISECONDS);
	}

	@Bean
	public NodeAdminClient nodeAdminClient() {
		long deadlineMs = node.getGrpc().getDeadlineMs();
		return new NodeAdminClient(deadlineMs, TimeUnit.MILLISECONDS);
	}

	@Bean
	@ConditionalOnProperty(prefix = "kvdb.gateway.grpc", name = "enabled", havingValue = "true")
	public GatewayAdminClient gatewayAdminClient() {
		HostPort hp = HostPort.parse(gateway.getGrpc().getAddress(), "kvdb.gateway.grpc.address");
		long deadlineMs = gateway.getGrpc().getDeadlineMs();
		return new GatewayAdminClient(hp.host(), hp.port(), deadlineMs, TimeUnit.MILLISECONDS);
	}

	/**
	 * Simple host:port parser with clear validation.
	 */
	static final class HostPort {
		private final String host;
		private final int port;

		private HostPort(String host, int port) {
			this.host = host;
			this.port = port;
		}

		public String host() {
			return host;
		}

		public int port() {
			return port;
		}

		static HostPort parse(String address, String propertyName) {
			Objects.requireNonNull(address, propertyName + " cannot be null");
			String trimmed = address.trim();
			if (trimmed.isEmpty()) {
				throw new IllegalArgumentException(propertyName + " cannot be blank");
			}

			int idx = trimmed.lastIndexOf(':');
			if (idx <= 0 || idx == trimmed.length() - 1) {
				throw new IllegalArgumentException(propertyName + " must be in host:port form (got: " + trimmed + ")");
			}

			String host = trimmed.substring(0, idx).trim();
			String portStr = trimmed.substring(idx + 1).trim();
			if (host.isEmpty() || portStr.isEmpty()) {
				throw new IllegalArgumentException(propertyName + " must be in host:port form (got: " + trimmed + ")");
			}

			int port;
			try {
				port = Integer.parseInt(portStr);
			} catch (NumberFormatException e) {
				throw new IllegalArgumentException(propertyName + " has invalid port: " + portStr, e);
			}

			if (port <= 0 || port > 65535) {
				throw new IllegalArgumentException(propertyName + " port out of range: " + port);
			}

			return new HostPort(host, port);
		}
	}
}
