package com.danieljhkim.kvdb.kvcommon.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.BindableService;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Example Usage:
 *
 * <p>
 * KVGrpcServer server = new KVGrpcServer.Builder() .setPort(8080) // Set the
 * desired port
 * .addService(new KVServiceImpl(new KVStoreRepository())) // Add gRPC service
 * .build();
 *
 * <p>
 * server.start();
 */
public class KVGrpcServer implements BaseServer {

	private final int port;
	private final Server server;
	private final List<BindableService> services = new ArrayList<>();
	private boolean running = false;

	private KVGrpcServer(Builder builder) {
		this.port = builder.port;
		ServerBuilder<?> serverBuilder = ServerBuilder.forPort(port);

		for (BindableService service : builder.services) {
			serverBuilder.addService(service);
		}
		this.server = serverBuilder.build();
	}

	public void start() throws Exception {
		server.start();
		System.out.println("gRPC KV server started on port " + port);
		server.awaitTermination();
		running = true;
	}

	public void shutdown() {
		System.out.println("Shutting down gRPC KV server...");
		try {
			server.shutdown().awaitTermination(5, TimeUnit.SECONDS);
			System.out.println("gRPC KV server shut down successfully.");
		} catch (InterruptedException e) {
			System.err.println("gRPC KV server shutdown interrupted: " + e.getMessage());
			server.shutdownNow();
			Thread.currentThread().interrupt();
		}
	}

	public boolean isRunning() {
		return running;
	}

	public static class Builder {
		private final List<BindableService> services = new ArrayList<>();
		private int port = 9001; // FIXME: Default port

		public Builder setPort(int port) {
			this.port = port;
			return this;
		}

		public Builder addService(BindableService service) {
			this.services.add(service);
			return this;
		}

		public KVGrpcServer build() {
			return new KVGrpcServer(this);
		}
	}
}
