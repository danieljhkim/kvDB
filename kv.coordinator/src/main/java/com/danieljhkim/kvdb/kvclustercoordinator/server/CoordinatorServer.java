package com.danieljhkim.kvdb.kvclustercoordinator.server;

import com.danieljhkim.kvdb.kvclustercoordinator.raft.StubRaftStateMachine;
import com.danieljhkim.kvdb.kvclustercoordinator.service.CoordinatorServiceImpl;
import com.danieljhkim.kvdb.kvclustercoordinator.service.WatcherManager;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import lombok.Getter;

import java.io.IOException;
import java.util.logging.Logger;

public class CoordinatorServer {
	private static final Logger LOGGER = Logger.getLogger(CoordinatorServer.class.getName());

	private final Server server;
	/**
	 * -- GETTER --
	 * Gets the Raft state machine for external access (e.g., for health checks).
	 */
	@Getter
	private final StubRaftStateMachine raftStateMachine;
	private final WatcherManager watcherManager;

	public CoordinatorServer(int port) {
		// Initialize Raft state machine
		this.raftStateMachine = new StubRaftStateMachine();

		// Initialize watcher manager and register it with Raft for delta events
		this.watcherManager = new WatcherManager();
		this.raftStateMachine.addWatcher(watcherManager);

		// Create the coordinator service
		CoordinatorServiceImpl coordinatorService = new CoordinatorServiceImpl(raftStateMachine, watcherManager);

		this.server = NettyServerBuilder
				.forPort(port)
				.addService(coordinatorService)
				.build();
	}

	public void start() throws IOException, InterruptedException {
		watcherManager.start();
		server.start();
		server.awaitTermination();
	}

	public void shutdown() throws InterruptedException {
		LOGGER.info("Shutting down CoordinatorServer...");

		// Shutdown gRPC server
		if (server != null) {
			server.shutdown().awaitTermination(3, java.util.concurrent.TimeUnit.SECONDS);
			LOGGER.info("CoordinatorServer stopped");
		}
	}

}