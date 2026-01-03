package com.danieljhkim.kvdb.kvclustercoordinator.server;

import com.danieljhkim.kvdb.kvclustercoordinator.raft.StubRaftStateMachine;
import com.danieljhkim.kvdb.kvclustercoordinator.service.CoordinatorServiceImpl;
import com.danieljhkim.kvdb.kvclustercoordinator.service.WatcherManager;
import com.danieljhkim.kvdb.kvcommon.grpc.GlobalExceptionInterceptor;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.io.IOException;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CoordinatorServer {
    private static final Logger logger = LoggerFactory.getLogger(CoordinatorServer.class);

    private final Server server;

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
        ServerServiceDefinition interceptedService =
                ServerInterceptors.intercept(coordinatorService, new GlobalExceptionInterceptor());

        this.server =
                NettyServerBuilder.forPort(port).addService(interceptedService).build();
    }

    public void start() throws IOException, InterruptedException {
        watcherManager.start();
        server.start();
        server.awaitTermination();
    }

    public void shutdown() throws InterruptedException {
        logger.info("Shutting down CoordinatorServer...");

        if (server != null) {
            server.shutdown().awaitTermination(3, java.util.concurrent.TimeUnit.SECONDS);
            logger.info("CoordinatorServer stopped");
        }
    }
}
