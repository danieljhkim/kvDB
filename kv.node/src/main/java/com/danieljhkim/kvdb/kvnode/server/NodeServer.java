package com.danieljhkim.kvdb.kvnode.server;

import com.danieljhkim.kvdb.kvnode.repository.KVStoreRepository;
import com.danieljhkim.kvdb.kvnode.service.KVServiceImpl;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;

import java.io.IOException;
import java.util.logging.Logger;

public class NodeServer {
    private static final Logger LOGGER = Logger.getLogger(NodeServer.class.getName());

    private final Server server;
    private final int port;

    public NodeServer(int port) {
        this.port = port;

        KVServiceImpl kvservice = new KVServiceImpl(new KVStoreRepository());
        this.server = NettyServerBuilder
                .forPort(port)
                .addService(kvservice)
                .build();
    }

    public void start() throws IOException, InterruptedException {
        server.start();
        server.awaitTermination();
    }

    public void shutdown() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(3, java.util.concurrent.TimeUnit.SECONDS);
            LOGGER.info("NodeServer stopped");
        }
    }
}