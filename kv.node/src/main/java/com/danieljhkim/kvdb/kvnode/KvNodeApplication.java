package com.danieljhkim.kvdb.kvnode;

import com.danieljhkim.kvdb.kvnode.repository.KVStoreRepository;
import com.danieljhkim.kvdb.kvcommon.config.SystemConfig;
import com.danieljhkim.kvdb.kvcommon.server.KVGrpcServer;
import com.danieljhkim.kvdb.kvnode.service.KVServiceImpl;

import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

public class KvNodeApplication {

	private static final Logger LOGGER = Logger.getLogger(KvNodeApplication.class.getName());
	private static final int DEFAULT_PORT = 8001;
	private static SystemConfig CONFIG;

	public static void main(String[] args) {
		try {
			if (args.length < 1) {
				CONFIG = SystemConfig.getInstance("node-1");
			} else {
				LOGGER.info(Arrays.toString(args));
				CONFIG = SystemConfig.getInstance(args[0]);
			}
			startGrpcServer();
		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, "Server failed to start", e);
			System.exit(1);
		}
	}

	private static void startGrpcServer() throws Exception {
		int port = getPort();
		KVGrpcServer server = new KVGrpcServer.Builder()
				.setPort(port)
				.addService(new KVServiceImpl(new KVStoreRepository()))
				.build();
		addShutdownHook(server::shutdown);
		LOGGER.info("Starting gRPC server on port " + port);
		server.start();
	}

	private static int getPort() {
		return Integer.parseInt(
				CONFIG.getProperty(
						"kvdb.server.port", String.valueOf(KvNodeApplication.DEFAULT_PORT)));
	}

	private static void addShutdownHook(Runnable shutdownTask) {
		Runtime.getRuntime()
				.addShutdownHook(
						new Thread(
								() -> {
									LOGGER.info("Shutting down server...");
									shutdownTask.run();
								}));
	}
}
