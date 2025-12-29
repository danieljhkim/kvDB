package com.danieljhkim.kvdb.kvnode.server;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.danieljhkim.kvdb.kvcommon.config.SystemConfig;
import com.danieljhkim.kvdb.kvcommon.grpc.GlobalExceptionInterceptor;
import com.danieljhkim.kvdb.kvnode.cache.ShardMapCache;
import com.danieljhkim.kvdb.kvnode.client.CoordinatorShardMapClient;
import com.danieljhkim.kvdb.kvnode.client.ReplicaWriteClient;
import com.danieljhkim.kvdb.kvnode.client.WatchShardMapClient;
import com.danieljhkim.kvdb.kvnode.service.KVServiceImpl;
import com.danieljhkim.kvdb.kvnode.storage.ShardStoreRegistry;

import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;

public class NodeServer {
	private static final Logger logger = LoggerFactory.getLogger(NodeServer.class);

	private final Server server;
	private final ShardMapCache shardMapCache;
	private final CoordinatorShardMapClient coordinatorClient;
	private final WatchShardMapClient watchShardMapClient;
	private final ShardStoreRegistry shardStores;
	private final ReplicaWriteClient replicaWriteClient;

	public NodeServer(int port) {
		SystemConfig config = SystemConfig.getInstance();
		String nodeId = config.getProperty("kvdb.node.id", "");
		String coordinatorHost = config.getProperty("kvdb.coordinator.host", "localhost");
		int coordinatorPort = Integer.parseInt(config.getProperty("kvdb.coordinator.port", "9000"));

		this.coordinatorClient = new CoordinatorShardMapClient(coordinatorHost, coordinatorPort);
		this.shardMapCache = new ShardMapCache();

		// Persistence + replication config
		String baseDir = config.getProperty("kvdb.persistence.baseDir", defaultBaseDir(nodeId));
		String snapshotFileName = config.getProperty("kvdb.persistence.shard.snapshotFileName", "kvstore.dat");
		String walFileName = config.getProperty("kvdb.persistence.shard.walFileName", "kvstore.wal");
		int flushInterval = Integer.parseInt(config.getProperty("kvdb.persistence.autoFlushInterval", "1000"));
		boolean enableAutoFlush = Boolean.parseBoolean(config.getProperty("kvdb.persistence.enableAutoFlush", "true"));
		long replicationTimeoutMs = Long.parseLong(config.getProperty("kvdb.replication.timeoutMs", "500"));

		this.shardStores = new ShardStoreRegistry(
				baseDir,
				snapshotFileName,
				walFileName,
				flushInterval,
				enableAutoFlush);
		this.replicaWriteClient = new ReplicaWriteClient(Duration.ofMillis(replicationTimeoutMs));

		this.watchShardMapClient = new WatchShardMapClient(
				coordinatorHost,
				coordinatorPort,
				shardMapCache,
				() -> Thread.startVirtualThread(this::refreshShardMapIfPossible));

		KVServiceImpl kvservice = new KVServiceImpl(
				nodeId,
				shardMapCache,
				shardStores,
				replicaWriteClient,
				Duration.ofMillis(replicationTimeoutMs));
		ServerServiceDefinition interceptedService = ServerInterceptors.intercept(kvservice,
				new GlobalExceptionInterceptor());

		this.server = NettyServerBuilder
				.forPort(port)
				.addService(interceptedService)
				.build();
	}

	public void start() throws IOException, InterruptedException {
		// Best-effort initial shard map fetch before accepting writes
		refreshShardMapIfPossible();
		watchShardMapClient.start(shardMapCache.getMapVersion());

		server.start();
		server.awaitTermination();
	}

	public void shutdown() throws InterruptedException {
		try {
			watchShardMapClient.shutdown();
		} catch (Exception e) {
			logger.warn("Failed to shutdown WatchShardMapClient", e);
		}
		try {
			coordinatorClient.shutdown();
		} catch (Exception e) {
			logger.warn("Failed to shutdown CoordinatorShardMapClient", e);
		}
		try {
			replicaWriteClient.shutdown();
		} catch (Exception e) {
			logger.warn("Failed to shutdown ReplicaWriteClient", e);
		}
		try {
			shardStores.shutdown();
		} catch (Exception e) {
			logger.warn("Failed to shutdown shard stores", e);
		}

		if (server != null) {
			server.shutdown().awaitTermination(3, TimeUnit.SECONDS);
			logger.info("NodeServer stopped");
		}
	}

	private void refreshShardMapIfPossible() {
		try {
			long current = shardMapCache.getMapVersion();
			var state = coordinatorClient.fetchShardMap(current);
			if (state != null) {
				shardMapCache.refreshFromFullState(state);
			}
		} catch (Exception e) {
			logger.debug("Initial shard map fetch failed (continuing)", e);
		}
	}

	private static String defaultBaseDir(String nodeId) {
		if (nodeId == null || nodeId.isBlank()) {
			return "data";
		}
		return "data/" + nodeId;
	}
}