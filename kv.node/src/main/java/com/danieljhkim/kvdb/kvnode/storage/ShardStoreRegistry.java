package com.danieljhkim.kvdb.kvnode.storage;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registry for shard-scoped stores on a node.
 */
public class ShardStoreRegistry {

	private static final Logger logger = LoggerFactory.getLogger(ShardStoreRegistry.class);

	private final Path baseDir;
	private final String snapshotFileName;
	private final String walFileName;
	private final int flushInterval;
	private final boolean enableAutoFlush;

	private final ConcurrentMap<String, ShardKVStore> stores = new ConcurrentHashMap<>();

	public ShardStoreRegistry(
			String baseDir,
			String snapshotFileName,
			String walFileName,
			int flushInterval,
			boolean enableAutoFlush) {
		this.baseDir = Paths.get(Objects.requireNonNull(baseDir, "baseDir"));
		this.snapshotFileName = Objects.requireNonNull(snapshotFileName, "snapshotFileName");
		this.walFileName = Objects.requireNonNull(walFileName, "walFileName");
		this.flushInterval = flushInterval;
		this.enableAutoFlush = enableAutoFlush;
	}

	public ShardKVStore getOrCreate(String shardId) {
		return stores.computeIfAbsent(shardId, this::createStore);
	}

	private ShardKVStore createStore(String shardId) {
		Path shardDir = baseDir.resolve(shardId);
		String snapshotPath = shardDir.resolve(snapshotFileName).toString();
		String walPath = shardDir.resolve(walFileName).toString();
		return new ShardKVStore(shardId, snapshotPath, walPath, flushInterval, enableAutoFlush);
	}

	public void shutdown() {
		for (Map.Entry<String, ShardKVStore> e : stores.entrySet()) {
			try {
				e.getValue().shutdown();
			} catch (Exception ex) {
				logger.warn("Failed to shutdown shard store shardId={}", e.getKey(), ex);
			}
		}
		stores.clear();
	}
}
