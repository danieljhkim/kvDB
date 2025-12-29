package com.danieljhkim.kvdb.kvadmin.service;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import com.danieljhkim.kvdb.kvadmin.api.dto.ShardMapSnapshotDto;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * In-memory cache for shard map snapshot with TTL.
 * Thread-safe with read-write lock.
 */
@Component
@ConfigurationProperties(prefix = "kvdb.admin.cache")
@Data
@Slf4j
public class ShardMapCache {

	/**
	 * TTL for shard map cache in seconds.
	 */
	private long shardMapTtlSeconds = 5;

	private ShardMapCacheEntry cachedEntry;
	private final ReadWriteLock lock = new ReentrantReadWriteLock();

	/**
	 * Gets the cached shard map if available and fresh.
	 * 
	 * @return Cached shard map, or null if cache miss or expired
	 */
	public ShardMapSnapshotDto get() {
		lock.readLock().lock();
		try {
			if (cachedEntry == null) {
				return null;
			}

			long now = System.currentTimeMillis();
			if (now - cachedEntry.timestampMs > shardMapTtlSeconds * 1000) {
				log.debug("Shard map cache expired");
				return null;
			}

			log.debug("Shard map cache hit");
			return cachedEntry.shardMap;
		} finally {
			lock.readLock().unlock();
		}
	}

	/**
	 * Gets the cached map version if available and fresh.
	 * 
	 * @return Cached map version, or null if cache miss or expired
	 */
	public Long getVersion() {
		ShardMapSnapshotDto cached = get();
		return cached != null ? cached.getMapVersion() : null;
	}

	/**
	 * Updates the cache with a new shard map.
	 * 
	 * @param shardMap
	 *            The shard map to cache
	 */
	public void put(ShardMapSnapshotDto shardMap) {
		lock.writeLock().lock();
		try {
			this.cachedEntry = new ShardMapCacheEntry(shardMap, System.currentTimeMillis());
			log.debug("Shard map cache updated, version: {}", shardMap.getMapVersion());
		} finally {
			lock.writeLock().unlock();
		}
	}

	/**
	 * Clears the cache.
	 */
	public void clear() {
		lock.writeLock().lock();
		try {
			this.cachedEntry = null;
			log.debug("Shard map cache cleared");
		} finally {
			lock.writeLock().unlock();
		}
	}

	private static class ShardMapCacheEntry {
		final ShardMapSnapshotDto shardMap;
		final long timestampMs;

		ShardMapCacheEntry(ShardMapSnapshotDto shardMap, long timestampMs) {
			this.shardMap = shardMap;
			this.timestampMs = timestampMs;
		}
	}
}
