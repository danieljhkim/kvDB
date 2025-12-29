package com.danieljhkim.kvdb.kvnode.service;

import com.danieljhkim.kvdb.kvcommon.config.SystemConfig;
import com.danieljhkim.kvdb.kvcommon.exception.*;
import com.danieljhkim.kvdb.kvnode.cache.ShardMapCache;
import com.danieljhkim.kvdb.kvnode.client.ReplicaWriteClient;
import com.danieljhkim.kvdb.kvnode.cluster.NodeLeadership;
import com.danieljhkim.kvdb.kvnode.repository.BaseRepository;
import com.danieljhkim.kvdb.kvnode.storage.ShardKVStore;
import com.danieljhkim.kvdb.kvnode.storage.ShardStoreRegistry;
import com.danieljhkim.kvdb.proto.coordinator.ShardRecord;
import com.kvdb.proto.kvstore.*;
import io.grpc.stub.StreamObserver;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class KVServiceImpl extends KVServiceGrpc.KVServiceImplBase {

	private final BaseRepository store; // legacy single-store mode
	private final NodeLeadership leadership;
	private final String nodeId;
	private final ShardMapCache shardMapCache;
	private final ShardStoreRegistry shardStores; // per-shard mode
	private final ReplicaWriteClient replicaWriteClient;
	private final Duration replicationTimeout;

	public KVServiceImpl(BaseRepository store) {
		this(store, new NodeLeadership(SystemConfig.getInstance()));
	}

	public KVServiceImpl(BaseRepository store, NodeLeadership leadership) {
		this.store = store;
		this.leadership = leadership;
		this.nodeId = SystemConfig.getInstance().getProperty("kvdb.node.id", "");
		this.shardMapCache = null;
		this.shardStores = null;
		this.replicaWriteClient = null;
		this.replicationTimeout = Duration.ofMillis(500);
	}

	/**
	 * Per-shard leader mode constructor: uses coordinator shard map to decide
	 * replica/leader for each key's shard.
	 */
	public KVServiceImpl(BaseRepository store, String nodeId, ShardMapCache shardMapCache) {
		this.store = store;
		this.nodeId = nodeId;
		this.shardMapCache = shardMapCache;
		this.leadership = new NodeLeadership(SystemConfig.getInstance());
		this.shardStores = null;
		this.replicaWriteClient = null;
		this.replicationTimeout = Duration.ofMillis(500);
	}

	/**
	 * Per-shard persistence + replication constructor.
	 */
	public KVServiceImpl(
			String nodeId,
			ShardMapCache shardMapCache,
			ShardStoreRegistry shardStores,
			ReplicaWriteClient replicaWriteClient,
			Duration replicationTimeout) {
		this.store = null;
		this.nodeId = nodeId;
		this.shardMapCache = shardMapCache;
		this.shardStores = shardStores;
		this.replicaWriteClient = replicaWriteClient;
		this.replicationTimeout = replicationTimeout != null ? replicationTimeout : Duration.ofMillis(500);
		this.leadership = new NodeLeadership(SystemConfig.getInstance());
	}

	@Override
	public void get(KeyRequest request, StreamObserver<ValueResponse> responseObserver) {
		String key = request.getKey();
		String value;
		if (shardStores == null) {
			value = store.get(key);
		} else {
			String shardId = resolveShardIdOrThrow(key);
			enforceReadRouting(key, shardId);
			value = shardStores.getOrCreate(shardId).get(key);
		}

		ValueResponse response = ValueResponse.newBuilder().setValue(value != null ? value : "").build();

		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}

	@Override
	public void set(KeyValueRequest request, StreamObserver<SetResponse> responseObserver) {
		String key = request.getKey();
		String value = request.getValue();
		if (shardStores == null) {
			enforceWriteRouting(key);
			store.update(key, value);
		} else {
			String shardId = resolveShardIdOrThrow(key);
			ShardRecord shard = enforceWriteRouting(key, shardId);
			shardStores.getOrCreate(shardId).set(key, value);
			replicateSetOrThrow(shardId, shard, key, value);
		}

		SetResponse response = SetResponse.newBuilder().setSuccess(true).build();

		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}

	@Override
	public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
		String key = request.getKey();
		boolean success;
		if (shardStores == null) {
			enforceWriteRouting(key);
			success = store.delete(key);
		} else {
			String shardId = resolveShardIdOrThrow(key);
			ShardRecord shard = enforceWriteRouting(key, shardId);
			success = shardStores.getOrCreate(shardId).del(key);
			replicateDeleteOrThrow(shardId, shard, key);
		}

		DeleteResponse response = DeleteResponse.newBuilder().setSuccess(success).build();

		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}

	@Override
	public void replicateSet(ReplicateSetRequest request, StreamObserver<SetResponse> responseObserver) {
		String shardId = request.getShardId();
		String key = request.getKey();
		String expectedShard = resolveShardIdOrThrow(key);
		if (!expectedShard.equals(shardId)) {
			throw new InvalidRequestException("shard_id does not match key's resolved shard");
		}

		enforceReplicaAndEpochOrThrow(shardId, request.getEpoch());
		ShardKVStore s = shardStores.getOrCreate(shardId);
		s.set(key, request.getValue());

		SetResponse response = SetResponse.newBuilder().setSuccess(true).build();
		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}

	@Override
	public void replicateDelete(
			ReplicateDeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
		String shardId = request.getShardId();
		String key = request.getKey();
		String expectedShard = resolveShardIdOrThrow(key);
		if (!expectedShard.equals(shardId)) {
			throw new InvalidRequestException("shard_id does not match key's resolved shard");
		}

		enforceReplicaAndEpochOrThrow(shardId, request.getEpoch());
		ShardKVStore s = shardStores.getOrCreate(shardId);
		boolean success = s.del(key);

		DeleteResponse response = DeleteResponse.newBuilder().setSuccess(success).build();
		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}

	private void enforceWriteRouting(String key) {
		// If shard map cache is present, use per-shard leader checks; otherwise fall
		// back
		// to the temporary single-leader mode.
		if (shardMapCache == null) {
			if (!leadership.isWriteLeader()) {
				throw new NotLeaderException(leadership.getWriteLeaderHint());
			}
			return;
		}

		if (!shardMapCache.isInitialized()) {
			throw new ShardMapUnavailableException("Shard map not initialized");
		}

		String shardId = shardMapCache.resolveShardId(key.getBytes(java.nio.charset.StandardCharsets.UTF_8));
		enforceWriteRouting(key, shardId);
	}

	private ShardRecord enforceWriteRouting(String key, String shardId) {
		if (shardMapCache == null || !shardMapCache.isInitialized()) {
			throw new ShardMapUnavailableException("Shard map not initialized");
		}
		ShardRecord shard = shardMapCache.getShard(shardId);
		if (shard == null) {
			throw new ShardMapUnavailableException("Shard not found in shard map: " + shardId);
		}

		// If we are not a replica for this shard -> SHARD_MOVED
		if (!shardMapCache.isReplica(shardId, nodeId)) {
			String hint = shardMapCache.getLeaderAddress(shardId)
					.or(() -> shardMapCache.getAnyReplicaAddress(shardId))
					.orElse("");
			throw new ShardMovedException(shardId, hint);
		}

		// We are a replica, but if we are not the leader -> NOT_LEADER with leader hint
		String leaderAddr = shardMapCache.getLeaderAddress(shardId).orElse(leadership.getWriteLeaderHint());
		String leaderNodeId = shardMapCache.getLeaderNodeId(shardId).orElse("");
		if (!leaderNodeId.isEmpty() && !leaderNodeId.equals(nodeId)) {
			throw new NotLeaderException(leaderAddr);
		}
		return shard;
	}

	private void enforceReadRouting(String key, String shardId) {
		if (shardMapCache == null || !shardMapCache.isInitialized()) {
			throw new ShardMapUnavailableException("Shard map not initialized");
		}
		// If we are not a replica for this shard -> SHARD_MOVED (leader hint preferred)
		if (!shardMapCache.isReplica(shardId, nodeId)) {
			String hint = shardMapCache.getLeaderAddress(shardId)
					.or(() -> shardMapCache.getAnyReplicaAddress(shardId))
					.orElse("");
			throw new ShardMovedException(shardId, hint);
		}
	}

	private void enforceReplicaAndEpochOrThrow(String shardId, long epoch) {
		if (shardMapCache == null || !shardMapCache.isInitialized()) {
			throw new ShardMapUnavailableException("Shard map not initialized");
		}
		ShardRecord shard = shardMapCache.getShard(shardId);
		if (shard == null) {
			throw new ShardMapUnavailableException("Shard not found in shard map: " + shardId);
		}
		if (!shardMapCache.isReplica(shardId, nodeId)) {
			String hint = shardMapCache.getLeaderAddress(shardId)
					.or(() -> shardMapCache.getAnyReplicaAddress(shardId))
					.orElse("");
			throw new ShardMovedException(shardId, hint);
		}
		if (epoch != 0 && shard.getEpoch() != 0 && shard.getEpoch() != epoch) {
			String hint = shardMapCache.getLeaderAddress(shardId)
					.or(() -> shardMapCache.getAnyReplicaAddress(shardId))
					.orElse("");
			throw new ShardMovedException(shardId, hint);
		}
	}

	private String resolveShardIdOrThrow(String key) {
		if (shardMapCache == null || !shardMapCache.isInitialized()) {
			throw new ShardMapUnavailableException("Shard map not initialized");
		}
		return shardMapCache.resolveShardId(key.getBytes(java.nio.charset.StandardCharsets.UTF_8));
	}

	@SuppressWarnings("all")
	private void replicateSetOrThrow(String shardId, ShardRecord shard, String key, String value) {
		if (replicaWriteClient == null) {
			return;
		}
		List<String> replicaIds = shard.getReplicasList();
		int n = replicaIds.size();
		if (n <= 1) {
			return;
		}

		int required = (n / 2) + 1; // quorum
		AtomicInteger acks = new AtomicInteger(1); // local leader write counts

		List<String> targets = new ArrayList<>();
		for (String rid : replicaIds) {
			if (rid != null && !rid.isEmpty() && !rid.equals(nodeId)) {
				targets.add(rid);
			}
		}

		long epoch = shard.getEpoch();
		ReplicateSetRequest req = ReplicateSetRequest.newBuilder()
				.setShardId(shardId)
				.setEpoch(epoch)
				.setKey(key)
				.setValue(value)
				.setOriginNodeId(nodeId)
				.build();

		List<Thread> threads = new ArrayList<>(targets.size());
		for (String replicaId : targets) {
			String address = shardMapCache == null ? "" : shardMapCache.getNodeAddress(replicaId).orElse("");
			if (address == null || address.isBlank()) {
				continue;
			}
			threads.add(Thread.startVirtualThread(() -> {
				try {
					SetResponse resp = replicaWriteClient.replicateSet(address, req);
					if (resp.getSuccess()) {
						acks.incrementAndGet();
					}
				} catch (Throwable t) {
					// ignore; count as failure
				}
			}));
		}

		long deadlineMs = System.currentTimeMillis() + replicationTimeout.toMillis();
		for (Thread t : threads) {
			if (acks.get() >= required) {
				return;
			}
			long remaining = deadlineMs - System.currentTimeMillis();
			if (remaining <= 0) {
				break;
			}
			try {
				t.join(remaining);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				break;
			}
		}
		if (acks.get() >= required) {
			return;
		}

		throw new NodeUnavailableException(
				"Replication quorum not reached for shard " + shardId + " (acks=" + acks.get() + ", required="
						+ required + ")",
				shardId);
	}

	@SuppressWarnings("all")
	private void replicateDeleteOrThrow(String shardId, ShardRecord shard, String key) {
		if (replicaWriteClient == null) {
			return;
		}
		List<String> replicaIds = shard.getReplicasList();
		int n = replicaIds.size();
		if (n <= 1) {
			return;
		}

		int required = (n / 2) + 1; // quorum
		AtomicInteger acks = new AtomicInteger(1); // local leader write counts

		List<String> targets = new ArrayList<>();
		for (String rid : replicaIds) {
			if (rid != null && !rid.isEmpty() && !rid.equals(nodeId)) {
				targets.add(rid);
			}
		}

		long epoch = shard.getEpoch();
		ReplicateDeleteRequest req = ReplicateDeleteRequest.newBuilder()
				.setShardId(shardId)
				.setEpoch(epoch)
				.setKey(key)
				.setOriginNodeId(nodeId)
				.build();

		List<Thread> threads = new ArrayList<>(targets.size());
		for (String replicaId : targets) {
			String address = shardMapCache == null ? "" : shardMapCache.getNodeAddress(replicaId).orElse("");
			if (address == null || address.isBlank()) {
				continue;
			}
			threads.add(Thread.startVirtualThread(() -> {
				try {
					replicaWriteClient.replicateDelete(address, req);
					acks.incrementAndGet();
				} catch (Throwable t) {
					// ignore; count as failure
				}
			}));
		}

		long deadlineMs = System.currentTimeMillis() + replicationTimeout.toMillis();
		for (Thread t : threads) {
			if (acks.get() >= required) {
				return;
			}
			long remaining = deadlineMs - System.currentTimeMillis();
			if (remaining <= 0) {
				break;
			}
			try {
				t.join(remaining);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				break;
			}
		}
		if (acks.get() >= required) {
			return;
		}

		throw new NodeUnavailableException(
				"Replication quorum not reached for shard " + shardId + " (acks=" + acks.get() + ", required="
						+ required + ")",
				shardId);
	}

	@Override
	public void ping(PingRequest request, StreamObserver<PingResponse> responseObserver) {
		PingResponse response = PingResponse.newBuilder().setMessage("pong").build();

		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}

	@Override
	public void shutdown(
			ShutdownRequest request, StreamObserver<ShutdownResponse> responseObserver) {
		ShutdownResponse response = ShutdownResponse.newBuilder().setMessage("goodbye").build();

		responseObserver.onNext(response);
		responseObserver.onCompleted();
		System.exit(0);
	}

}
