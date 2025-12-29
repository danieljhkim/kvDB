package com.danieljhkim.kvdb.kvnode.service;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

import com.danieljhkim.kvdb.kvcommon.exception.NotLeaderException;
import com.danieljhkim.kvdb.kvcommon.exception.ShardMovedException;
import com.danieljhkim.kvdb.kvnode.cache.ShardMapCache;
import com.danieljhkim.kvdb.kvnode.cluster.NodeLeadership;
import com.danieljhkim.kvdb.kvnode.repository.BaseRepository;
import com.kvdb.proto.kvstore.DeleteRequest;
import com.kvdb.proto.kvstore.KeyValueRequest;

import io.grpc.stub.StreamObserver;

class KVServiceLeaderCheckTest {

	private static final class NoopRepo implements BaseRepository {
		@Override
		public String get(String key) {
			return null;
		}

		@Override
		public boolean put(String key, String value) {
			return false;
		}

		@Override
		public boolean update(String key, String value) {
			return false;
		}

		@Override
		public boolean delete(String key) {
			return false;
		}

		@Override
		public boolean exists(String key) {
			return false;
		}

		@Override
		public List<String> getAllKeys() {
			return List.of();
		}

		@Override
		public Map<String, String> getMultiple(List<String> keys) {
			return Map.of();
		}

		@Override
		public int truncate() {
			return 0;
		}

		@Override
		public boolean isHealthy() {
			return true;
		}

		@Override
		public void initialize(String tableName) {
		}

		@Override
		public String getTableName() {
			return "";
		}

		@Override
		public void shutdown() {
		}
	}

	private static final class NoopObserver<T> implements StreamObserver<T> {
		@Override
		public void onNext(T value) {
		}

		@Override
		public void onError(Throwable t) {
		}

		@Override
		public void onCompleted() {
		}
	}

	@Test
	void set_onNonLeader_throwsNotLeader() {
		NodeLeadership leadership = new NodeLeadership("node-2", "node-1", "localhost", 8001);
		KVServiceImpl svc = new KVServiceImpl(new NoopRepo(), leadership);

		assertThrows(NotLeaderException.class, () -> svc.set(
				KeyValueRequest.newBuilder().setKey("k").setValue("v").build(),
				new NoopObserver<>()));
	}

	@Test
	void delete_onNonLeader_throwsNotLeader() {
		NodeLeadership leadership = new NodeLeadership("node-2", "node-1", "localhost", 8001);
		KVServiceImpl svc = new KVServiceImpl(new NoopRepo(), leadership);

		assertThrows(NotLeaderException.class, () -> svc.delete(
				DeleteRequest.newBuilder().setKey("k").build(),
				new NoopObserver<>()));
	}

	@Test
	void set_whenNotReplica_throwsShardMoved() {
		ShardMapCache cache = new ShardMapCache();
		// Do not initialize cache with state; KVServiceImpl should treat this as
		// unavailable -> SHARD_MOVED won't apply.
		// Instead, we provide a minimal state by using refreshFromFullState in a
		// dedicated test that constructs state.
		// Here we just validate the code path by ensuring a ShardMovedException is
		// thrown once cache is initialized.

		// Build minimal cluster state: shard-0 replicas=[node-1], leader=node-1, node-1
		// address=localhost:8001
		var state = com.danieljhkim.kvdb.proto.coordinator.ClusterState.newBuilder()
				.setMapVersion(1)
				.putNodes("node-1", com.danieljhkim.kvdb.proto.coordinator.NodeRecord.newBuilder()
						.setNodeId("node-1")
						.setAddress("localhost:8001")
						.build())
				.putShards("shard-0", com.danieljhkim.kvdb.proto.coordinator.ShardRecord.newBuilder()
						.setShardId("shard-0")
						.addReplicas("node-1")
						.setLeader("node-1")
						.build())
				.setPartitioning(com.danieljhkim.kvdb.proto.coordinator.PartitioningConfig.newBuilder()
						.setNumShards(1)
						.setReplicationFactor(1)
						.build())
				.build();
		cache.refreshFromFullState(state);

		KVServiceImpl svc = new KVServiceImpl(new NoopRepo(), "node-2", cache);
		assertThrows(ShardMovedException.class, () -> svc.set(
				KeyValueRequest.newBuilder().setKey("k").setValue("v").build(),
				new NoopObserver<>()));
	}
}
