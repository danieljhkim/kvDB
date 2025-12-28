package com.danieljhkim.kvdb.kvgateway.service;

import com.danieljhkim.kvdb.kvgateway.cache.ShardMapCache;
import com.danieljhkim.kvdb.kvgateway.client.NodeConnectionPool;
import com.danieljhkim.kvdb.proto.coordinator.NodeRecord;
import com.danieljhkim.kvdb.proto.gateway.Consistency;
import com.danieljhkim.kvdb.proto.gateway.DeleteRequest;
import com.danieljhkim.kvdb.proto.gateway.DeleteResponse;
import com.danieljhkim.kvdb.proto.gateway.GetRequest;
import com.danieljhkim.kvdb.proto.gateway.GetResponse;
import com.danieljhkim.kvdb.proto.gateway.KeyValue;
import com.danieljhkim.kvdb.proto.gateway.KvGatewayGrpc;
import com.danieljhkim.kvdb.proto.gateway.PutRequest;
import com.danieljhkim.kvdb.proto.gateway.PutResponse;
import com.danieljhkim.kvdb.proto.gateway.Status;
import com.kvdb.proto.kvstore.KVServiceGrpc;
import com.kvdb.proto.kvstore.KeyRequest;
import com.kvdb.proto.kvstore.KeyValueRequest;
import com.kvdb.proto.kvstore.SetResponse;
import com.kvdb.proto.kvstore.ValueResponse;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * gRPC service implementation for the KvGateway.
 * Handles Get, Put, Delete operations by routing to appropriate storage nodes.
 */
public class KvGatewayServiceImpl extends KvGatewayGrpc.KvGatewayImplBase {

	private static final Logger LOGGER = Logger.getLogger(KvGatewayServiceImpl.class.getName());
	private static final int DEFAULT_TIMEOUT_MS = 5000;

	private final ShardMapCache shardMapCache;
	private final NodeConnectionPool nodePool;

	public KvGatewayServiceImpl(ShardMapCache shardMapCache, NodeConnectionPool nodePool) {
		this.shardMapCache = shardMapCache;
		this.nodePool = nodePool;
	}

	@Override
	public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
		try {
			// Validate request
			if (request.getKey().isEmpty()) {
				responseObserver.onNext(GetResponse.newBuilder()
						.setStatus(Status.newBuilder()
								.setCode(Status.Code.INVALID_ARGUMENT)
								.setMessage("Key cannot be empty")
								.build())
						.build());
				responseObserver.onCompleted();
				return;
			}

			byte[] keyBytes = request.getKey().toByteArray();
			String keyStr = new String(keyBytes, StandardCharsets.UTF_8);

			// Resolve shard and get target node
			String shardId = shardMapCache.resolveShardId(keyBytes);
			NodeRecord targetNode = selectNodeForRead(shardId, request.getOptions().getConsistency());

			if (targetNode == null) {
				responseObserver.onNext(GetResponse.newBuilder()
						.setStatus(Status.newBuilder()
								.setCode(Status.Code.UNAVAILABLE)
								.setMessage("No available node for shard: " + shardId)
								.setShardId(shardId)
								.build())
						.build());
				responseObserver.onCompleted();
				return;
			}

			// Forward to storage node
			KVServiceGrpc.KVServiceBlockingStub stub = nodePool.getStub(targetNode.getAddress());
			KeyRequest nodeRequest = KeyRequest.newBuilder()
					.setKey(keyStr)
					.build();

			ValueResponse nodeResponse = stub
					.withDeadlineAfter(DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS)
					.get(nodeRequest);

			// Map response
			if (nodeResponse.getValue().isEmpty()) {
				responseObserver.onNext(GetResponse.newBuilder()
						.setStatus(Status.newBuilder()
								.setCode(Status.Code.NOT_FOUND)
								.setMessage("Key not found: " + keyStr)
								.setShardId(shardId)
								.build())
						.build());
			} else {
				responseObserver.onNext(GetResponse.newBuilder()
						.setStatus(Status.newBuilder()
								.setCode(Status.Code.OK)
								.setShardId(shardId)
								.build())
						.setKv(KeyValue.newBuilder()
								.setKey(request.getKey())
								.setValue(com.google.protobuf.ByteString.copyFromUtf8(nodeResponse.getValue()))
								.build())
						.build());
			}
			responseObserver.onCompleted();

		} catch (IllegalStateException e) {
			LOGGER.log(Level.WARNING, "Shard map not available", e);
			responseObserver.onNext(GetResponse.newBuilder()
					.setStatus(Status.newBuilder()
							.setCode(Status.Code.UNAVAILABLE)
							.setMessage("Shard map not available: " + e.getMessage())
							.build())
					.build());
			responseObserver.onCompleted();
		} catch (StatusRuntimeException e) {
			LOGGER.log(Level.WARNING, "Node communication error during GET", e);
			responseObserver.onNext(GetResponse.newBuilder()
					.setStatus(Status.newBuilder()
							.setCode(Status.Code.UNAVAILABLE)
							.setMessage("Node communication error: " + e.getStatus().getDescription())
							.build())
					.build());
			responseObserver.onCompleted();
		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, "Unexpected error during GET", e);
			responseObserver.onNext(GetResponse.newBuilder()
					.setStatus(Status.newBuilder()
							.setCode(Status.Code.INTERNAL)
							.setMessage("Internal error: " + e.getMessage())
							.build())
					.build());
			responseObserver.onCompleted();
		}
	}

	@Override
	public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
		try {
			// Validate request
			if (request.getKey().isEmpty()) {
				responseObserver.onNext(PutResponse.newBuilder()
						.setStatus(Status.newBuilder()
								.setCode(Status.Code.INVALID_ARGUMENT)
								.setMessage("Key cannot be empty")
								.build())
						.build());
				responseObserver.onCompleted();
				return;
			}

			byte[] keyBytes = request.getKey().toByteArray();
			String keyStr = new String(keyBytes, StandardCharsets.UTF_8);
			String valueStr = request.getValue().toStringUtf8();

			// Resolve shard and get leader (writes always go to leader)
			String shardId = shardMapCache.resolveShardId(keyBytes);
			NodeRecord leader = shardMapCache.getLeader(shardId);

			if (leader == null) {
				// Fallback: try any replica
				leader = shardMapCache.getHealthyReplica(shardId);
			}

			if (leader == null) {
				responseObserver.onNext(PutResponse.newBuilder()
						.setStatus(Status.newBuilder()
								.setCode(Status.Code.UNAVAILABLE)
								.setMessage("No available node for shard: " + shardId)
								.setShardId(shardId)
								.build())
						.build());
				responseObserver.onCompleted();
				return;
			}

			// Forward to storage node
			KVServiceGrpc.KVServiceBlockingStub stub = nodePool.getStub(leader.getAddress());
			KeyValueRequest nodeRequest = KeyValueRequest.newBuilder()
					.setKey(keyStr)
					.setValue(valueStr)
					.build();

			SetResponse nodeResponse = stub
					.withDeadlineAfter(DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS)
					.set(nodeRequest);

			// Map response
			if (nodeResponse.getSuccess()) {
				responseObserver.onNext(PutResponse.newBuilder()
						.setStatus(Status.newBuilder()
								.setCode(Status.Code.OK)
								.setShardId(shardId)
								.build())
						.setVersion(1) // Storage nodes don't track versions yet
						.build());
			} else {
				responseObserver.onNext(PutResponse.newBuilder()
						.setStatus(Status.newBuilder()
								.setCode(Status.Code.INTERNAL)
								.setMessage("Put operation failed on storage node")
								.setShardId(shardId)
								.build())
						.build());
			}
			responseObserver.onCompleted();

		} catch (IllegalStateException e) {
			LOGGER.log(Level.WARNING, "Shard map not available", e);
			responseObserver.onNext(PutResponse.newBuilder()
					.setStatus(Status.newBuilder()
							.setCode(Status.Code.UNAVAILABLE)
							.setMessage("Shard map not available: " + e.getMessage())
							.build())
					.build());
			responseObserver.onCompleted();
		} catch (StatusRuntimeException e) {
			LOGGER.log(Level.WARNING, "Node communication error during PUT", e);
			responseObserver.onNext(PutResponse.newBuilder()
					.setStatus(Status.newBuilder()
							.setCode(Status.Code.UNAVAILABLE)
							.setMessage("Node communication error: " + e.getStatus().getDescription())
							.build())
					.build());
			responseObserver.onCompleted();
		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, "Unexpected error during PUT", e);
			responseObserver.onNext(PutResponse.newBuilder()
					.setStatus(Status.newBuilder()
							.setCode(Status.Code.INTERNAL)
							.setMessage("Internal error: " + e.getMessage())
							.build())
					.build());
			responseObserver.onCompleted();
		}
	}

	@Override
	public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
		try {
			// Validate request
			if (request.getKey().isEmpty()) {
				responseObserver.onNext(DeleteResponse.newBuilder()
						.setStatus(Status.newBuilder()
								.setCode(Status.Code.INVALID_ARGUMENT)
								.setMessage("Key cannot be empty")
								.build())
						.build());
				responseObserver.onCompleted();
				return;
			}

			byte[] keyBytes = request.getKey().toByteArray();
			String keyStr = new String(keyBytes, StandardCharsets.UTF_8);

			// Resolve shard and get leader (writes always go to leader)
			String shardId = shardMapCache.resolveShardId(keyBytes);
			NodeRecord leader = shardMapCache.getLeader(shardId);

			if (leader == null) {
				// Fallback: try any replica
				leader = shardMapCache.getHealthyReplica(shardId);
			}

			if (leader == null) {
				responseObserver.onNext(DeleteResponse.newBuilder()
						.setStatus(Status.newBuilder()
								.setCode(Status.Code.UNAVAILABLE)
								.setMessage("No available node for shard: " + shardId)
								.setShardId(shardId)
								.build())
						.build());
				responseObserver.onCompleted();
				return;
			}

			// Forward to storage node
			KVServiceGrpc.KVServiceBlockingStub stub = nodePool.getStub(leader.getAddress());
			com.kvdb.proto.kvstore.DeleteRequest nodeRequest = com.kvdb.proto.kvstore.DeleteRequest.newBuilder()
					.setKey(keyStr)
					.build();

			com.kvdb.proto.kvstore.DeleteResponse nodeResponse = stub
					.withDeadlineAfter(DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS)
					.delete(nodeRequest);

			// Map response
			if (nodeResponse.getSuccess()) {
				responseObserver.onNext(DeleteResponse.newBuilder()
						.setStatus(Status.newBuilder()
								.setCode(Status.Code.OK)
								.setShardId(shardId)
								.build())
						.setVersion(1)
						.build());
			} else {
				responseObserver.onNext(DeleteResponse.newBuilder()
						.setStatus(Status.newBuilder()
								.setCode(Status.Code.NOT_FOUND)
								.setMessage("Key not found or delete failed")
								.setShardId(shardId)
								.build())
						.build());
			}
			responseObserver.onCompleted();

		} catch (IllegalStateException e) {
			LOGGER.log(Level.WARNING, "Shard map not available", e);
			responseObserver.onNext(DeleteResponse.newBuilder()
					.setStatus(Status.newBuilder()
							.setCode(Status.Code.UNAVAILABLE)
							.setMessage("Shard map not available: " + e.getMessage())
							.build())
					.build());
			responseObserver.onCompleted();
		} catch (StatusRuntimeException e) {
			LOGGER.log(Level.WARNING, "Node communication error during DELETE", e);
			responseObserver.onNext(DeleteResponse.newBuilder()
					.setStatus(Status.newBuilder()
							.setCode(Status.Code.UNAVAILABLE)
							.setMessage("Node communication error: " + e.getStatus().getDescription())
							.build())
					.build());
			responseObserver.onCompleted();
		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, "Unexpected error during DELETE", e);
			responseObserver.onNext(DeleteResponse.newBuilder()
					.setStatus(Status.newBuilder()
							.setCode(Status.Code.INTERNAL)
							.setMessage("Internal error: " + e.getMessage())
							.build())
					.build());
			responseObserver.onCompleted();
		}
	}

	/**
	 * Selects the appropriate node for a read operation based on consistency level.
	 *
	 * @param shardId
	 *            The shard ID
	 * @param consistency
	 *            The consistency level
	 * @return The target node, or null if none available
	 */
	private NodeRecord selectNodeForRead(String shardId, Consistency consistency) {
		if (consistency == Consistency.STRONG) {
			// Strong consistency: always go to leader
			NodeRecord leader = shardMapCache.getLeader(shardId);
			if (leader != null) {
				return leader;
			}
			// Fallback to any healthy replica if leader unknown
			return shardMapCache.getHealthyReplica(shardId);
		} else {
			// Eventual consistency: prefer any healthy replica
			NodeRecord replica = shardMapCache.getHealthyReplica(shardId);
			if (replica != null) {
				return replica;
			}
			// Fallback to leader
			return shardMapCache.getLeader(shardId);
		}
	}
}
