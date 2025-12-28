package com.danieljhkim.kvdb.kvgateway.service;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import com.danieljhkim.kvdb.kvcommon.exception.InvalidRequestException;
import com.danieljhkim.kvdb.kvcommon.exception.KeyNotFoundException;
import com.danieljhkim.kvdb.kvcommon.exception.KvException;
import com.danieljhkim.kvdb.kvcommon.exception.NodeOperationException;
import com.danieljhkim.kvdb.kvcommon.exception.NodeUnavailableException;
import com.danieljhkim.kvdb.kvcommon.exception.ShardMapUnavailableException;
import com.danieljhkim.kvdb.kvgateway.cache.ShardMapCache;
import com.danieljhkim.kvdb.kvgateway.retry.RequestExecutor;
import com.danieljhkim.kvdb.kvgateway.retry.RequestExecutor.ExecutionResult;
import com.danieljhkim.kvdb.proto.coordinator.NodeRecord;
import com.danieljhkim.kvdb.proto.coordinator.NodeStatus;
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
import com.kvdb.proto.kvstore.KeyRequest;
import com.kvdb.proto.kvstore.KeyValueRequest;
import com.kvdb.proto.kvstore.SetResponse;
import com.kvdb.proto.kvstore.ValueResponse;

import io.grpc.stub.StreamObserver;

/**
 * gRPC service implementation for the KvGateway.
 * Handles Get, Put, Delete operations by routing to appropriate storage nodes
 * with retry logic and cache invalidation.
 */
public class KvGatewayServiceImpl extends KvGatewayGrpc.KvGatewayImplBase {

	private final ShardMapCache shardMapCache;
	private final RequestExecutor requestExecutor;

	public KvGatewayServiceImpl(ShardMapCache shardMapCache, RequestExecutor requestExecutor) {
		this.shardMapCache = shardMapCache;
		this.requestExecutor = requestExecutor;
	}

	@Override
	public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
		try {
			// Validate request
			if (request.getKey().isEmpty()) {
				throw new InvalidRequestException("Key cannot be empty");
			}

			byte[] keyBytes = request.getKey().toByteArray();
			String keyStr = new String(keyBytes, StandardCharsets.UTF_8);

			// Resolve shard
			final String shardId = resolveShardId(keyBytes);
			Consistency consistency = request.getOptions().getConsistency();

			// Build the node request
			KeyRequest nodeRequest = KeyRequest.newBuilder()
					.setKey(keyStr)
					.build();

			// Execute with retry
			ExecutionResult<ValueResponse> result = requestExecutor.executeWithRetry(
					shardId,
					false,
					stub -> stub.get(nodeRequest),
					() -> getNodesForRead(shardId, consistency));

			// Handle result
			if (!result.isSuccess()) {
				throw new NodeUnavailableException(
						result.getErrorMessage(), shardId, result.getErrorCode());
			}

			ValueResponse nodeResponse = result.getResponse();
			if (nodeResponse.getValue().isEmpty()) {
				throw new KeyNotFoundException(keyStr, shardId);
			}

			// Success response
			responseObserver.onNext(GetResponse.newBuilder()
					.setStatus(okStatus(shardId))
					.setKv(KeyValue.newBuilder()
							.setKey(request.getKey())
							.setValue(com.google.protobuf.ByteString.copyFromUtf8(nodeResponse.getValue()))
							.build())
					.build());
			responseObserver.onCompleted();

		} catch (KvException e) {
			responseObserver.onNext(GetResponse.newBuilder()
					.setStatus(exceptionToStatus(e))
					.build());
			responseObserver.onCompleted();
		}
	}

	@Override
	public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
		try {
			// Validate request
			if (request.getKey().isEmpty()) {
				throw new InvalidRequestException("Key cannot be empty");
			}

			byte[] keyBytes = request.getKey().toByteArray();
			String keyStr = new String(keyBytes, StandardCharsets.UTF_8);
			String valueStr = request.getValue().toStringUtf8();

			// Resolve shard
			final String shardId = resolveShardId(keyBytes);

			// Build the node request
			KeyValueRequest nodeRequest = KeyValueRequest.newBuilder()
					.setKey(keyStr)
					.setValue(valueStr)
					.build();

			// Execute with retry
			ExecutionResult<SetResponse> result = requestExecutor.executeWithRetry(
					shardId,
					true,
					stub -> stub.set(nodeRequest),
					() -> getNodesForWrite(shardId));

			// Handle result
			if (!result.isSuccess()) {
				throw new NodeUnavailableException(
						result.getErrorMessage(), shardId, result.getErrorCode());
			}

			SetResponse nodeResponse = result.getResponse();
			if (!nodeResponse.getSuccess()) {
				throw new NodeOperationException("Put operation failed on storage node", shardId);
			}

			// Success response
			responseObserver.onNext(PutResponse.newBuilder()
					.setStatus(okStatus(shardId))
					.setVersion(1)
					.build());
			responseObserver.onCompleted();

		} catch (KvException e) {
			responseObserver.onNext(PutResponse.newBuilder()
					.setStatus(exceptionToStatus(e))
					.build());
			responseObserver.onCompleted();
		}
	}

	@Override
	public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
		try {
			// Validate request
			if (request.getKey().isEmpty()) {
				throw new InvalidRequestException("Key cannot be empty");
			}

			byte[] keyBytes = request.getKey().toByteArray();
			String keyStr = new String(keyBytes, StandardCharsets.UTF_8);

			// Resolve shard
			final String shardId = resolveShardId(keyBytes);

			// Build the node request
			com.kvdb.proto.kvstore.DeleteRequest nodeRequest = com.kvdb.proto.kvstore.DeleteRequest.newBuilder()
					.setKey(keyStr)
					.build();

			// Execute with retry
			ExecutionResult<com.kvdb.proto.kvstore.DeleteResponse> result = requestExecutor.executeWithRetry(
					shardId,
					true,
					stub -> stub.delete(nodeRequest),
					() -> getNodesForWrite(shardId));

			// Handle result
			if (!result.isSuccess()) {
				throw new NodeUnavailableException(
						result.getErrorMessage(), shardId, result.getErrorCode());
			}

			com.kvdb.proto.kvstore.DeleteResponse nodeResponse = result.getResponse();
			if (!nodeResponse.getSuccess()) {
				throw new KeyNotFoundException(keyStr, shardId);
			}

			// Success response
			responseObserver.onNext(DeleteResponse.newBuilder()
					.setStatus(okStatus(shardId))
					.setVersion(1)
					.build());
			responseObserver.onCompleted();

		} catch (KvException e) {
			responseObserver.onNext(DeleteResponse.newBuilder()
					.setStatus(exceptionToStatus(e))
					.build());
			responseObserver.onCompleted();
		}
	}

	// ========== Helper Methods ==========

	/**
	 * Resolves shard ID, converting IllegalStateException to
	 * ShardMapUnavailableException.
	 */
	private String resolveShardId(byte[] keyBytes) {
		try {
			return shardMapCache.resolveShardId(keyBytes);
		} catch (IllegalStateException e) {
			throw new ShardMapUnavailableException("Shard map not available: " + e.getMessage(), e);
		}
	}

	/**
	 * Creates an OK status with optional shard ID.
	 */
	private Status okStatus(String shardId) {
		Status.Builder builder = Status.newBuilder().setCode(Status.Code.OK);
		if (shardId != null) {
			builder.setShardId(shardId);
		}
		return builder.build();
	}

	/**
	 * Converts a KvException to a gateway Status proto.
	 */
	private Status exceptionToStatus(KvException e) {
		Status.Builder builder = Status.newBuilder()
				.setCode(mapGrpcCodeToStatusCode(e.getGrpcStatusCode()))
				.setMessage(e.getMessage());

		if (e.getShardId() != null) {
			builder.setShardId(e.getShardId());
		}

		return builder.build();
	}

	/**
	 * Maps gRPC status codes to gateway Status.Code.
	 */
	private Status.Code mapGrpcCodeToStatusCode(io.grpc.Status.Code grpcCode) {
		if (grpcCode == null) {
			return Status.Code.INTERNAL;
		}
		return switch (grpcCode) {
			case OK -> Status.Code.OK;
			case NOT_FOUND -> Status.Code.NOT_FOUND;
			case INVALID_ARGUMENT -> Status.Code.INVALID_ARGUMENT;
			case ALREADY_EXISTS -> Status.Code.ALREADY_EXISTS;
			case FAILED_PRECONDITION -> Status.Code.PRECONDITION_FAILED;
			case RESOURCE_EXHAUSTED -> Status.Code.RATE_LIMITED;
			case UNAVAILABLE -> Status.Code.UNAVAILABLE;
			case DEADLINE_EXCEEDED -> Status.Code.TIMEOUT;
			case CANCELLED -> Status.Code.UNAVAILABLE;
			default -> Status.Code.INTERNAL;
		};
	}

	/**
	 * Gets candidate nodes for a read operation.
	 */
	private List<NodeRecord> getNodesForRead(String shardId, Consistency consistency) {
		List<NodeRecord> candidates = new ArrayList<>();

		if (consistency == Consistency.STRONG) {
			NodeRecord leader = shardMapCache.getLeader(shardId);
			if (leader != null && leader.getStatus() == NodeStatus.ALIVE) {
				candidates.add(leader);
			}
			for (NodeRecord replica : shardMapCache.getReplicas(shardId)) {
				if (replica.getStatus() == NodeStatus.ALIVE && !candidates.contains(replica)) {
					candidates.add(replica);
				}
			}
		} else {
			for (NodeRecord replica : shardMapCache.getReplicas(shardId)) {
				if (replica.getStatus() == NodeStatus.ALIVE) {
					candidates.add(replica);
				}
			}
			NodeRecord leader = shardMapCache.getLeader(shardId);
			if (leader != null && leader.getStatus() == NodeStatus.ALIVE && !candidates.contains(leader)) {
				candidates.add(leader);
			}
		}

		return candidates;
	}

	/**
	 * Gets candidate nodes for a write operation.
	 */
	private List<NodeRecord> getNodesForWrite(String shardId) {
		List<NodeRecord> candidates = new ArrayList<>();

		NodeRecord leader = shardMapCache.getLeader(shardId);
		if (leader != null) {
			candidates.add(leader);
		}

		if (candidates.isEmpty()) {
			for (NodeRecord replica : shardMapCache.getReplicas(shardId)) {
				if (replica.getStatus() == NodeStatus.ALIVE) {
					candidates.add(replica);
				}
			}
		}

		return candidates;
	}
}
