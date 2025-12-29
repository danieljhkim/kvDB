package com.danieljhkim.kvdb.kvadmin.middleware;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import com.danieljhkim.kvdb.kvadmin.api.dto.ErrorDto;
import com.danieljhkim.kvdb.kvcommon.exception.CodeRedException;
import com.danieljhkim.kvdb.kvcommon.exception.DatabaseException;
import com.danieljhkim.kvdb.kvcommon.exception.InvalidRequestException;
import com.danieljhkim.kvdb.kvcommon.exception.KeyNotFoundException;
import com.danieljhkim.kvdb.kvcommon.exception.KvException;
import com.danieljhkim.kvdb.kvcommon.exception.NoHealthyNodesAvailable;
import com.danieljhkim.kvdb.kvcommon.exception.NodeOperationException;
import com.danieljhkim.kvdb.kvcommon.exception.NodeUnavailableException;
import com.danieljhkim.kvdb.kvcommon.exception.NotLeaderException;
import com.danieljhkim.kvdb.kvcommon.exception.ServerException;
import com.danieljhkim.kvdb.kvcommon.exception.ShardMapUnavailableException;
import com.danieljhkim.kvdb.kvcommon.exception.ShardMovedException;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;

/**
 * Global exception handler that maps domain exceptions to HTTP status codes and
 * error responses.
 */
@RestControllerAdvice
@Slf4j
public class ExceptionMapper {

	@ExceptionHandler(IllegalArgumentException.class)
	public ResponseEntity<ErrorDto> handleIllegalArgument(IllegalArgumentException e) {
		log.warn("Illegal argument: {}", e.getMessage());
		ErrorDto error = ErrorDto.builder()
				.error("INVALID_ARGUMENT")
				.message(e.getMessage())
				.code(String.valueOf(HttpStatus.BAD_REQUEST.value()))
				.timestampMs(System.currentTimeMillis())
				.build();
		return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(error);
	}

	@ExceptionHandler(KvException.class)
	public ResponseEntity<ErrorDto> handleKvException(KvException e) {
		log.warn("KvException: {}", e.getMessage());
		HttpStatus httpStatus = mapGrpcStatusToHttp(e.getGrpcStatusCode());

		ErrorDto.ErrorDtoBuilder errorBuilder = ErrorDto.builder()
				.error(e.getClass().getSimpleName())
				.message(e.getMessage())
				.code(String.valueOf(httpStatus.value()))
				.timestampMs(System.currentTimeMillis())
				.shardId(e.getShardId());

		// Add routing hints for specific exception types
		if (e instanceof NotLeaderException notLeaderEx) {
			errorBuilder.leaderHint(notLeaderEx.getLeaderHint());
		} else if (e instanceof ShardMovedException shardMovedEx) {
			errorBuilder.newNodeHint(shardMovedEx.getNewNodeHint());
		}

		return ResponseEntity.status(httpStatus).body(errorBuilder.build());
	}

	@ExceptionHandler(InvalidRequestException.class)
	public ResponseEntity<ErrorDto> handleInvalidRequest(InvalidRequestException e) {
		return handleKvException(e);
	}

	@ExceptionHandler(KeyNotFoundException.class)
	public ResponseEntity<ErrorDto> handleKeyNotFound(KeyNotFoundException e) {
		return handleKvException(e);
	}

	@ExceptionHandler(NodeUnavailableException.class)
	public ResponseEntity<ErrorDto> handleNodeUnavailable(NodeUnavailableException e) {
		return handleKvException(e);
	}

	@ExceptionHandler(NoHealthyNodesAvailable.class)
	public ResponseEntity<ErrorDto> handleNoHealthyNodes(NoHealthyNodesAvailable e) {
		// NoHealthyNodesAvailable extends CodeRedException, not KvException
		log.error("No healthy nodes available", e);
		ErrorDto error = ErrorDto.builder()
				.error("NO_HEALTHY_NODES")
				.message(e.getMessage())
				.code(String.valueOf(HttpStatus.SERVICE_UNAVAILABLE.value()))
				.timestampMs(System.currentTimeMillis())
				.build();
		return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(error);
	}

	@ExceptionHandler(NotLeaderException.class)
	public ResponseEntity<ErrorDto> handleNotLeader(NotLeaderException e) {
		// NotLeaderException maps to 503 (Service Unavailable) with leader hint
		log.warn("NotLeaderException: leader hint = {}", e.getLeaderHint());
		ErrorDto error = ErrorDto.builder()
				.error("NOT_LEADER")
				.message(e.getMessage())
				.code(String.valueOf(HttpStatus.SERVICE_UNAVAILABLE.value()))
				.timestampMs(System.currentTimeMillis())
				.shardId(e.getShardId())
				.leaderHint(e.getLeaderHint())
				.build();
		return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(error);
	}

	@ExceptionHandler(ShardMapUnavailableException.class)
	public ResponseEntity<ErrorDto> handleShardMapUnavailable(ShardMapUnavailableException e) {
		return handleKvException(e);
	}

	@ExceptionHandler(ShardMovedException.class)
	public ResponseEntity<ErrorDto> handleShardMoved(ShardMovedException e) {
		// ShardMovedException maps to 410 Gone or 503 with new node hint
		log.warn("ShardMovedException: new node hint = {}", e.getNewNodeHint());
		ErrorDto error = ErrorDto.builder()
				.error("SHARD_MOVED")
				.message(e.getMessage())
				.code(String.valueOf(HttpStatus.GONE.value()))
				.timestampMs(System.currentTimeMillis())
				.shardId(e.getShardId())
				.newNodeHint(e.getNewNodeHint())
				.build();
		return ResponseEntity.status(HttpStatus.GONE).body(error);
	}

	@ExceptionHandler(NodeOperationException.class)
	public ResponseEntity<ErrorDto> handleNodeOperation(NodeOperationException e) {
		return handleKvException(e);
	}

	@ExceptionHandler(ServerException.class)
	public ResponseEntity<ErrorDto> handleServerException(ServerException e) {
		log.error("ServerException", e);
		ErrorDto error = ErrorDto.builder()
				.error("SERVER_ERROR")
				.message(e.getMessage())
				.code(String.valueOf(HttpStatus.INTERNAL_SERVER_ERROR.value()))
				.timestampMs(System.currentTimeMillis())
				.build();
		return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
	}

	@ExceptionHandler(CodeRedException.class)
	public ResponseEntity<ErrorDto> handleCodeRed(CodeRedException e) {
		log.error("CODE RED - Critical failure", e);
		ErrorDto error = ErrorDto.builder()
				.error("CODE_RED")
				.message(e.getMessage())
				.code(String.valueOf(HttpStatus.SERVICE_UNAVAILABLE.value()))
				.timestampMs(System.currentTimeMillis())
				.build();
		return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(error);
	}

	@ExceptionHandler(DatabaseException.class)
	public ResponseEntity<ErrorDto> handleDatabaseException(DatabaseException e) {
		log.error("DatabaseException", e);
		ErrorDto error = ErrorDto.builder()
				.error("DATABASE_ERROR")
				.message(e.getMessage())
				.code(String.valueOf(HttpStatus.INTERNAL_SERVER_ERROR.value()))
				.timestampMs(System.currentTimeMillis())
				.build();
		return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
	}

	@ExceptionHandler(StatusRuntimeException.class)
	public ResponseEntity<ErrorDto> handleGrpcException(StatusRuntimeException e) {
		log.error("gRPC error", e);
		HttpStatus httpStatus = mapGrpcStatusToHttp(e.getStatus().getCode());
		ErrorDto error = ErrorDto.builder()
				.error("GRPC_ERROR")
				.message(e.getMessage())
				.code(String.valueOf(httpStatus.value()))
				.timestampMs(System.currentTimeMillis())
				.build();
		return ResponseEntity.status(httpStatus).body(error);
	}

	@ExceptionHandler(Exception.class)
	public ResponseEntity<ErrorDto> handleGenericException(Exception e) {
		log.error("Unexpected error", e);
		ErrorDto error = ErrorDto.builder()
				.error("INTERNAL_ERROR")
				.message(e.getMessage())
				.code(String.valueOf(HttpStatus.INTERNAL_SERVER_ERROR.value()))
				.timestampMs(System.currentTimeMillis())
				.build();
		return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
	}

	private HttpStatus mapGrpcStatusToHttp(Status.Code grpcCode) {
		return switch (grpcCode) {
			case NOT_FOUND -> HttpStatus.NOT_FOUND;
			case INVALID_ARGUMENT -> HttpStatus.BAD_REQUEST;
			case ALREADY_EXISTS -> HttpStatus.CONFLICT;
			case PERMISSION_DENIED -> HttpStatus.FORBIDDEN;
			case FAILED_PRECONDITION -> HttpStatus.PRECONDITION_FAILED;
			case UNAVAILABLE -> HttpStatus.SERVICE_UNAVAILABLE;
			case DEADLINE_EXCEEDED -> HttpStatus.GATEWAY_TIMEOUT;
			case RESOURCE_EXHAUSTED -> HttpStatus.TOO_MANY_REQUESTS;
			case INTERNAL -> HttpStatus.INTERNAL_SERVER_ERROR;
			default -> HttpStatus.INTERNAL_SERVER_ERROR;
		};
	}
}
