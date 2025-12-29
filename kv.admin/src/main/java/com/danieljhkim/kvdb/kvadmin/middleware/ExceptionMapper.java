package com.danieljhkim.kvdb.kvadmin.middleware;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import com.danieljhkim.kvdb.kvadmin.api.dto.ErrorDto;

import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;

/**
 * Global exception handler that maps domain exceptions to HTTP status codes and error responses.
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

	private HttpStatus mapGrpcStatusToHttp(io.grpc.Status.Code grpcCode) {
		return switch (grpcCode) {
			case NOT_FOUND -> HttpStatus.NOT_FOUND;
			case INVALID_ARGUMENT -> HttpStatus.BAD_REQUEST;
			case ALREADY_EXISTS -> HttpStatus.CONFLICT;
			case PERMISSION_DENIED -> HttpStatus.FORBIDDEN;
			case UNAVAILABLE -> HttpStatus.SERVICE_UNAVAILABLE;
			case DEADLINE_EXCEEDED -> HttpStatus.REQUEST_TIMEOUT;
			default -> HttpStatus.INTERNAL_SERVER_ERROR;
		};
	}
}

