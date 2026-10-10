package com.danieljhkim.kvdb.kvadmin.middleware;

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
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.MessageSourceResolvable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.FieldError;
import org.springframework.validation.ObjectError;
import org.springframework.validation.method.ParameterErrors;
import org.springframework.validation.method.ParameterValidationResult;
import org.springframework.web.HttpMediaTypeNotSupportedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.HandlerMethodValidationException;

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

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ErrorDto> handleUnreadableMessage(HttpMessageNotReadableException e) {
        UnrecognizedPropertyException unknown = findCause(e, UnrecognizedPropertyException.class);
        if (unknown != null) {
            String field = unknown.getPropertyName() == null ? "unknown" : unknown.getPropertyName();
            log.warn("Rejected unknown JSON field: {}", field);
            return clientError(HttpStatus.BAD_REQUEST, "UNKNOWN_FIELD", "Unknown field: " + field);
        }
        if (findCause(e, JsonParseException.class) != null) {
            log.warn("Rejected malformed JSON request body");
            return clientError(HttpStatus.BAD_REQUEST, "MALFORMED_JSON", "Request body is not valid JSON");
        }
        log.warn(
                "Rejected unreadable request body ({})",
                e.getMostSpecificCause().getClass().getSimpleName());
        return clientError(
                HttpStatus.BAD_REQUEST, "INVALID_REQUEST", "Request body does not match the expected schema");
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorDto> handleMethodArgumentNotValid(MethodArgumentNotValidException e) {
        log.warn("Request validation failed");
        List<String> messages = new ArrayList<>();
        messages.addAll(fieldMessages(e.getBindingResult().getFieldErrors()));
        for (ObjectError error : e.getBindingResult().getGlobalErrors()) {
            messages.add(safeClientMessage(error.getDefaultMessage()));
        }
        return clientError(HttpStatus.BAD_REQUEST, "VALIDATION_ERROR", joinMessages(messages));
    }

    @ExceptionHandler(HandlerMethodValidationException.class)
    public ResponseEntity<ErrorDto> handleHandlerMethodValidation(HandlerMethodValidationException e) {
        log.warn("Request validation failed");
        List<String> messages = new ArrayList<>();
        for (ParameterValidationResult result : e.getParameterValidationResults()) {
            if (result instanceof ParameterErrors errors) {
                messages.addAll(fieldMessages(errors.getFieldErrors()));
                for (ObjectError error : errors.getGlobalErrors()) {
                    messages.add(safeClientMessage(error.getDefaultMessage()));
                }
            } else {
                for (MessageSourceResolvable resolvable : result.getResolvableErrors()) {
                    messages.add(safeClientMessage(resolvable.getDefaultMessage()));
                }
            }
        }
        return clientError(HttpStatus.BAD_REQUEST, "VALIDATION_ERROR", joinMessages(messages));
    }

    @ExceptionHandler(ConstraintViolationException.class)
    public ResponseEntity<ErrorDto> handleConstraintViolation(ConstraintViolationException e) {
        log.warn("Request validation failed");
        List<String> messages = new ArrayList<>();
        for (ConstraintViolation<?> violation : e.getConstraintViolations()) {
            String path = violation.getPropertyPath() == null
                    ? "request"
                    : violation.getPropertyPath().toString();
            int dot = path.lastIndexOf('.');
            String field = dot >= 0 ? path.substring(dot + 1) : path;
            messages.add(wireName(field) + ": " + safeClientMessage(violation.getMessage()));
        }
        return clientError(HttpStatus.BAD_REQUEST, "VALIDATION_ERROR", joinMessages(messages));
    }

    @ExceptionHandler(HttpMediaTypeNotSupportedException.class)
    public ResponseEntity<ErrorDto> handleUnsupportedMediaType(HttpMediaTypeNotSupportedException e) {
        log.warn("Rejected unsupported media type");
        return clientError(
                HttpStatus.UNSUPPORTED_MEDIA_TYPE, "UNSUPPORTED_MEDIA_TYPE", "Content-Type must be application/json");
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

    private ResponseEntity<ErrorDto> clientError(HttpStatus status, String errorCode, String message) {
        ErrorDto error = ErrorDto.builder()
                .error(errorCode)
                .message(message)
                .code(String.valueOf(status.value()))
                .timestampMs(System.currentTimeMillis())
                .build();
        return ResponseEntity.status(status).body(error);
    }

    private static List<String> fieldMessages(List<FieldError> errors) {
        List<String> messages = new ArrayList<>();
        for (FieldError error : errors) {
            messages.add(wireName(error.getField()) + ": " + safeClientMessage(error.getDefaultMessage()));
        }
        return messages;
    }

    private static String joinMessages(List<String> messages) {
        List<String> cleaned = messages.stream()
                .filter(message -> message != null && !message.isBlank())
                .sorted()
                .toList();
        if (cleaned.isEmpty()) {
            return "Request validation failed";
        }
        return String.join("; ", cleaned);
    }

    /**
     * Bean Validation messages we author are safe to return. Anything that looks like a Java
     * exception, a stack frame, or a parser diagnostic is replaced.
     */
    private static String safeClientMessage(String message) {
        if (message == null || message.isBlank()) {
            return "is invalid";
        }
        String lower = message.toLowerCase(Locale.ROOT);
        if (message.contains("Exception")
                || lower.contains("java.")
                || lower.contains("com.fasterxml")
                || lower.contains("com.danieljhkim")
                || lower.contains("json parse error")
                || message.contains("toUpperCase")
                || message.contains("NullPointer")) {
            return "is invalid";
        }
        return message;
    }

    private static String wireName(String field) {
        if (field == null || field.isBlank()) {
            return "request";
        }
        StringBuilder out = new StringBuilder(field.length() + 8);
        for (int i = 0; i < field.length(); i++) {
            char c = field.charAt(i);
            if (Character.isUpperCase(c)) {
                if (i > 0 && field.charAt(i - 1) != '.' && field.charAt(i - 1) != '_') {
                    out.append('_');
                }
                out.append(Character.toLowerCase(c));
            } else {
                out.append(c);
            }
        }
        return out.toString();
    }

    private static <T extends Throwable> T findCause(Throwable error, Class<T> type) {
        Throwable current = error;
        while (current != null) {
            if (type.isInstance(current)) {
                return type.cast(current);
            }
            Throwable next = current.getCause();
            if (next == current) {
                break;
            }
            current = next;
        }
        return null;
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
