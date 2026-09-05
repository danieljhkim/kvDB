package com.danieljhkim.kvdb.kvgateway.service;

import com.danieljhkim.kvdb.kvcommon.cache.ShardMapCache;
import com.danieljhkim.kvdb.kvcommon.exception.InvalidRequestException;
import com.danieljhkim.kvdb.kvcommon.exception.KeyNotFoundException;
import com.danieljhkim.kvdb.kvcommon.exception.KvException;
import com.danieljhkim.kvdb.kvcommon.exception.NodeUnavailableException;
import com.danieljhkim.kvdb.kvcommon.exception.PayloadTooLargeException;
import com.danieljhkim.kvdb.kvcommon.exception.ShardMapUnavailableException;
import com.danieljhkim.kvdb.kvcommon.grpc.GrpcIdentity;
import com.danieljhkim.kvdb.kvcommon.grpc.GrpcPeerIdentity;
import com.danieljhkim.kvdb.kvcommon.limits.KvRequestLimits;
import com.danieljhkim.kvdb.kvgateway.retry.RequestExecutor;
import com.danieljhkim.kvdb.kvgateway.retry.RequestExecutor.ExecutionResult;
import com.danieljhkim.kvdb.proto.coordinator.NodeRecord;
import com.danieljhkim.kvdb.proto.coordinator.NodeStatus;
import com.danieljhkim.kvdb.proto.gateway.BatchGetOutcome;
import com.danieljhkim.kvdb.proto.gateway.BatchGetRequest;
import com.danieljhkim.kvdb.proto.gateway.BatchGetResponse;
import com.danieljhkim.kvdb.proto.gateway.BatchGetResult;
import com.danieljhkim.kvdb.proto.gateway.Consistency;
import com.danieljhkim.kvdb.proto.gateway.DeleteRequest;
import com.danieljhkim.kvdb.proto.gateway.DeleteResponse;
import com.danieljhkim.kvdb.proto.gateway.GetRequest;
import com.danieljhkim.kvdb.proto.gateway.GetResponse;
import com.danieljhkim.kvdb.proto.gateway.KeyValue;
import com.danieljhkim.kvdb.proto.gateway.KvGatewayGrpc;
import com.danieljhkim.kvdb.proto.gateway.PutRequest;
import com.danieljhkim.kvdb.proto.gateway.PutResponse;
import com.danieljhkim.kvdb.proto.gateway.ReadMode;
import com.danieljhkim.kvdb.proto.gateway.ReadOptions;
import com.danieljhkim.kvdb.proto.gateway.Status;
import com.danieljhkim.kvdb.proto.gateway.WriteDurability;
import com.danieljhkim.kvdb.proto.gateway.WriteOptions;
import com.kvdb.proto.kvstore.KeyRequest;
import com.kvdb.proto.kvstore.KeyValueRequest;
import com.kvdb.proto.kvstore.MutationOutcome;
import com.kvdb.proto.kvstore.SetResponse;
import com.kvdb.proto.kvstore.ValueResponse;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * gRPC service implementation for the KvGateway. Handles Get, Put, Delete operations by routing to appropriate storage
 * nodes with retry logic and cache invalidation.
 */
public class KvGatewayServiceImpl extends KvGatewayGrpc.KvGatewayImplBase {

    private static final Logger logger = LoggerFactory.getLogger(KvGatewayServiceImpl.class);

    private final ShardMapCache shardMapCache;
    private final RequestExecutor requestExecutor;
    private final KvRequestLimits limits;
    private final ExecutorService batchExecutor;

    public KvGatewayServiceImpl(ShardMapCache shardMapCache, RequestExecutor requestExecutor) {
        this(shardMapCache, requestExecutor, new KvRequestLimits(null));
    }

    public KvGatewayServiceImpl(ShardMapCache shardMapCache, RequestExecutor requestExecutor, KvRequestLimits limits) {
        this.shardMapCache = shardMapCache;
        this.requestExecutor = requestExecutor;
        this.limits = limits;
        this.batchExecutor = Executors.newFixedThreadPool(
                limits.maxBatchGetConcurrency(),
                Thread.ofVirtual().name("batch-get-", 0).factory());
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        responseObserver.onNext(read(request));
        responseObserver.onCompleted();
    }

    @Override
    public void batchGet(BatchGetRequest request, StreamObserver<BatchGetResponse> responseObserver) {
        try {
            limits.validateMessage(request);
            limits.validateBatchKeys(request.getKeysList());
            validateReadOptions(request.getOptions());

            List<BatchGetResult> reservationResults = request.getKeysList().stream()
                    .map(KvGatewayServiceImpl::responseReservationResult)
                    .toList();
            BatchGetResponse reservationOnly = BatchGetResponse.newBuilder()
                    .setStatus(okStatus(null))
                    .addAllResults(reservationResults)
                    .build();
            if (reservationOnly.getSerializedSize() > limits.maxBatchGetResponseBytes()) {
                throw new PayloadTooLargeException(
                        "BatchGet response budget cannot represent one outcome per input key");
            }

            io.grpc.Context context = io.grpc.Context.current();
            BatchGetResult initiallyStopped = stoppedBatchResult(request.getKeys(0), context);
            if (initiallyStopped != null) {
                BatchGetResponse.Builder stoppedResponse =
                        BatchGetResponse.newBuilder().setStatus(okStatus(null));
                appendStoppedResults(
                        stoppedResponse, request, 0, initiallyStopped.getOutcome(), initiallyStopped.getStatus());
                responseObserver.onNext(stoppedResponse.build());
                responseObserver.onCompleted();
                return;
            }
            List<Future<GetResponse>> futures =
                    new CopyOnWriteArrayList<>(Collections.nCopies(request.getKeysCount(), null));
            int initialReads = Math.min(request.getKeysCount(), limits.maxBatchGetConcurrency());
            for (int index = 0; index < initialReads; index++) {
                submitBatchRead(request, futures, context, index);
            }

            io.grpc.Context.CancellationListener cancellationListener = ignored -> cancelAll(futures);
            context.addListener(cancellationListener, Runnable::run);
            BatchGetResponse response;
            try {
                response = collectBatchResponse(request, futures, reservationResults, context);
            } finally {
                context.removeListener(cancellationListener);
                cancelAll(futures);
            }
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (KvException e) {
            responseObserver.onNext(BatchGetResponse.newBuilder()
                    .setStatus(exceptionToStatus(e))
                    .build());
            responseObserver.onCompleted();
        }
    }

    private GetResponse read(GetRequest request) {
        try {
            limits.validateMessage(request);
            limits.validateKey(request.getKey());
            validateReadOptions(request.getOptions());
            byte[] keyBytes = request.getKey().toByteArray();
            final String shardId = resolveShardId(keyBytes);
            Consistency consistency = normalizedConsistency(request.getOptions().getConsistency());
            KeyRequest nodeRequest = KeyRequest.newBuilder()
                    .setKey(request.getKey())
                    .setRequireLeader(consistency == Consistency.STRONG)
                    .setHeadOnly(request.getHeadOnly())
                    .build();

            ExecutionResult<ValueResponse> result = requestExecutor.executeWithRetry(
                    shardId, false, true, stub -> stub.get(nodeRequest), () -> getNodesForRead(shardId, consistency));
            if (!result.isSuccess()) {
                throw new NodeUnavailableException(result.getErrorMessage(), shardId, result.getErrorCode());
            }
            ValueResponse nodeResponse = result.getResponse();
            if (!nodeResponse.getFound()) {
                throw new KeyNotFoundException(printableKey(request.getKey()), shardId);
            }
            return GetResponse.newBuilder()
                    .setStatus(okStatus(shardId))
                    .setKv(KeyValue.newBuilder()
                            .setKey(request.getKey())
                            .setValue(nodeResponse.getValue())
                            .setVersion(nodeResponse.getVersion())
                            .setCreateTimeMs(nodeResponse.getCreateTimeMs())
                            .setUpdateTimeMs(nodeResponse.getUpdateTimeMs())
                            .setExpireTimeMs(nodeResponse.getExpireTimeMs())
                            .build())
                    .setAppliedVersion(nodeResponse.getAppliedVersion())
                    .build();
        } catch (KvException e) {
            return GetResponse.newBuilder().setStatus(exceptionToStatus(e)).build();
        }
    }

    private BatchGetResponse collectBatchResponse(
            BatchGetRequest request,
            List<Future<GetResponse>> futures,
            List<BatchGetResult> reservationResults,
            io.grpc.Context context) {
        BatchGetResponse.Builder response = BatchGetResponse.newBuilder().setStatus(okStatus(null));
        for (int index = 0; index < futures.size(); index++) {
            BatchGetResult stopped = stoppedBatchResult(request.getKeys(index), context);
            if (stopped != null) {
                cancelAll(futures.subList(index, futures.size()));
                appendStoppedResults(response, request, index, stopped.getOutcome(), stopped.getStatus());
                break;
            }

            BatchGetResult candidate;
            try {
                GetResponse itemResponse = await(futures.get(index), context);
                candidate = completedBatchResult(request.getKeys(index), itemResponse);
            } catch (TimeoutException e) {
                cancelAll(futures.subList(index, futures.size()));
                appendStoppedResults(
                        response,
                        request,
                        index,
                        BatchGetOutcome.DEADLINE_EXCEEDED,
                        Status.newBuilder()
                                .setCode(Status.Code.TIMEOUT)
                                .setMessage("BatchGet deadline exceeded")
                                .build());
                break;
            } catch (CancellationException e) {
                BatchGetResult cancelled = stoppedBatchResult(request.getKeys(index), context);
                Status status = cancelled == null
                        ? Status.newBuilder()
                                .setCode(Status.Code.CANCELLED)
                                .setMessage("BatchGet item was cancelled")
                                .build()
                        : cancelled.getStatus();
                BatchGetOutcome outcome = cancelled == null ? BatchGetOutcome.CANCELLED : cancelled.getOutcome();
                appendStoppedResults(response, request, index, outcome, status);
                break;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                cancelAll(futures.subList(index, futures.size()));
                appendStoppedResults(
                        response,
                        request,
                        index,
                        BatchGetOutcome.CANCELLED,
                        Status.newBuilder()
                                .setCode(Status.Code.CANCELLED)
                                .setMessage("BatchGet interrupted")
                                .build());
                break;
            } catch (ExecutionException e) {
                candidate = BatchGetResult.newBuilder()
                        .setKey(request.getKeys(index))
                        .setStatus(Status.newBuilder()
                                .setCode(Status.Code.INTERNAL)
                                .setMessage("BatchGet item failed internally"))
                        .setOutcome(BatchGetOutcome.COMPLETED)
                        .build();
            }

            BatchGetResponse prospective = response.clone()
                    .addResults(candidate)
                    .addAllResults(reservationResults.subList(index + 1, reservationResults.size()))
                    .build();
            if (prospective.getSerializedSize() <= limits.maxBatchGetResponseBytes()) {
                response.addResults(candidate);
                int nextIndex = index + limits.maxBatchGetConcurrency();
                if (nextIndex < request.getKeysCount()) {
                    submitBatchRead(request, futures, context, nextIndex);
                }
                continue;
            }

            cancelAll(futures.subList(index, futures.size()));
            response.addAllResults(request.getKeysList().subList(index, request.getKeysCount()).stream()
                    .map(KvGatewayServiceImpl::responseBudgetResult)
                    .toList());
            break;
        }
        return response.build();
    }

    private void submitBatchRead(
            BatchGetRequest request, List<Future<GetResponse>> futures, io.grpc.Context context, int index) {
        GetRequest itemRequest = GetRequest.newBuilder()
                .setCtx(request.getCtx())
                .setKey(request.getKeys(index))
                .setOptions(request.getOptions())
                .setHeadOnly(request.getHeadOnly())
                .build();
        futures.set(index, batchExecutor.submit(() -> context.call(() -> read(itemRequest))));
    }

    private static GetResponse await(Future<GetResponse> future, io.grpc.Context context)
            throws InterruptedException, ExecutionException, TimeoutException {
        io.grpc.Deadline deadline = context.getDeadline();
        if (deadline == null) {
            return future.get();
        }
        long remainingNanos = deadline.timeRemaining(TimeUnit.NANOSECONDS);
        if (remainingNanos <= 0) {
            throw new TimeoutException("deadline expired");
        }
        return future.get(remainingNanos, TimeUnit.NANOSECONDS);
    }

    private static BatchGetResult completedBatchResult(com.google.protobuf.ByteString key, GetResponse response) {
        BatchGetResult.Builder result = BatchGetResult.newBuilder()
                .setKey(key)
                .setStatus(response.getStatus())
                .setAppliedVersion(response.getAppliedVersion())
                .setOutcome(BatchGetOutcome.COMPLETED);
        if (response.hasKv()) {
            result.setKv(response.getKv());
        }
        return result.build();
    }

    private static BatchGetResult stoppedBatchResult(com.google.protobuf.ByteString key, io.grpc.Context context) {
        io.grpc.Deadline deadline = context.getDeadline();
        if (deadline != null && deadline.isExpired()) {
            return BatchGetResult.newBuilder()
                    .setKey(key)
                    .setStatus(
                            Status.newBuilder().setCode(Status.Code.TIMEOUT).setMessage("BatchGet deadline exceeded"))
                    .setOutcome(BatchGetOutcome.DEADLINE_EXCEEDED)
                    .build();
        }
        if (context.isCancelled()) {
            return BatchGetResult.newBuilder()
                    .setKey(key)
                    .setStatus(
                            Status.newBuilder().setCode(Status.Code.CANCELLED).setMessage("BatchGet cancelled"))
                    .setOutcome(BatchGetOutcome.CANCELLED)
                    .build();
        }
        return null;
    }

    private static BatchGetResult responseBudgetResult(com.google.protobuf.ByteString key) {
        return BatchGetResult.newBuilder()
                .setKey(key)
                .setStatus(Status.newBuilder().setCode(Status.Code.PAYLOAD_TOO_LARGE))
                .setOutcome(BatchGetOutcome.RESPONSE_BUDGET_EXHAUSTED)
                .build();
    }

    private static BatchGetResult responseReservationResult(com.google.protobuf.ByteString key) {
        return BatchGetResult.newBuilder()
                .setKey(key)
                .setStatus(Status.newBuilder().setCode(Status.Code.TIMEOUT).setMessage("BatchGet deadline exceeded"))
                .setOutcome(BatchGetOutcome.DEADLINE_EXCEEDED)
                .build();
    }

    private static void appendStoppedResults(
            BatchGetResponse.Builder response,
            BatchGetRequest request,
            int start,
            BatchGetOutcome outcome,
            Status status) {
        for (int index = start; index < request.getKeysCount(); index++) {
            response.addResults(BatchGetResult.newBuilder()
                    .setKey(request.getKeys(index))
                    .setStatus(status)
                    .setOutcome(outcome));
        }
    }

    private static void cancelAll(List<? extends Future<?>> futures) {
        futures.stream().filter(java.util.Objects::nonNull).forEach(future -> future.cancel(true));
    }

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        try {
            limits.validateMessage(request);
            limits.validateKey(request.getKey());
            limits.validateValue(request.getValue());
            limits.validateWriteContext(request.getCtx());
            GrpcIdentity identity = GrpcPeerIdentity.require();
            byte[] keyBytes = request.getKey().toByteArray();
            final String shardId = resolveShardId(keyBytes);
            String requestId = requireWriteRequestId(request.getCtx().getRequestId());
            boolean replaySafe = request.getOptions().getRequireIdempotency();
            KeyValueRequest.Builder nodeRequest = KeyValueRequest.newBuilder()
                    .setKey(request.getKey())
                    .setValue(request.getValue())
                    .setRequestId(requestId)
                    .setDurability(nodeDurability(request.getOptions().getDurability()))
                    .setTtlMs(request.getOptions().getTtlMs())
                    .setIfNotExists(request.getOptions().getIfNotExists());
            if (request.getOptions().hasIfVersionEquals()) {
                nodeRequest.setIfVersionEquals(request.getOptions().getIfVersionEquals());
            }
            ExecutionResult<SetResponse> result = requestExecutor.executeWithRetry(
                    shardId, true, replaySafe, stub -> stub.set(nodeRequest.build()), () -> getNodesForWrite(shardId));
            if (!result.isSuccess()) {
                if (result.isAmbiguous()) {
                    responseObserver.onNext(PutResponse.newBuilder()
                            .setStatus(writeOutcomeUnknownStatus(shardId, result))
                            .build());
                    responseObserver.onCompleted();
                    return;
                }
                throw new NodeUnavailableException(result.getErrorMessage(), shardId, result.getErrorCode());
            }
            SetResponse nodeResponse = result.getResponse();
            if (!nodeResponse.getSuccess()) {
                responseObserver.onNext(PutResponse.newBuilder()
                        .setStatus(mutationStatus(shardId, nodeResponse.getOutcome(), nodeResponse.getMessage()))
                        .setVersion(nodeResponse.getVersion())
                        .build());
                responseObserver.onCompleted();
                return;
            }
            auditWrite("put", requestId, shardId, identity, request.getCtx().getTraceparent());
            responseObserver.onNext(PutResponse.newBuilder()
                    .setStatus(okStatus(shardId))
                    .setVersion(nodeResponse.getVersion())
                    .build());
            responseObserver.onCompleted();

        } catch (KvException e) {
            responseObserver.onNext(
                    PutResponse.newBuilder().setStatus(exceptionToStatus(e)).build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
        try {
            limits.validateMessage(request);
            limits.validateKey(request.getKey());
            limits.validateWriteContext(request.getCtx());
            validateDeleteOptions(request.getOptions());
            GrpcIdentity identity = GrpcPeerIdentity.require();
            byte[] keyBytes = request.getKey().toByteArray();
            final String shardId = resolveShardId(keyBytes);
            String requestId = requireWriteRequestId(request.getCtx().getRequestId());
            boolean replaySafe = request.getOptions().getRequireIdempotency();
            com.kvdb.proto.kvstore.DeleteRequest.Builder nodeRequest = com.kvdb.proto.kvstore.DeleteRequest.newBuilder()
                    .setKey(request.getKey())
                    .setRequestId(requestId)
                    .setDurability(nodeDurability(request.getOptions().getDurability()));
            if (request.getOptions().hasIfVersionEquals()) {
                nodeRequest.setIfVersionEquals(request.getOptions().getIfVersionEquals());
            }
            ExecutionResult<com.kvdb.proto.kvstore.DeleteResponse> result = requestExecutor.executeWithRetry(
                    shardId,
                    true,
                    replaySafe,
                    stub -> stub.delete(nodeRequest.build()),
                    () -> getNodesForWrite(shardId));

            if (!result.isSuccess()) {
                if (result.isAmbiguous()) {
                    responseObserver.onNext(DeleteResponse.newBuilder()
                            .setStatus(writeOutcomeUnknownStatus(shardId, result))
                            .build());
                    responseObserver.onCompleted();
                    return;
                }
                throw new NodeUnavailableException(result.getErrorMessage(), shardId, result.getErrorCode());
            }
            com.kvdb.proto.kvstore.DeleteResponse nodeResponse = result.getResponse();
            if (!nodeResponse.getSuccess()) {
                responseObserver.onNext(DeleteResponse.newBuilder()
                        .setStatus(mutationStatus(shardId, nodeResponse.getOutcome(), nodeResponse.getMessage()))
                        .setVersion(nodeResponse.getVersion())
                        .build());
                responseObserver.onCompleted();
                return;
            }

            auditWrite("delete", requestId, shardId, identity, request.getCtx().getTraceparent());

            responseObserver.onNext(DeleteResponse.newBuilder()
                    .setStatus(okStatus(shardId))
                    .setVersion(nodeResponse.getVersion())
                    .build());
            responseObserver.onCompleted();

        } catch (KvException e) {
            responseObserver.onNext(
                    DeleteResponse.newBuilder().setStatus(exceptionToStatus(e)).build());
            responseObserver.onCompleted();
        }
    }

    // ========== Helper Methods ==========

    /**
     * Resolves shard ID, converting IllegalStateException to ShardMapUnavailableException.
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
            case RESOURCE_EXHAUSTED -> Status.Code.PAYLOAD_TOO_LARGE;
            case UNAVAILABLE -> Status.Code.UNAVAILABLE;
            case DEADLINE_EXCEEDED -> Status.Code.TIMEOUT;
            case CANCELLED -> Status.Code.CANCELLED;
            default -> Status.Code.INTERNAL;
        };
    }

    /**
     * Gets candidate nodes for a read operation.
     */
    List<NodeRecord> getNodesForRead(String shardId, Consistency consistency) {
        List<NodeRecord> candidates = new ArrayList<>();

        NodeRecord leader = shardMapCache.getLeaderNode(shardId);
        List<NodeRecord> replicas = shardMapCache.getReplicaNodes(shardId);

        if (consistency == Consistency.STRONG) {
            // Never fall back: the node also validates leadership and crosses a quorum read barrier.
            addIfAlive(candidates, leader);
        } else {
            // Prefer a follower explicitly; the applied version in the response exposes its staleness.
            for (NodeRecord replica : replicas) {
                if (!replica.equals(leader)) {
                    addIfAlive(candidates, replica);
                }
            }
            addIfAlive(candidates, leader);
        }

        return candidates;
    }

    private void addIfAlive(List<NodeRecord> list, NodeRecord node) {
        if (node != null && node.getStatus() == NodeStatus.ALIVE && !list.contains(node)) {
            list.add(node);
        }
    }

    private void addAllIfAlive(List<NodeRecord> list, List<NodeRecord> nodes) {
        for (NodeRecord node : nodes) {
            addIfAlive(list, node);
        }
    }

    /**
     * Gets candidate nodes for a write operation.
     */
    private List<NodeRecord> getNodesForWrite(String shardId) {
        List<NodeRecord> candidates = new ArrayList<>();

        NodeRecord leader = shardMapCache.getLeaderNode(shardId);
        if (leader != null) {
            candidates.add(leader);
        }
        if (candidates.isEmpty()) {
            for (NodeRecord replica : shardMapCache.getReplicaNodes(shardId)) {
                if (replica.getStatus() == NodeStatus.ALIVE) {
                    candidates.add(replica);
                }
            }
        }

        return candidates;
    }

    static String requireWriteRequestId(String requestId) {
        if (requestId == null || requestId.isBlank()) {
            throw new InvalidRequestException("request_id is required for writes and must be reused by retries");
        }
        return requestId;
    }

    private static Consistency normalizedConsistency(Consistency consistency) {
        return consistency == Consistency.CONSISTENCY_UNSPECIFIED ? Consistency.STRONG : consistency;
    }

    private static void validateReadOptions(ReadOptions options) {
        if (options.getMaxStalenessMs() != 0) {
            throw new InvalidRequestException("max_staleness_ms is not supported");
        }
        if (options.getConsistency() == Consistency.UNRECOGNIZED) {
            throw new InvalidRequestException("consistency is unrecognized");
        }
        Consistency consistency = normalizedConsistency(options.getConsistency());
        ReadMode mode = options.getReadMode();
        if (mode == ReadMode.UNRECOGNIZED) {
            throw new InvalidRequestException("read_mode is unrecognized");
        }
        if (mode == ReadMode.READ_YOUR_WRITES && consistency != Consistency.STRONG) {
            throw new InvalidRequestException("READ_YOUR_WRITES requires STRONG consistency");
        }
        if (mode == ReadMode.LOW_LATENCY && consistency != Consistency.EVENTUAL) {
            throw new InvalidRequestException("LOW_LATENCY requires EVENTUAL consistency");
        }
    }

    private static void validateDeleteOptions(WriteOptions options) {
        if (options.getTtlMs() != 0) {
            throw new InvalidRequestException("ttl_ms is not valid for delete");
        }
        if (options.getIfNotExists()) {
            throw new InvalidRequestException("if_not_exists is not valid for delete");
        }
    }

    private static com.kvdb.proto.kvstore.WriteDurability nodeDurability(WriteDurability durability) {
        return switch (durability) {
            case DURABILITY_UNSPECIFIED, QUORUM_SYNC -> com.kvdb.proto.kvstore.WriteDurability.QUORUM_SYNC;
            case WAL_SYNC -> com.kvdb.proto.kvstore.WriteDurability.LOCAL_SYNC;
            case WAL_ASYNC -> throw new InvalidRequestException("WAL_ASYNC durability is not supported");
            case UNRECOGNIZED -> throw new InvalidRequestException("durability is unrecognized");
        };
    }

    private static Status mutationStatus(String shardId, MutationOutcome outcome, String message) {
        Status.Code code =
                switch (outcome) {
                    case ALREADY_EXISTS -> Status.Code.ALREADY_EXISTS;
                    case VERSION_MISMATCH -> Status.Code.VERSION_MISMATCH;
                    case INVALID_OPTIONS, MUTATION_OUTCOME_UNSPECIFIED, UNRECOGNIZED -> Status.Code.PRECONDITION_FAILED;
                    case APPLIED -> Status.Code.OK;
                };
        return Status.newBuilder()
                .setCode(code)
                .setMessage(message)
                .setShardId(shardId)
                .build();
    }

    private static String printableKey(com.google.protobuf.ByteString key) {
        return Base64.getEncoder().encodeToString(key.toByteArray());
    }

    private static void auditWrite(
            String operation, String requestId, String shardId, GrpcIdentity identity, String traceparent) {
        logger.info(
                "KV write audit operation={} requestId={} shardId={} authenticatedRole={} authenticatedTenant={} authenticatedPrincipal={} traceparent={}",
                operation,
                requestId,
                shardId,
                identity.role().sanValue(),
                identity.tenant(),
                identity.principal(),
                traceparent);
    }

    private static Status writeOutcomeUnknownStatus(String shardId, ExecutionResult<?> result) {
        return Status.newBuilder()
                .setCode(Status.Code.WRITE_OUTCOME_UNKNOWN)
                .setMessage(result.getErrorMessage())
                .setShardId(shardId)
                .build();
    }
}
