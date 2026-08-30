package com.danieljhkim.kvdb.kvnode.cluster;

import com.danieljhkim.kvdb.kvcommon.cache.ShardMapCache;
import com.danieljhkim.kvdb.kvcommon.exception.NodeUnavailableException;
import com.danieljhkim.kvdb.kvcommon.observability.Metrics;
import com.danieljhkim.kvdb.kvnode.client.ReplicaWriteClient;
import com.danieljhkim.kvdb.kvnode.storage.ShardKVStore;
import com.danieljhkim.kvdb.kvnode.storage.ShardStoreRegistry;
import com.danieljhkim.kvdb.proto.coordinator.ShardRecord;
import com.google.protobuf.ByteString;
import com.kvdb.proto.kvstore.MutationKind;
import com.kvdb.proto.kvstore.ReplicaRepairRequest;
import com.kvdb.proto.kvstore.ReplicaRepairResponse;
import com.kvdb.proto.kvstore.ReplicaStateRequest;
import com.kvdb.proto.kvstore.ReplicaStateResponse;
import com.kvdb.proto.kvstore.ReplicateMutationRequest;
import com.kvdb.proto.kvstore.ReplicatedMutation;
import com.kvdb.proto.kvstore.ReplicationAck;
import com.kvdb.proto.kvstore.ReplicationPhase;
import com.kvdb.proto.kvstore.WriteDurability;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Coordinates durable quorum replication. A mutation is first fsynced as hidden PREPARE state locally and on the
 * requested replica quorum. Only after that quorum exists is it committed into the visible local keyspace. Prepared
 * replicas are finalized synchronously when possible and by the bounded repair worker otherwise.
 */
public class ReplicationManager implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ReplicationManager.class);
    private static final int REPAIR_BATCH_SIZE = 128;
    private static final int MAX_PULL_BATCHES_PER_PASS = 8;

    private final String nodeId;
    private final ShardMapCache shardMapCache;
    private final ShardStoreRegistry shardStores;
    private final ReplicaWriteClient replicaWriteClient;
    private final Duration replicationTimeout;
    private final ConcurrentMap<String, ReentrantLock> shardLocks = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ShardRecord> knownShards = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, RepairProgress> repairProgress = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Long> repairCursors = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Long> pullCursors = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Long> reconciledEpochs = new ConcurrentHashMap<>();
    private final ScheduledExecutorService repairExecutor;

    public ReplicationManager(
            String nodeId,
            ShardMapCache shardMapCache,
            ShardStoreRegistry shardStores,
            ReplicaWriteClient replicaWriteClient,
            Duration replicationTimeout) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
        this.shardMapCache = Objects.requireNonNull(shardMapCache, "shardMapCache");
        this.shardStores = Objects.requireNonNull(shardStores, "shardStores");
        this.replicaWriteClient = replicaWriteClient;
        this.replicationTimeout = replicationTimeout != null ? replicationTimeout : Duration.ofMillis(500);
        this.repairExecutor = Executors.newSingleThreadScheduledExecutor(
                Thread.ofPlatform().daemon().name("kvdb-replica-repair-", 0).factory());
        long repairIntervalMs = Math.max(1_000L, this.replicationTimeout.toMillis() * 4L);
        repairExecutor.scheduleWithFixedDelay(
                this::repairKnownShardsSafely, repairIntervalMs, repairIntervalMs, TimeUnit.MILLISECONDS);
    }

    public MutationResult replicateSet(
            String shardId, ShardRecord shard, String key, String value, String requestId, WriteDurability durability) {
        return replicateSet(
                shardId,
                shard,
                ByteString.copyFromUtf8(key),
                ByteString.copyFromUtf8(value),
                requestId,
                durability,
                0,
                OptionalLong.empty(),
                false);
    }

    public MutationResult replicateSet(
            String shardId,
            ShardRecord shard,
            ByteString key,
            ByteString value,
            String requestId,
            WriteDurability durability,
            long ttlMs,
            OptionalLong expectedVersion,
            boolean ifNotExists) {
        return replicateMutation(
                shardId,
                shard,
                key,
                value,
                requestId,
                MutationKind.SET,
                durability,
                ttlMs,
                expectedVersion,
                ifNotExists);
    }

    public MutationResult replicateDelete(
            String shardId, ShardRecord shard, String key, String requestId, WriteDurability durability) {
        return replicateDelete(
                shardId, shard, ByteString.copyFromUtf8(key), requestId, durability, OptionalLong.empty());
    }

    public MutationResult replicateDelete(
            String shardId,
            ShardRecord shard,
            ByteString key,
            String requestId,
            WriteDurability durability,
            OptionalLong expectedVersion) {
        return replicateMutation(
                shardId,
                shard,
                key,
                ByteString.EMPTY,
                requestId,
                MutationKind.DELETE,
                durability,
                0,
                expectedVersion,
                false);
    }

    /** Runs one bounded anti-entropy pass immediately. Exposed for operational hooks and deterministic tests. */
    public void repairReplicas(String shardId, ShardRecord shard) {
        knownShards.put(shardId, shard);
        if (replicaWriteClient == null) {
            return;
        }
        ShardKVStore local = shardStores.getOrCreate(shardId);
        List<ReplicatedMutation> mutations = local.committedMutations();
        if (mutations.isEmpty()) {
            return;
        }

        for (String target : getReplicationTargets(shard)) {
            String progressKey = target + "/" + shardId;
            long cursor = repairCursors.getOrDefault(progressKey, 0L);
            List<ReplicatedMutation> batch = mutations.stream()
                    .filter(mutation -> mutation.getVersion() > cursor)
                    .limit(REPAIR_BATCH_SIZE)
                    .toList();
            if (batch.isEmpty()) {
                repairCursors.put(progressKey, 0L);
                repairProgress.put(progressKey, new RepairProgress(0, 0, true));
                continue;
            }

            int applied = 0;
            boolean complete = false;
            try {
                ReplicaRepairResponse response = replicaWriteClient.repairReplica(
                        target,
                        ReplicaRepairRequest.newBuilder()
                                .setShardId(shardId)
                                .setEpoch(shard.getEpoch())
                                .addAllCommittedMutations(batch)
                                .build());
                if (response.getSuccess() && response.getDurable()) {
                    applied = response.getAppliedMutations();
                    long lastVersion = batch.getLast().getVersion();
                    repairCursors.put(progressKey, lastVersion);
                    complete = mutations.getLast().getVersion() <= lastVersion;
                    Metrics.increment("kvdb_replica_repair_batches_total", "node", "repair", "ok");
                } else {
                    Metrics.increment("kvdb_replica_repair_batches_total", "node", "repair", "error");
                }
            } catch (RuntimeException e) {
                Metrics.increment("kvdb_replica_repair_batches_total", "node", "repair", "error");
                log.debug("Replica repair failed target={} shard={}: {}", target, shardId, e.getMessage());
            }
            repairProgress.put(progressKey, new RepairProgress(batch.size(), applied, complete));
            log.debug(
                    "Replica repair progress target={} shard={} sent={} applied={} complete={}",
                    target,
                    shardId,
                    batch.size(),
                    applied,
                    complete);
        }
    }

    public RepairProgress repairProgress(String target, String shardId) {
        return repairProgress.getOrDefault(target + "/" + shardId, new RepairProgress(0, 0, false));
    }

    /** Pulls committed state from a replica quorum before this node serves as a newly elected leader. */
    public void ensureLeaderReconciled(String shardId, ShardRecord shard) {
        ReentrantLock lock = shardLocks.computeIfAbsent(shardId, ignored -> new ReentrantLock());
        lock.lock();
        try {
            ensureLeaderReconciledLocked(shardId, shard, false);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Performs a quorum read barrier for a STRONG read. Unlike write admission, this deliberately rechecks a replica
     * quorum even after the current epoch was reconciled so a partitioned or superseded leader cannot serve from a
     * cached leadership decision.
     */
    public void ensureStrongReadReady(String shardId, ShardRecord shard) {
        ReentrantLock lock = shardLocks.computeIfAbsent(shardId, ignored -> new ReentrantLock());
        lock.lock();
        try {
            ensureLeaderReconciledLocked(shardId, shard, true);
        } finally {
            lock.unlock();
        }
    }

    private MutationResult replicateMutation(
            String shardId,
            ShardRecord shard,
            ByteString key,
            ByteString value,
            String requestId,
            MutationKind kind,
            WriteDurability durability,
            long ttlMs,
            OptionalLong expectedVersion,
            boolean ifNotExists) {
        Objects.requireNonNull(shard, "shard");
        knownShards.put(shardId, shard);
        ReentrantLock lock = shardLocks.computeIfAbsent(shardId, ignored -> new ReentrantLock());
        lock.lock();
        try {
            ensureLeaderReconciledLocked(shardId, shard, false);
            ShardKVStore local = shardStores.getOrCreate(shardId);
            ReplicatedMutation mutation = local.prepareNewMutation(
                    requestId,
                    shard.getEpoch(),
                    kind,
                    key,
                    value,
                    nodeId,
                    ttlMs,
                    expectedVersion,
                    ifNotExists,
                    System.currentTimeMillis());
            List<String> targets = getReplicationTargets(shard);
            int totalReplicas = Math.max(1, shard.getReplicasCount());
            int requiredAcks = requiredAcks(totalReplicas, durability);

            if (local.isCommitted(mutation.getRequestId())) {
                return new MutationResult(mutation.getRequestId(), mutation.getVersion(), requiredAcks);
            }

            if (requiredAcks == 1) {
                ShardKVStore.MutationStatus committed = local.commitMutation(mutation);
                if (!committed.success() || !committed.durable()) {
                    throw unavailable(shardId, "Local durable commit failed: " + committed.message());
                }
                return new MutationResult(mutation.getRequestId(), mutation.getVersion(), 1);
            }
            if (replicaWriteClient == null) {
                local.abortMutation(mutation);
                throw unavailable(shardId, "No replica client is configured for required durability");
            }

            Set<String> preparedTargets = executePhase(targets, mutation, ReplicationPhase.PREPARE);
            int durablePrepareAcks = 1 + preparedTargets.size();
            if (durablePrepareAcks < requiredAcks) {
                local.abortMutation(mutation);
                sendBestEffort(preparedTargets, mutation, ReplicationPhase.ABORT);
                Metrics.increment(
                        "kvdb_replica_quorum_total", "node", kind.name().toLowerCase(), "unavailable");
                throw unavailable(
                        shardId,
                        String.format(
                                "Replication quorum not reached for shard %s (durableAcks=%d, required=%d)",
                                shardId, durablePrepareAcks, requiredAcks));
            }

            Set<String> committedTargets = executePhase(preparedTargets, mutation, ReplicationPhase.COMMIT);
            int durableCommitAcks = 1 + committedTargets.size();
            if (durableCommitAcks < requiredAcks) {
                local.abortMutation(mutation);
                Set<String> stillPrepared = ConcurrentHashMap.newKeySet();
                stillPrepared.addAll(preparedTargets);
                stillPrepared.removeAll(committedTargets);
                sendBestEffort(stillPrepared, mutation, ReplicationPhase.ABORT);
                Metrics.increment(
                        "kvdb_replica_quorum_total", "node", kind.name().toLowerCase(), "unavailable");
                throw unavailable(
                        shardId,
                        String.format(
                                "Replication commit quorum not reached for shard %s (durableAcks=%d, required=%d)",
                                shardId, durableCommitAcks, requiredAcks));
            }

            ShardKVStore.MutationStatus committed = local.commitMutation(mutation);
            if (!committed.success() || !committed.durable()) {
                throw unavailable(shardId, "Local durable commit failed: " + committed.message());
            }
            if (committedTargets.size() < preparedTargets.size()) {
                Metrics.increment("kvdb_replica_repair_needed_total", "node", "mutation", "lagging");
                repairExecutor.execute(() -> repairReplicas(shardId, shard));
            }
            Metrics.increment("kvdb_replica_quorum_total", "node", kind.name().toLowerCase(), "ok");
            return new MutationResult(mutation.getRequestId(), mutation.getVersion(), durableCommitAcks);
        } finally {
            lock.unlock();
        }
    }

    private Set<String> executePhase(Iterable<String> targets, ReplicatedMutation mutation, ReplicationPhase phase) {
        Set<String> successful = ConcurrentHashMap.newKeySet();
        List<Thread> threads = new ArrayList<>();
        ReplicateMutationRequest request = ReplicateMutationRequest.newBuilder()
                .setMutation(mutation)
                .setPhase(phase)
                .build();
        for (String target : targets) {
            threads.add(Thread.startVirtualThread(() -> {
                try {
                    ReplicationAck ack = replicaWriteClient.replicateMutation(target, request);
                    if (ack.getSuccess() && ack.getDurable()) {
                        successful.add(target);
                    }
                } catch (RuntimeException e) {
                    log.debug(
                            "Replication phase={} target={} requestId={} failed: {}",
                            phase,
                            target,
                            mutation.getRequestId(),
                            e.getMessage());
                }
            }));
        }

        long deadlineNanos = System.nanoTime() + replicationTimeout.toNanos();
        for (Thread thread : threads) {
            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0) {
                break;
            }
            try {
                thread.join(Duration.ofNanos(remainingNanos));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        return Collections.unmodifiableSet(successful);
    }

    private void sendBestEffort(Set<String> targets, ReplicatedMutation mutation, ReplicationPhase phase) {
        if (!targets.isEmpty()) {
            executePhase(targets, mutation, phase);
        }
    }

    private List<String> getReplicationTargets(ShardRecord shard) {
        List<String> targets = new ArrayList<>();
        for (String replicaId : shard.getReplicasList()) {
            if (replicaId == null || replicaId.isEmpty() || replicaId.equals(nodeId)) {
                continue;
            }
            shardMapCache.getNodeAddress(replicaId).ifPresent(targets::add);
        }
        return targets;
    }

    private static int requiredAcks(int totalReplicas, WriteDurability durability) {
        if (durability == WriteDurability.LOCAL_SYNC) {
            return 1;
        }
        return durability == WriteDurability.ALL_SYNC ? totalReplicas : (totalReplicas / 2) + 1;
    }

    private void ensureLeaderReconciledLocked(String shardId, ShardRecord shard, boolean forceQuorumRead) {
        if (!nodeId.equals(shard.getLeader())) {
            throw unavailable(shardId, "Node is not the shard leader for reconciliation");
        }
        if (!forceQuorumRead && reconciledEpochs.getOrDefault(shardId, 0L) >= shard.getEpoch()) {
            return;
        }

        int required = (Math.max(1, shard.getReplicasCount()) / 2) + 1;
        if (required == 1) {
            reconciledEpochs.put(shardId, shard.getEpoch());
            return;
        }
        if (replicaWriteClient == null) {
            throw unavailable(shardId, "No replica client is configured for leader reconciliation");
        }

        ShardKVStore local = shardStores.getOrCreate(shardId);
        int durableStateAcks = 1;
        for (String target : getReplicationTargets(shard)) {
            if (pullReplicaState(target, shardId, shard.getEpoch(), local)) {
                durableStateAcks++;
            }
        }
        if (durableStateAcks < required) {
            Metrics.increment("kvdb_replica_reconcile_total", "node", "leader", "unavailable");
            throw unavailable(
                    shardId,
                    String.format(
                            "Leader reconciliation quorum not reached for shard %s (durableAcks=%d, required=%d)",
                            shardId, durableStateAcks, required));
        }
        reconciledEpochs.put(shardId, shard.getEpoch());
        Metrics.increment("kvdb_replica_reconcile_total", "node", "leader", "ok");
    }

    private boolean pullReplicaState(String target, String shardId, long epoch, ShardKVStore local) {
        String cursorKey = target + "/" + shardId + "/" + epoch;
        long afterVersion = pullCursors.getOrDefault(cursorKey, local.committedVersion());
        for (int batchIndex = 0; batchIndex < MAX_PULL_BATCHES_PER_PASS; batchIndex++) {
            try {
                ReplicaStateResponse response = replicaWriteClient.fetchReplicaState(
                        target,
                        ReplicaStateRequest.newBuilder()
                                .setShardId(shardId)
                                .setEpoch(epoch)
                                .setAfterVersion(afterVersion)
                                .setMaxMutations(REPAIR_BATCH_SIZE)
                                .build());
                if (!response.getSuccess() || !response.getDurable()) {
                    return false;
                }
                for (ReplicatedMutation mutation : response.getCommittedMutationsList()) {
                    ShardKVStore.MutationStatus status = local.repairMutation(mutation);
                    if (!status.success() || !status.durable()) {
                        return false;
                    }
                    afterVersion = Math.max(afterVersion, mutation.getVersion());
                }
                pullCursors.put(cursorKey, afterVersion);
                if (!response.getHasMore()) {
                    pullCursors.remove(cursorKey);
                    return true;
                }
                if (response.getCommittedMutationsCount() == 0) {
                    return false;
                }
            } catch (RuntimeException e) {
                log.debug("Replica state pull failed target={} shard={}: {}", target, shardId, e.getMessage());
                return false;
            }
        }
        return false;
    }

    private NodeUnavailableException unavailable(String shardId, String message) {
        return new NodeUnavailableException(message, shardId);
    }

    private void repairKnownShardsSafely() {
        try {
            for (ShardRecord shard : shardMapCache.getShards()) {
                if (nodeId.equals(shard.getLeader())) {
                    knownShards.put(shard.getShardId(), shard);
                }
            }
            for (Map.Entry<String, ShardRecord> entry : knownShards.entrySet()) {
                if (nodeId.equals(entry.getValue().getLeader())) {
                    try {
                        ensureLeaderReconciled(entry.getKey(), entry.getValue());
                        repairReplicas(entry.getKey(), entry.getValue());
                    } catch (NodeUnavailableException e) {
                        log.debug("Skipping repair until leader reconciliation succeeds for {}", entry.getKey());
                    }
                }
            }
        } catch (RuntimeException e) {
            Metrics.increment("kvdb_replica_repair_batches_total", "node", "repair", "error");
            log.warn("Periodic replica repair failed", e);
        }
    }

    @Override
    public void close() {
        repairExecutor.shutdownNow();
    }

    public record MutationResult(String requestId, long version, int durableAcks) {}

    public record RepairProgress(int sent, int applied, boolean complete) {}
}
