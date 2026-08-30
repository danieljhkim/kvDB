package com.danieljhkim.kvdb.kvnode.storage;

import com.danieljhkim.kvdb.kvcommon.annotations.Timer;
import com.danieljhkim.kvdb.kvcommon.observability.Metrics;
import com.danieljhkim.kvdb.kvcommon.persistence.WALManager;
import com.danieljhkim.kvdb.kvnode.persistence.FilePersistenceManager;
import com.danieljhkim.kvdb.kvnode.persistence.PersistenceManager;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.protobuf.InvalidProtocolBufferException;
import com.kvdb.proto.kvstore.MutationKind;
import com.kvdb.proto.kvstore.ReplicatedMutation;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A shard-scoped key-value store with its own snapshot + WAL.
 *
 * <p>
 * Persistence layout is controlled by the provided snapshot + WAL file paths.
 */
public class ShardKVStore {

    private static final Logger logger = LoggerFactory.getLogger(ShardKVStore.class);

    private static final String NIL_RESPONSE = "(nil)";

    private final String shardId;
    private final int flushInterval;
    private final boolean enableAutoFlush;

    private final AtomicInteger curFlushInterval = new AtomicInteger(0);
    /** Serializes the WAL append, in-memory mutation, and snapshot/WAL cut. */
    private final ReentrantLock stateLock = new ReentrantLock();

    private final Map<String, String> store = new ConcurrentHashMap<>();
    private final PersistenceManager<Map<String, String>> persistenceManager;
    private final WALManager walManager;
    private final WALManager replicationWalManager;
    private final Map<String, ReplicatedMutation> mutationsByRequest = new HashMap<>();
    private final Map<String, MutationState> mutationStates = new HashMap<>();
    private final Map<Long, String> requestByVersion = new HashMap<>();
    private final Map<String, ReplicatedMutation> committedByKey = new HashMap<>();
    private long shardEpoch;
    private long highestVersion;
    private long committedVersion;

    public ShardKVStore(
            String shardId, String snapshotFilePath, String walFilePath, int flushInterval, boolean enableAutoFlush) {
        this(
                shardId,
                flushInterval,
                enableAutoFlush,
                new FilePersistenceManager<>(snapshotFilePath, new TypeReference<Map<String, String>>() {}),
                new WALManager(walFilePath),
                new WALManager(walFilePath + ".replication"));
    }

    ShardKVStore(
            String shardId,
            int flushInterval,
            boolean enableAutoFlush,
            PersistenceManager<Map<String, String>> persistenceManager,
            WALManager walManager) {
        this(shardId, flushInterval, enableAutoFlush, persistenceManager, walManager, null);
    }

    private ShardKVStore(
            String shardId,
            int flushInterval,
            boolean enableAutoFlush,
            PersistenceManager<Map<String, String>> persistenceManager,
            WALManager walManager,
            WALManager replicationWalManager) {
        this.shardId = Objects.requireNonNull(shardId, "shardId");
        this.flushInterval = flushInterval;
        this.enableAutoFlush = enableAutoFlush;
        this.persistenceManager = Objects.requireNonNull(persistenceManager, "persistenceManager");
        this.walManager = Objects.requireNonNull(walManager, "walManager");
        this.replicationWalManager = replicationWalManager;

        loadFromDisk();
        recoverFromWal();
        recoverReplicationState();

        logger.info(
                "ShardKVStore initialized shardId={}, autoFlushInterval={}, autoFlushEnabled={}",
                shardId,
                flushInterval,
                enableAutoFlush);
    }

    public String getShardId() {
        return shardId;
    }

    public boolean set(String key, String value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");

        stateLock.lock();
        try {
            walManager.log("SET", key, value);
            store.put(key, value);
            flushIfNeededLocked();
            return true;
        } finally {
            stateLock.unlock();
        }
    }

    public boolean del(String key) {
        Objects.requireNonNull(key, "key");

        stateLock.lock();
        try {
            walManager.log("DEL", key, null);
            boolean removed = (store.remove(key) != null);
            flushIfNeededLocked();
            return removed;
        } finally {
            stateLock.unlock();
        }
    }

    /**
     * Durably stages a new leader mutation without changing the visible keyspace. Reusing a request id returns the
     * original mutation when its immutable fields match.
     */
    public ReplicatedMutation prepareNewMutation(
            String requestId, long epoch, MutationKind kind, String key, String value, String originNodeId) {
        stateLock.lock();
        try {
            requireReplicationJournal();
            ReplicatedMutation existing = mutationsByRequest.get(requestId);
            if (existing != null) {
                if (!sameOperation(existing, epoch, kind, key, value)) {
                    throw new IllegalStateException("request_id was already used for a different mutation");
                }
                if (mutationStates.get(requestId) == MutationState.ABORTED) {
                    MutationStatus prepared = prepareMutationLocked(existing);
                    if (!prepared.success()) {
                        throw new IllegalStateException(prepared.message());
                    }
                }
                return existing;
            }

            ReplicatedMutation mutation = ReplicatedMutation.newBuilder()
                    .setRequestId(requireNonBlank(requestId, "request_id"))
                    .setShardId(shardId)
                    .setEpoch(epoch)
                    .setVersion(highestVersion + 1)
                    .setKind(kind)
                    .setKey(Objects.requireNonNull(key, "key"))
                    .setValue(value == null ? "" : value)
                    .setOriginNodeId(requireNonBlank(originNodeId, "origin_node_id"))
                    .build();
            MutationStatus prepared = prepareMutationLocked(mutation);
            if (!prepared.success()) {
                throw new IllegalStateException(prepared.message());
            }
            return mutation;
        } finally {
            stateLock.unlock();
        }
    }

    /** Durably records a hidden follower prepare. Duplicate delivery is idempotent. */
    public MutationStatus prepareMutation(ReplicatedMutation mutation) {
        stateLock.lock();
        try {
            requireReplicationJournal();
            return prepareMutationLocked(mutation);
        } finally {
            stateLock.unlock();
        }
    }

    /** Durably commits and then exposes a prepared mutation. Duplicate delivery is idempotent. */
    public MutationStatus commitMutation(ReplicatedMutation mutation) {
        stateLock.lock();
        try {
            requireReplicationJournal();
            String validation = validateMutation(mutation);
            if (validation != null) {
                return rejected(validation);
            }

            ReplicatedMutation existing = mutationsByRequest.get(mutation.getRequestId());
            MutationState state = mutationStates.get(mutation.getRequestId());
            if (existing == null || !existing.equals(mutation)) {
                return rejected("mutation was not durably prepared");
            }
            if (state == MutationState.COMMITTED) {
                return accepted("already committed");
            }
            if (state != MutationState.PREPARED) {
                return rejected("mutation is not in prepared state");
            }
            if (mutation.getEpoch() < shardEpoch || mutation.getVersion() <= committedVersion) {
                return rejected("stale epoch or version");
            }
            boolean lowerPrepared = mutationsByRequest.values().stream()
                    .anyMatch(candidate -> mutationStates.get(candidate.getRequestId()) == MutationState.PREPARED
                            && candidate.getEpoch() <= mutation.getEpoch()
                            && candidate.getVersion() < mutation.getVersion());
            if (lowerPrepared) {
                return rejected("out-of-order commit; an earlier mutation is still prepared");
            }

            replicationWalManager.log("COMMIT", mutation.getRequestId().getBytes(StandardCharsets.UTF_8), null);
            applyVisibleLocked(mutation);
            markCommitted(mutation);
            return accepted("committed");
        } finally {
            stateLock.unlock();
        }
    }

    /** Durably discards a hidden prepare. Committed mutations cannot be aborted. */
    public MutationStatus abortMutation(ReplicatedMutation mutation) {
        stateLock.lock();
        try {
            requireReplicationJournal();
            ReplicatedMutation existing = mutationsByRequest.get(mutation.getRequestId());
            if (existing == null) {
                return accepted("nothing to abort");
            }
            if (!existing.equals(mutation)) {
                return rejected("request_id conflicts with an existing mutation");
            }
            MutationState state = mutationStates.get(mutation.getRequestId());
            if (state == MutationState.COMMITTED) {
                return rejected("committed mutation cannot be aborted");
            }
            if (state != MutationState.ABORTED) {
                replicationWalManager.log("ABORT", mutation.getRequestId().getBytes(StandardCharsets.UTF_8), null);
                mutationStates.put(mutation.getRequestId(), MutationState.ABORTED);
            }
            return accepted("aborted");
        } finally {
            stateLock.unlock();
        }
    }

    /** Applies a committed state-transfer entry when it is newer for that key. */
    public MutationStatus repairMutation(ReplicatedMutation mutation) {
        stateLock.lock();
        try {
            requireReplicationJournal();
            String validation = validateMutation(mutation);
            if (validation != null) {
                return rejected(validation);
            }
            ReplicatedMutation known = mutationsByRequest.get(mutation.getRequestId());
            if (known != null && !known.equals(mutation)) {
                return rejected("request_id conflicts with an existing mutation");
            }
            String versionOwner = requestByVersion.get(mutation.getVersion());
            if (versionOwner != null && !versionOwner.equals(mutation.getRequestId())) {
                return rejected("version conflicts with a different mutation");
            }
            ReplicatedMutation current = committedByKey.get(mutation.getKey());
            if (current != null && current.getVersion() >= mutation.getVersion()) {
                return accepted("repair entry is already superseded");
            }

            replicationWalManager.log(
                    "REPAIR", mutation.getRequestId().getBytes(StandardCharsets.UTF_8), mutation.toByteArray());
            applyVisibleLocked(mutation);
            mutationsByRequest.put(mutation.getRequestId(), mutation);
            requestByVersion.put(mutation.getVersion(), mutation.getRequestId());
            markCommitted(mutation);
            return accepted("repaired");
        } finally {
            stateLock.unlock();
        }
    }

    public List<ReplicatedMutation> committedMutations() {
        stateLock.lock();
        try {
            return committedByKey.values().stream()
                    .sorted(Comparator.comparingLong(ReplicatedMutation::getVersion))
                    .toList();
        } finally {
            stateLock.unlock();
        }
    }

    public List<ReplicatedMutation> committedMutationsAfter(long afterVersion, int limit) {
        if (afterVersion < 0 || limit <= 0) {
            throw new IllegalArgumentException("afterVersion must be non-negative and limit must be positive");
        }
        stateLock.lock();
        try {
            return committedByKey.values().stream()
                    .filter(mutation -> mutation.getVersion() > afterVersion)
                    .sorted(Comparator.comparingLong(ReplicatedMutation::getVersion))
                    .limit(limit)
                    .toList();
        } finally {
            stateLock.unlock();
        }
    }

    public long committedVersion() {
        stateLock.lock();
        try {
            return committedVersion;
        } finally {
            stateLock.unlock();
        }
    }

    public long shardEpoch() {
        stateLock.lock();
        try {
            return shardEpoch;
        } finally {
            stateLock.unlock();
        }
    }

    public boolean isCommitted(String requestId) {
        stateLock.lock();
        try {
            return mutationStates.get(requestId) == MutationState.COMMITTED;
        } finally {
            stateLock.unlock();
        }
    }

    @Timer
    public String get(String key) {
        Objects.requireNonNull(key, "key");
        return store.getOrDefault(key, NIL_RESPONSE);
    }

    public int size() {
        return store.size();
    }

    public Map<String, String> snapshot() {
        stateLock.lock();
        try {
            return Collections.unmodifiableMap(new HashMap<>(store));
        } finally {
            stateLock.unlock();
        }
    }

    public Map<String, String> getMultiple(List<String> keys) {
        Objects.requireNonNull(keys, "keys");
        // Pre-size HashMap to avoid rehashing - account for load factor
        Map<String, String> result = new HashMap<>((int) (keys.size() / 0.75f) + 1);
        for (String key : keys) {
            String value = store.get(key);
            if (value != null) {
                result.put(key, value);
            }
        }
        return result;
    }

    @Timer
    private void loadFromDisk() {
        try {
            Map<String, String> loadedData = persistenceManager.load();
            if (loadedData == null) {
                return;
            }
            store.putAll(loadedData);
            logger.info("Loaded {} entries from disk for shardId={}", loadedData.size(), shardId);
        } catch (IOException e) {
            Metrics.increment("kvdb_snapshot_failures_total", "node", "load", "error");
            throw new UncheckedIOException("Failed to load shard snapshot for shardId=" + shardId, e);
        }
    }

    @Timer
    private void saveToDisk() {
        try {
            persistenceManager.save(new HashMap<>(store));
        } catch (IOException e) {
            Metrics.increment("kvdb_snapshot_failures_total", "node", "save", "error");
            throw new UncheckedIOException("Failed to save shard snapshot for shardId=" + shardId, e);
        }
    }

    @Timer
    private void recoverFromWal() {
        List<String[]> ops = walManager.replay();
        for (String[] op : ops) {
            if (op.length != 3) {
                throw new WALManager.WALCorruptionException("Malformed WAL operation for shardId=" + shardId);
            }
            String operation = op[0];
            String key = op[1];
            String value = op.length > 2 ? op[2] : null;

            switch (operation) {
                case "SET" -> store.put(key, value);
                case "DEL" -> store.remove(key);
                default -> throw new WALManager.WALCorruptionException(
                        "Unknown WAL operation " + operation + " for shardId=" + shardId);
            }
        }
        if (!ops.isEmpty()) {
            logger.info("Replayed {} WAL operations during recovery for shardId={}", ops.size(), shardId);
        }
    }

    /**
     * Incremental auto-flush mechanism: every write increments a counter. Once it reaches flushInterval, we persist the
     * current state and clear the WAL.
     */
    public void flushIfNeeded() {
        stateLock.lock();
        try {
            flushIfNeededLocked();
        } finally {
            stateLock.unlock();
        }
    }

    /**
     * Force persistence immediately (snapshot + WAL clear), regardless of the auto-flush counter.
     */
    public void persistNow() {
        stateLock.lock();
        try {
            snapshotAndRotateWal();
        } finally {
            stateLock.unlock();
        }
    }

    public void shutdown() {
        stateLock.lock();
        RuntimeException failure = null;
        try {
            try {
                snapshotAndRotateWal();
            } catch (RuntimeException e) {
                failure = e;
            }
            try {
                persistenceManager.close();
            } catch (IOException e) {
                RuntimeException closeFailure =
                        new UncheckedIOException("Failed to close persistence manager for shardId=" + shardId, e);
                failure = combineFailures(failure, closeFailure);
            }
            try {
                walManager.close();
            } catch (RuntimeException e) {
                failure = combineFailures(failure, e);
            }
            if (replicationWalManager != null) {
                try {
                    replicationWalManager.close();
                } catch (RuntimeException e) {
                    failure = combineFailures(failure, e);
                }
            }
        } finally {
            stateLock.unlock();
        }
        if (failure != null) {
            throw failure;
        }
    }

    public WALManager.Durability acknowledgedDurability() {
        return walManager.durability();
    }

    private void flushIfNeededLocked() {
        if (!enableAutoFlush) {
            return;
        }
        if (curFlushInterval.incrementAndGet() >= flushInterval) {
            snapshotAndRotateWal();
        }
    }

    private void snapshotAndRotateWal() {
        saveToDisk();
        walManager.clear();
        curFlushInterval.set(0);
    }

    private MutationStatus prepareMutationLocked(ReplicatedMutation mutation) {
        String validation = validateMutation(mutation);
        if (validation != null) {
            return rejected(validation);
        }
        ReplicatedMutation existing = mutationsByRequest.get(mutation.getRequestId());
        if (existing != null) {
            if (!existing.equals(mutation)) {
                return rejected("request_id conflicts with an existing mutation");
            }
            MutationState state = mutationStates.get(mutation.getRequestId());
            if (state == MutationState.ABORTED) {
                if (mutation.getEpoch() < shardEpoch || mutation.getVersion() <= committedVersion) {
                    return rejected("aborted mutation is now stale");
                }
                replicationWalManager.log(
                        "PREPARE", mutation.getRequestId().getBytes(StandardCharsets.UTF_8), mutation.toByteArray());
                mutationStates.put(mutation.getRequestId(), MutationState.PREPARED);
                return accepted("prepared again");
            }
            return accepted(state == MutationState.COMMITTED ? "already committed" : "already prepared");
        }
        String versionOwner = requestByVersion.get(mutation.getVersion());
        if (versionOwner != null && !versionOwner.equals(mutation.getRequestId())) {
            return rejected("version conflicts with a different mutation");
        }
        if (mutation.getEpoch() < shardEpoch || mutation.getVersion() <= committedVersion) {
            return rejected("stale epoch or version");
        }

        replicationWalManager.log(
                "PREPARE", mutation.getRequestId().getBytes(StandardCharsets.UTF_8), mutation.toByteArray());
        mutationsByRequest.put(mutation.getRequestId(), mutation);
        mutationStates.put(mutation.getRequestId(), MutationState.PREPARED);
        requestByVersion.put(mutation.getVersion(), mutation.getRequestId());
        shardEpoch = Math.max(shardEpoch, mutation.getEpoch());
        highestVersion = Math.max(highestVersion, mutation.getVersion());
        return accepted("prepared");
    }

    private String validateMutation(ReplicatedMutation mutation) {
        if (mutation == null || mutation.getRequestId().isBlank()) {
            return "request_id is required";
        }
        if (!shardId.equals(mutation.getShardId())) {
            return "mutation targets a different shard";
        }
        if (mutation.getEpoch() == 0 || mutation.getVersion() == 0) {
            return "epoch and version must be positive";
        }
        if (mutation.getKind() != MutationKind.SET && mutation.getKind() != MutationKind.DELETE) {
            return "mutation kind must be SET or DELETE";
        }
        if (mutation.getKey().isEmpty() || mutation.getOriginNodeId().isEmpty()) {
            return "key and origin_node_id are required";
        }
        return null;
    }

    private void applyVisibleLocked(ReplicatedMutation mutation) {
        if (mutation.getKind() == MutationKind.SET) {
            walManager.log("SET", mutation.getKey(), mutation.getValue());
            store.put(mutation.getKey(), mutation.getValue());
        } else {
            walManager.log("DEL", mutation.getKey(), null);
            store.remove(mutation.getKey());
        }
        flushIfNeededLocked();
    }

    private void markCommitted(ReplicatedMutation mutation) {
        mutationsByRequest.put(mutation.getRequestId(), mutation);
        mutationStates.put(mutation.getRequestId(), MutationState.COMMITTED);
        requestByVersion.put(mutation.getVersion(), mutation.getRequestId());
        ReplicatedMutation current = committedByKey.get(mutation.getKey());
        if (current == null || current.getVersion() < mutation.getVersion()) {
            committedByKey.put(mutation.getKey(), mutation);
        }
        shardEpoch = Math.max(shardEpoch, mutation.getEpoch());
        highestVersion = Math.max(highestVersion, mutation.getVersion());
        committedVersion = Math.max(committedVersion, mutation.getVersion());
    }

    private void recoverReplicationState() {
        if (replicationWalManager == null) {
            return;
        }
        for (WALManager.WalRecord record : replicationWalManager.replayRecords()) {
            String requestId = new String(record.key(), StandardCharsets.UTF_8);
            switch (record.operation()) {
                case "PREPARE" -> {
                    ReplicatedMutation mutation = parseMutation(record.value());
                    mutationsByRequest.put(requestId, mutation);
                    mutationStates.put(requestId, MutationState.PREPARED);
                    requestByVersion.put(mutation.getVersion(), requestId);
                    shardEpoch = Math.max(shardEpoch, mutation.getEpoch());
                    highestVersion = Math.max(highestVersion, mutation.getVersion());
                }
                case "COMMIT" -> {
                    ReplicatedMutation mutation = mutationsByRequest.get(requestId);
                    if (mutation == null) {
                        throw new WALManager.WALCorruptionException("COMMIT without PREPARE for " + requestId);
                    }
                    markCommitted(mutation);
                }
                case "ABORT" -> {
                    if (mutationsByRequest.containsKey(requestId)) {
                        mutationStates.put(requestId, MutationState.ABORTED);
                    }
                }
                case "REPAIR" -> {
                    ReplicatedMutation mutation = parseMutation(record.value());
                    markCommitted(mutation);
                }
                default -> throw new WALManager.WALCorruptionException(
                        "Unknown replication WAL operation " + record.operation());
            }
        }

        List<ReplicatedMutation> finalEntries = new ArrayList<>(committedByKey.values());
        finalEntries.sort(Comparator.comparingLong(ReplicatedMutation::getVersion));
        for (ReplicatedMutation mutation : finalEntries) {
            boolean alreadyApplied = mutation.getKind() == MutationKind.SET
                    ? mutation.getValue().equals(store.get(mutation.getKey()))
                    : !store.containsKey(mutation.getKey());
            if (!alreadyApplied) {
                applyVisibleLocked(mutation);
            }
        }
    }

    private static ReplicatedMutation parseMutation(byte[] value) {
        if (value == null) {
            throw new WALManager.WALCorruptionException("Replication WAL mutation payload is missing");
        }
        try {
            return ReplicatedMutation.parseFrom(value);
        } catch (InvalidProtocolBufferException e) {
            throw new WALManager.WALCorruptionException("Replication WAL mutation payload is malformed");
        }
    }

    private MutationStatus accepted(String message) {
        return new MutationStatus(true, true, message, committedVersion);
    }

    private MutationStatus rejected(String message) {
        return new MutationStatus(false, false, message, committedVersion);
    }

    private void requireReplicationJournal() {
        if (replicationWalManager == null) {
            throw new IllegalStateException("Versioned replication is unavailable for this test-only store");
        }
    }

    private static String requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value;
    }

    private static boolean sameOperation(
            ReplicatedMutation mutation, long epoch, MutationKind kind, String key, String value) {
        return mutation.getEpoch() == epoch
                && mutation.getKind() == kind
                && mutation.getKey().equals(key)
                && mutation.getValue().equals(value == null ? "" : value);
    }

    public record MutationStatus(boolean success, boolean durable, String message, long committedVersion) {}

    private enum MutationState {
        PREPARED,
        COMMITTED,
        ABORTED
    }

    private static RuntimeException combineFailures(RuntimeException primary, RuntimeException additional) {
        if (primary == null) {
            return additional;
        }
        primary.addSuppressed(additional);
        return primary;
    }
}
