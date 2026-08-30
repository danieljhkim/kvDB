package com.danieljhkim.kvdb.kvnode.storage;

import com.danieljhkim.kvdb.kvcommon.annotations.Timer;
import com.danieljhkim.kvdb.kvcommon.observability.Metrics;
import com.danieljhkim.kvdb.kvcommon.persistence.WALManager;
import com.danieljhkim.kvdb.kvnode.persistence.FilePersistenceManager;
import com.danieljhkim.kvdb.kvnode.persistence.PersistenceManager;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.kvdb.proto.kvstore.MutationKind;
import com.kvdb.proto.kvstore.MutationOutcome;
import com.kvdb.proto.kvstore.ReplicatedMutation;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
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
    private static final String SNAPSHOT_KEY_PREFIX = "kvdb-binary-key-v1:";
    private static final String SNAPSHOT_VALUE_PREFIX = "kvdb-entry-v1:";

    private final String shardId;
    private final int flushInterval;
    private final boolean enableAutoFlush;

    private final AtomicInteger curFlushInterval = new AtomicInteger(0);
    /** Serializes the WAL append, in-memory mutation, and snapshot/WAL cut. */
    private final ReentrantLock stateLock = new ReentrantLock();

    private final Map<ByteString, StoredValue> store = new ConcurrentHashMap<>();
    private final PersistenceManager<Map<String, String>> persistenceManager;
    private final WALManager walManager;
    private final WALManager replicationWalManager;
    private final Map<String, ReplicatedMutation> mutationsByRequest = new HashMap<>();
    private final Map<String, MutationState> mutationStates = new HashMap<>();
    private final Map<Long, String> requestByVersion = new HashMap<>();
    private final Map<ByteString, ReplicatedMutation> committedByKey = new HashMap<>();
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
        return set(ByteString.copyFromUtf8(key), ByteString.copyFromUtf8(value));
    }

    public boolean set(ByteString key, ByteString value) {
        applyStandaloneMutation(MutationKind.SET, key, value);
        return true;
    }

    public boolean del(String key) {
        return del(ByteString.copyFromUtf8(key));
    }

    public boolean del(ByteString key) {
        Objects.requireNonNull(key, "key");
        stateLock.lock();
        try {
            expireIfNeededLocked(key, System.currentTimeMillis());
            boolean existed = store.containsKey(key);
            applyStandaloneMutationLocked(MutationKind.DELETE, key, ByteString.EMPTY);
            return existed;
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
        return prepareNewMutation(
                requestId,
                epoch,
                kind,
                ByteString.copyFromUtf8(key),
                ByteString.copyFromUtf8(value == null ? "" : value),
                originNodeId,
                0,
                OptionalLong.empty(),
                false,
                System.currentTimeMillis());
    }

    public ReplicatedMutation prepareNewMutation(
            String requestId,
            long epoch,
            MutationKind kind,
            ByteString key,
            ByteString value,
            String originNodeId,
            long ttlMs,
            OptionalLong expectedVersion,
            boolean ifNotExists,
            long nowMs) {
        stateLock.lock();
        try {
            requireReplicationJournal();
            requireNonEmpty(key, "key");
            Objects.requireNonNull(value, "value");
            ReplicatedMutation existing = mutationsByRequest.get(requestId);
            if (existing != null) {
                if (!sameOperation(existing, epoch, kind, key, value, ttlMs, expectedVersion, ifNotExists)) {
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

            if (kind == MutationKind.DELETE && ttlMs != 0) {
                throw new InvalidMutationOptionsException("ttl_ms is not valid for delete");
            }
            if (kind == MutationKind.DELETE && ifNotExists) {
                throw new InvalidMutationOptionsException("if_not_exists is not valid for delete");
            }
            if (ttlMs < 0 || nowMs < 0) {
                throw new InvalidMutationOptionsException("ttl_ms and current time must be non-negative");
            }
            expireIfNeededLocked(key, nowMs);
            StoredValue current = store.get(key);
            if (ifNotExists && current != null) {
                throw new ConditionalMutationException(MutationOutcome.ALREADY_EXISTS, "key already exists");
            }
            if (expectedVersion.isPresent()) {
                long actualVersion = current == null ? 0 : current.version();
                if (actualVersion != expectedVersion.getAsLong()) {
                    throw new ConditionalMutationException(
                            MutationOutcome.VERSION_MISMATCH,
                            "version mismatch (expected=" + expectedVersion.getAsLong() + ", actual=" + actualVersion
                                    + ")");
                }
            }

            long expireTimeMs = expirationTime(nowMs, ttlMs);
            long createTimeMs = current == null ? nowMs : current.createTimeMs();

            ReplicatedMutation.Builder mutation = ReplicatedMutation.newBuilder()
                    .setRequestId(requireNonBlank(requestId, "request_id"))
                    .setShardId(shardId)
                    .setEpoch(epoch)
                    .setVersion(highestVersion + 1)
                    .setKind(kind)
                    .setKey(Objects.requireNonNull(key, "key"))
                    .setValue(value)
                    .setOriginNodeId(requireNonBlank(originNodeId, "origin_node_id"))
                    .setTtlMs(ttlMs)
                    .setIfNotExists(ifNotExists)
                    .setCreateTimeMs(createTimeMs)
                    .setUpdateTimeMs(nowMs)
                    .setExpireTimeMs(expireTimeMs);
            expectedVersion.ifPresent(mutation::setIfVersionEquals);
            ReplicatedMutation builtMutation = mutation.build();
            MutationStatus prepared = prepareMutationLocked(builtMutation);
            if (!prepared.success()) {
                throw new IllegalStateException(prepared.message());
            }
            return builtMutation;
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
        ReadResult result = read(ByteString.copyFromUtf8(key));
        return result.found() ? result.value().toStringUtf8() : NIL_RESPONSE;
    }

    /** Returns a value and its committed version metadata from one locked shard snapshot. */
    public ReadResult read(String key) {
        return read(ByteString.copyFromUtf8(key));
    }

    public ReadResult read(ByteString key) {
        Objects.requireNonNull(key, "key");
        stateLock.lock();
        try {
            expireIfNeededLocked(key, System.currentTimeMillis());
            StoredValue stored = store.get(key);
            ReplicatedMutation mutation = committedByKey.get(key);
            long keyVersion = stored != null ? stored.version() : mutation == null ? 0 : mutation.getVersion();
            if (stored == null) {
                return new ReadResult(ByteString.EMPTY, false, keyVersion, committedVersion, shardEpoch, 0, 0, 0);
            }
            return new ReadResult(
                    stored.value(),
                    true,
                    keyVersion,
                    committedVersion,
                    shardEpoch,
                    stored.createTimeMs(),
                    stored.updateTimeMs(),
                    stored.expireTimeMs());
        } finally {
            stateLock.unlock();
        }
    }

    public int size() {
        return store.size();
    }

    public Map<String, String> snapshot() {
        stateLock.lock();
        try {
            Map<String, String> result = new HashMap<>();
            store.forEach(
                    (key, value) -> result.put(key.toStringUtf8(), value.value().toStringUtf8()));
            return Collections.unmodifiableMap(result);
        } finally {
            stateLock.unlock();
        }
    }

    public Map<String, String> getMultiple(List<String> keys) {
        Objects.requireNonNull(keys, "keys");
        // Pre-size HashMap to avoid rehashing - account for load factor
        Map<String, String> result = new HashMap<>((int) (keys.size() / 0.75f) + 1);
        for (String key : keys) {
            ReadResult read = read(key);
            if (read.found()) {
                result.put(key, read.value().toStringUtf8());
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
            loadedData.forEach((key, value) -> {
                DecodedSnapshotEntry decoded = decodeSnapshotEntry(key, value);
                store.put(decoded.key(), decoded.value());
            });
            logger.info("Loaded {} entries from disk for shardId={}", loadedData.size(), shardId);
        } catch (IOException e) {
            Metrics.increment("kvdb_snapshot_failures_total", "node", "load", "error");
            throw new UncheckedIOException("Failed to load shard snapshot for shardId=" + shardId, e);
        }
    }

    @Timer
    private void saveToDisk() {
        try {
            Map<String, String> encoded = new HashMap<>();
            store.forEach((key, value) -> encoded.put(encodeSnapshotKey(key), encodeSnapshotValue(value)));
            persistenceManager.save(encoded);
        } catch (IOException e) {
            Metrics.increment("kvdb_snapshot_failures_total", "node", "save", "error");
            throw new UncheckedIOException("Failed to save shard snapshot for shardId=" + shardId, e);
        }
    }

    @Timer
    private void recoverFromWal() {
        List<WALManager.WalRecord> ops = walManager.replayRecords();
        for (WALManager.WalRecord op : ops) {
            ByteString key = ByteString.copyFrom(op.key());
            switch (op.operation()) {
                case "SET" -> store.put(
                        key,
                        new StoredValue(
                                ByteString.copyFrom(op.value() == null ? new byte[0] : op.value()), 0, 0, 0, 0));
                case "SET2" -> store.put(key, decodeStoredValue(op.value()));
                case "DEL" -> store.remove(key);
                default -> throw new WALManager.WALCorruptionException(
                        "Unknown WAL operation " + op.operation() + " for shardId=" + shardId);
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

    private void applyStandaloneMutation(MutationKind kind, ByteString key, ByteString value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        stateLock.lock();
        try {
            applyStandaloneMutationLocked(kind, key, value);
        } finally {
            stateLock.unlock();
        }
    }

    private void applyStandaloneMutationLocked(MutationKind kind, ByteString key, ByteString value) {
        long nowMs = System.currentTimeMillis();
        expireIfNeededLocked(key, nowMs);
        StoredValue current = store.get(key);
        long version = highestVersion + 1;
        ReplicatedMutation mutation = ReplicatedMutation.newBuilder()
                .setRequestId("standalone-" + version)
                .setShardId(shardId)
                .setEpoch(Math.max(1, shardEpoch))
                .setVersion(version)
                .setKind(kind)
                .setKey(key)
                .setValue(value)
                .setOriginNodeId("standalone")
                .setCreateTimeMs(current == null ? nowMs : current.createTimeMs())
                .setUpdateTimeMs(nowMs)
                .build();
        applyVisibleLocked(mutation);
        markCommitted(mutation);
    }

    private void expireIfNeededLocked(ByteString key, long nowMs) {
        StoredValue current = store.get(key);
        if (current != null && current.expireTimeMs() != 0 && current.expireTimeMs() <= nowMs) {
            store.remove(key, current);
        }
    }

    private static long expirationTime(long nowMs, long ttlMs) {
        if (ttlMs == 0) {
            return 0;
        }
        try {
            return Math.addExact(nowMs, ttlMs);
        } catch (ArithmeticException e) {
            throw new InvalidMutationOptionsException("ttl_ms overflows expire_time_ms");
        }
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
        if (mutation.getKind() == MutationKind.DELETE && (mutation.getTtlMs() != 0 || mutation.getIfNotExists())) {
            return "delete mutation contains invalid TTL or create-only options";
        }
        if (mutation.getExpireTimeMs() != 0 && mutation.getExpireTimeMs() < mutation.getUpdateTimeMs()) {
            return "expire_time_ms precedes update_time_ms";
        }
        return null;
    }

    private void applyVisibleLocked(ReplicatedMutation mutation) {
        if (mutation.getKind() == MutationKind.SET) {
            StoredValue value = new StoredValue(
                    mutation.getValue(),
                    mutation.getVersion(),
                    mutation.getCreateTimeMs(),
                    mutation.getUpdateTimeMs(),
                    mutation.getExpireTimeMs());
            walManager.log("SET2", mutation.getKey().toByteArray(), encodeStoredValue(value));
            store.put(mutation.getKey(), value);
        } else {
            walManager.log("DEL", mutation.getKey().toByteArray(), null);
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
                    ? storedMutationMatches(mutation, store.get(mutation.getKey()))
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

    private static boolean storedMutationMatches(ReplicatedMutation mutation, StoredValue stored) {
        return stored != null
                && stored.value().equals(mutation.getValue())
                && stored.version() == mutation.getVersion()
                && stored.createTimeMs() == mutation.getCreateTimeMs()
                && stored.updateTimeMs() == mutation.getUpdateTimeMs()
                && stored.expireTimeMs() == mutation.getExpireTimeMs();
    }

    private static String encodeSnapshotKey(ByteString key) {
        return SNAPSHOT_KEY_PREFIX + Base64.getEncoder().encodeToString(key.toByteArray());
    }

    private static String encodeSnapshotValue(StoredValue value) {
        return SNAPSHOT_VALUE_PREFIX + Base64.getEncoder().encodeToString(encodeStoredValue(value));
    }

    private static DecodedSnapshotEntry decodeSnapshotEntry(String key, String value) {
        if (key.startsWith(SNAPSHOT_KEY_PREFIX) && value.startsWith(SNAPSHOT_VALUE_PREFIX)) {
            try {
                ByteString decodedKey =
                        ByteString.copyFrom(Base64.getDecoder().decode(key.substring(SNAPSHOT_KEY_PREFIX.length())));
                StoredValue decodedValue =
                        decodeStoredValue(Base64.getDecoder().decode(value.substring(SNAPSHOT_VALUE_PREFIX.length())));
                return new DecodedSnapshotEntry(decodedKey, decodedValue);
            } catch (IllegalArgumentException | WALManager.WALCorruptionException ignored) {
                // A legacy UTF-8 entry may coincidentally begin with the marker.
            }
        }
        return new DecodedSnapshotEntry(
                ByteString.copyFromUtf8(key), new StoredValue(ByteString.copyFromUtf8(value), 0, 0, 0, 0));
    }

    private static byte[] encodeStoredValue(StoredValue value) {
        byte[] bytes = value.value().toByteArray();
        return ByteBuffer.allocate(4 * Long.BYTES + Integer.BYTES + bytes.length)
                .putLong(value.version())
                .putLong(value.createTimeMs())
                .putLong(value.updateTimeMs())
                .putLong(value.expireTimeMs())
                .putInt(bytes.length)
                .put(bytes)
                .array();
    }

    private static StoredValue decodeStoredValue(byte[] bytes) {
        if (bytes == null || bytes.length < 4 * Long.BYTES + Integer.BYTES) {
            throw new WALManager.WALCorruptionException("Stored value payload is truncated");
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        long version = buffer.getLong();
        long createTimeMs = buffer.getLong();
        long updateTimeMs = buffer.getLong();
        long expireTimeMs = buffer.getLong();
        int valueLength = buffer.getInt();
        if (valueLength < 0 || valueLength != buffer.remaining()) {
            throw new WALManager.WALCorruptionException("Stored value payload length is invalid");
        }
        byte[] value = new byte[valueLength];
        buffer.get(value);
        return new StoredValue(ByteString.copyFrom(value), version, createTimeMs, updateTimeMs, expireTimeMs);
    }

    private static void requireNonEmpty(ByteString value, String name) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException(name + " is required");
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
            ReplicatedMutation mutation,
            long epoch,
            MutationKind kind,
            ByteString key,
            ByteString value,
            long ttlMs,
            OptionalLong expectedVersion,
            boolean ifNotExists) {
        return mutation.getEpoch() == epoch
                && mutation.getKind() == kind
                && mutation.getKey().equals(key)
                && mutation.getValue().equals(value)
                && mutation.getTtlMs() == ttlMs
                && mutation.getIfNotExists() == ifNotExists
                && mutation.hasIfVersionEquals() == expectedVersion.isPresent()
                && (!mutation.hasIfVersionEquals() || mutation.getIfVersionEquals() == expectedVersion.getAsLong());
    }

    public record MutationStatus(boolean success, boolean durable, String message, long committedVersion) {}

    public record ReadResult(
            ByteString value,
            boolean found,
            long version,
            long appliedVersion,
            long shardEpoch,
            long createTimeMs,
            long updateTimeMs,
            long expireTimeMs) {}

    public static final class ConditionalMutationException extends IllegalStateException {
        private final MutationOutcome outcome;

        ConditionalMutationException(MutationOutcome outcome, String message) {
            super(message);
            this.outcome = outcome;
        }

        public MutationOutcome outcome() {
            return outcome;
        }
    }

    public static final class InvalidMutationOptionsException extends IllegalArgumentException {
        InvalidMutationOptionsException(String message) {
            super(message);
        }
    }

    private record StoredValue(
            ByteString value, long version, long createTimeMs, long updateTimeMs, long expireTimeMs) {}

    private record DecodedSnapshotEntry(ByteString key, StoredValue value) {}

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
