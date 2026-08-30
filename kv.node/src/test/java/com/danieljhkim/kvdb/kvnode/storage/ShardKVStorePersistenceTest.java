package com.danieljhkim.kvdb.kvnode.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.danieljhkim.kvdb.kvcommon.persistence.WALManager;
import com.danieljhkim.kvdb.kvnode.persistence.FilePersistenceManager;
import com.danieljhkim.kvdb.kvnode.persistence.PersistenceManager;
import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ShardKVStorePersistenceTest {

    private static final TypeReference<Map<String, String>> MAP_TYPE = new TypeReference<>() {};

    @TempDir
    Path tempDir;

    @Test
    void walFailurePreventsMutationAndSuccess() {
        Path walPath = tempDir.resolve("failed-append.wal");
        WALManager failedWal = new WALManager(walPath.toString()) {
            @Override
            public synchronized void log(String operation, String key, String value) {
                throw new UncheckedIOException("No space left on device", new IOException("disk full"));
            }
        };
        ShardKVStore store = new ShardKVStore("shard", 100, false, new InMemoryPersistence(), failedWal);

        assertThrows(UncheckedIOException.class, () -> store.set("key", "value"));
        assertEquals("(nil)", store.get("key"));
    }

    @Test
    void snapshotFailureKeepsWalNeededForRestart() {
        Path walPath = tempDir.resolve("snapshot-failure.wal");
        WALManager wal = new WALManager(walPath.toString());
        PersistenceManager<Map<String, String>> failedSnapshot = new InMemoryPersistence() {
            @Override
            public void save(Map<String, String> data) throws IOException {
                throw new IOException("permission lost");
            }
        };
        ShardKVStore store = new ShardKVStore("shard", 1, true, failedSnapshot, wal);

        assertThrows(UncheckedIOException.class, () -> store.set("durable", "yes"));
        wal.close();

        ShardKVStore recovered = newStore("snapshot-failure");
        assertEquals("yes", recovered.get("durable"));
    }

    @Test
    void crashAfterSnapshotBeforeWalRotationRecoversAcknowledgedWrites() {
        Path snapshot = tempDir.resolve("rotation-boundary.json");
        Path walPath = tempDir.resolve("rotation-boundary.wal");
        WALManager interruptedRotation = new WALManager(walPath.toString()) {
            @Override
            public synchronized void clear() {
                throw new UncheckedIOException("crash before WAL rename", new IOException("interrupted rename"));
            }
        };
        ShardKVStore store = new ShardKVStore(
                "shard", 100, false, new FilePersistenceManager<>(snapshot.toString(), MAP_TYPE), interruptedRotation);
        assertTrue(store.set("before-cut", "kept"));

        assertThrows(UncheckedIOException.class, store::persistNow);
        interruptedRotation.close();

        ShardKVStore recovered = new ShardKVStore("shard", snapshot.toString(), walPath.toString(), 100, false);
        assertEquals("kept", recovered.get("before-cut"));
    }

    @Test
    void crashAfterWalRotationRecoversFromCommittedSnapshot() {
        ShardKVStore store = newStore("after-rotation");
        assertTrue(store.set("snapshotted", "kept"));
        store.persistNow();

        ShardKVStore recovered = newStore("after-rotation");
        assertEquals("kept", recovered.get("snapshotted"));
    }

    @Test
    void writeArrivingDuringSnapshotSurvivesRestartExactlyAsAcknowledged() throws Exception {
        Path snapshot = tempDir.resolve("concurrent.json");
        Path walPath = tempDir.resolve("concurrent.wal");
        BlockingPersistence persistence = new BlockingPersistence(snapshot);
        WALManager wal = new WALManager(walPath.toString());
        ShardKVStore store = new ShardKVStore("shard", 100, false, persistence, wal);
        assertTrue(store.set("before", "snapshot"));

        try (var executor = Executors.newFixedThreadPool(2)) {
            var snapshotFuture = executor.submit(store::persistNow);
            assertTrue(persistence.saveStarted.await(5, TimeUnit.SECONDS));
            var writeFuture = executor.submit(() -> store.set("during", "acknowledged"));
            assertFalse(writeFuture.isDone());

            persistence.allowSave.countDown();
            snapshotFuture.get(5, TimeUnit.SECONDS);
            assertTrue(writeFuture.get(5, TimeUnit.SECONDS));
        }
        wal.close();

        ShardKVStore recovered = new ShardKVStore("shard", snapshot.toString(), walPath.toString(), 100, false);
        assertEquals(Map.of("before", "snapshot", "during", "acknowledged"), recovered.snapshot());
    }

    private ShardKVStore newStore(String name) {
        return new ShardKVStore(
                "shard",
                tempDir.resolve(name + ".json").toString(),
                tempDir.resolve(name + ".wal").toString(),
                100,
                false);
    }

    private static class InMemoryPersistence implements PersistenceManager<Map<String, String>> {
        private Map<String, String> data;

        @Override
        public void save(Map<String, String> data) throws IOException {
            this.data = Map.copyOf(data);
        }

        @Override
        public Map<String, String> load() throws IOException {
            return data;
        }

        @Override
        public void close() throws IOException {}
    }

    private static final class BlockingPersistence implements PersistenceManager<Map<String, String>> {
        private final FilePersistenceManager<Map<String, String>> delegate;
        private final CountDownLatch saveStarted = new CountDownLatch(1);
        private final CountDownLatch allowSave = new CountDownLatch(1);

        private BlockingPersistence(Path snapshot) {
            delegate = new FilePersistenceManager<>(snapshot.toString(), MAP_TYPE);
        }

        @Override
        public void save(Map<String, String> data) throws IOException {
            saveStarted.countDown();
            try {
                if (!allowSave.await(5, TimeUnit.SECONDS)) {
                    throw new IOException("timed out waiting to continue snapshot");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("snapshot interrupted", e);
            }
            delegate.save(data);
        }

        @Override
        public Map<String, String> load() throws IOException {
            return delegate.load();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
