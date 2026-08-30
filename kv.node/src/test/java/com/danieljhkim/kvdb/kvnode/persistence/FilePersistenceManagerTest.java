package com.danieljhkim.kvdb.kvnode.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FilePersistenceManagerTest {

    private static final TypeReference<Map<String, String>> MAP_TYPE = new TypeReference<>() {};

    @TempDir
    Path tempDir;

    @Test
    void failedSnapshotPreservesThePriorSnapshotAtEveryPreCommitBoundary() throws IOException {
        for (FilePersistenceManager.FaultPoint failure : List.of(
                FilePersistenceManager.FaultPoint.BEFORE_WRITE,
                FilePersistenceManager.FaultPoint.BEFORE_FILE_SYNC,
                FilePersistenceManager.FaultPoint.BEFORE_ATOMIC_MOVE,
                FilePersistenceManager.FaultPoint.BEFORE_DIRECTORY_SYNC)) {
            Path snapshot = tempDir.resolve(failure.name() + ".json");
            FilePersistenceManager<Map<String, String>> initial =
                    new FilePersistenceManager<>(snapshot.toString(), MAP_TYPE);
            initial.save(Map.of("stable", "old"));

            FilePersistenceManager<Map<String, String>> faulting =
                    new FilePersistenceManager<>(snapshot.toString(), MAP_TYPE, point -> {
                        if (point == failure) {
                            String message =
                                    switch (failure) {
                                        case BEFORE_WRITE -> "No space left on device";
                                        case BEFORE_FILE_SYNC -> "snapshot fsync failed";
                                        case BEFORE_ATOMIC_MOVE -> "permission denied during rename";
                                        case BEFORE_DIRECTORY_SYNC -> "directory fsync failed";
                                    };
                            throw new IOException(message);
                        }
                    });

            assertThrows(IOException.class, () -> faulting.save(Map.of("new", "value")));
            assertEquals(Map.of("stable", "old"), initial.load());
        }
    }

    @Test
    void successfulSaveAtomicallyReplacesSnapshot() throws IOException {
        Path snapshot = tempDir.resolve("snapshot.json");
        FilePersistenceManager<Map<String, String>> manager =
                new FilePersistenceManager<>(snapshot.toString(), MAP_TYPE);
        manager.save(Map.of("old", "one"));
        manager.save(Map.of("new", "two"));

        assertEquals(Map.of("new", "two"), manager.load());
    }
}
