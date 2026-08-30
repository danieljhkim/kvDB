package com.danieljhkim.kvdb.kvnode.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import com.kvdb.proto.kvstore.MutationKind;
import com.kvdb.proto.kvstore.ReplicatedMutation;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class VersionedShardMutationTest {

    @TempDir
    Path tempDir;

    @Test
    void prepareIsHiddenAcrossRestartAndCommitIsDurableAndIdempotent() {
        ShardKVStore store = newStore("restart");
        ReplicatedMutation mutation =
                store.prepareNewMutation("request-1", 7, MutationKind.SET, "key", "value", "leader");

        assertEquals(1, mutation.getVersion());
        assertEquals("(nil)", store.get("key"));
        store.shutdown();

        ShardKVStore recoveredPrepared = newStore("restart");
        assertEquals("(nil)", recoveredPrepared.get("key"));
        assertTrue(recoveredPrepared.commitMutation(mutation).success());
        assertEquals("value", recoveredPrepared.get("key"));
        assertTrue(recoveredPrepared.commitMutation(mutation).success());
        recoveredPrepared.shutdown();

        ShardKVStore recoveredCommitted = newStore("restart");
        assertEquals("value", recoveredCommitted.get("key"));
        assertEquals(1, recoveredCommitted.committedVersion());
        assertEquals(1, recoveredCommitted.committedMutations().size());
        recoveredCommitted.shutdown();
    }

    @Test
    void duplicateConflictStaleVersionAndReorderedCommitAreRejected() {
        ShardKVStore store = newStore("ordering");
        ReplicatedMutation first = store.prepareNewMutation("request-1", 1, MutationKind.SET, "key-1", "one", "leader");
        ReplicatedMutation second = ReplicatedMutation.newBuilder()
                .setRequestId("request-2")
                .setShardId("shard-0")
                .setEpoch(1)
                .setVersion(2)
                .setKind(MutationKind.SET)
                .setKey(ByteString.copyFromUtf8("key-2"))
                .setValue(ByteString.copyFromUtf8("two"))
                .setOriginNodeId("leader")
                .build();

        assertTrue(store.prepareMutation(first).success());
        assertTrue(store.prepareMutation(second).success());
        assertFalse(store.commitMutation(second).success());
        assertEquals("(nil)", store.get("key-2"));
        assertTrue(store.commitMutation(first).success());
        assertTrue(store.commitMutation(second).success());

        ReplicatedMutation conflictingDuplicate =
                first.toBuilder().setValue(ByteString.copyFromUtf8("different")).build();
        assertFalse(store.prepareMutation(conflictingDuplicate).success());
        ReplicatedMutation stale =
                first.toBuilder().setRequestId("request-stale").build();
        assertFalse(store.prepareMutation(stale).success());
        store.shutdown();
    }

    @Test
    void boundedStateRepairConvergesDespiteReorderingAndDuplicateDelivery() {
        ShardKVStore leader = newStore("leader");
        commit(leader, "request-1", MutationKind.SET, "a", "one");
        commit(leader, "request-2", MutationKind.SET, "b", "two");
        commit(leader, "request-3", MutationKind.DELETE, "a", "");

        ShardKVStore follower = newStore("follower");
        List<ReplicatedMutation> state = leader.committedMutations();
        for (int i = state.size() - 1; i >= 0; i--) {
            assertTrue(follower.repairMutation(state.get(i)).success());
        }
        for (ReplicatedMutation duplicate : state) {
            assertTrue(follower.repairMutation(duplicate).success());
        }

        assertEquals(leader.snapshot(), follower.snapshot());
        assertEquals(leader.committedVersion(), follower.committedVersion());
        assertEquals("(nil)", follower.get("a"));
        assertEquals("two", follower.get("b"));
        leader.shutdown();
        follower.shutdown();
    }

    private void commit(ShardKVStore store, String requestId, MutationKind kind, String key, String value) {
        ReplicatedMutation mutation = store.prepareNewMutation(requestId, 1, kind, key, value, "leader");
        assertTrue(store.commitMutation(mutation).success());
    }

    private ShardKVStore newStore(String name) {
        return new ShardKVStore(
                "shard-0",
                tempDir.resolve(name + ".json").toString(),
                tempDir.resolve(name + ".wal").toString(),
                100,
                false);
    }
}
