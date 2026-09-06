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
    void newerCommitSupersedesAnAbandonedPrepareWithoutExposingIt() {
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
        assertTrue(store.commitMutation(second).success());
        assertEquals("two", store.get("key-2"));
        assertEquals("(nil)", store.get("key-1"));
        assertFalse(store.commitMutation(first).success());

        ReplicatedMutation conflictingDuplicate =
                first.toBuilder().setValue(ByteString.copyFromUtf8("different")).build();
        assertFalse(store.prepareMutation(conflictingDuplicate).success());
        ReplicatedMutation stale =
                first.toBuilder().setRequestId("request-stale").build();
        assertFalse(store.prepareMutation(stale).success());
        store.shutdown();
    }

    @Test
    void supersededPrepareStaysAbortedAcrossRestart() {
        ShardKVStore store = newStore("orphan-restart");
        ReplicatedMutation orphan =
                store.prepareNewMutation("request-orphan", 3, MutationKind.SET, "aborted", "hidden", "old-leader");
        store.shutdown();

        ShardKVStore restarted = newStore("orphan-restart");
        ReplicatedMutation successor = restarted.prepareNewMutation(
                "request-successor", 4, MutationKind.SET, "committed", "visible", "new-leader");
        assertEquals(2, successor.getVersion());
        assertTrue(restarted.commitMutation(successor).success());
        assertEquals("(nil)", restarted.get("aborted"));
        assertEquals("visible", restarted.get("committed"));
        assertFalse(restarted.commitMutation(orphan).success());
        restarted.shutdown();

        ShardKVStore recovered = newStore("orphan-restart");
        assertEquals("(nil)", recovered.get("aborted"));
        assertEquals("visible", recovered.get("committed"));
        assertEquals(2, recovered.committedVersion());
        assertFalse(recovered.commitMutation(orphan).success());
        recovered.shutdown();
    }

    @Test
    void higherEpochPrepareReplacesOnlyAnOlderPreparedVersionOwnerAcrossRestart() {
        ShardKVStore store = newStore("higher-epoch-replacement");
        ReplicatedMutation orphan =
                store.prepareNewMutation("request-orphan", 3, MutationKind.SET, "orphan", "hidden", "old-leader");
        ReplicatedMutation successor = orphan.toBuilder()
                .setRequestId("request-successor")
                .setEpoch(4)
                .setKey(ByteString.copyFromUtf8("successor"))
                .setValue(ByteString.copyFromUtf8("visible"))
                .setOriginNodeId("new-leader")
                .build();

        assertTrue(store.prepareMutation(successor).success());
        assertEquals("(nil)", store.get("orphan"));
        assertFalse(store.commitMutation(orphan).success());
        assertTrue(store.commitMutation(successor).success());
        assertEquals("visible", store.get("successor"));
        store.shutdown();

        ShardKVStore restarted = newStore("higher-epoch-replacement");
        assertEquals("(nil)", restarted.get("orphan"));
        assertEquals("visible", restarted.get("successor"));
        assertFalse(restarted.commitMutation(orphan).success());
        restarted.shutdown();
    }

    @Test
    void versionOwnerReplacementRejectsCommittedSameAndLowerEpochConflicts() {
        ShardKVStore committedStore = newStore("committed-version-owner");
        ReplicatedMutation committed =
                committedStore.prepareNewMutation("request-committed", 3, MutationKind.SET, "key", "value", "leader");
        assertTrue(committedStore.commitMutation(committed).success());
        assertFalse(committedStore
                .prepareMutation(committed.toBuilder()
                        .setRequestId("request-conflict")
                        .setEpoch(4)
                        .build())
                .success());
        committedStore.shutdown();

        ShardKVStore preparedStore = newStore("prepared-version-owner");
        ReplicatedMutation prepared =
                preparedStore.prepareNewMutation("request-prepared", 4, MutationKind.SET, "key", "value", "leader");
        assertFalse(preparedStore
                .prepareMutation(
                        prepared.toBuilder().setRequestId("request-same-epoch").build())
                .success());
        assertFalse(preparedStore
                .prepareMutation(prepared.toBuilder()
                        .setRequestId("request-lower-epoch")
                        .setEpoch(3)
                        .build())
                .success());
        preparedStore.shutdown();
    }

    @Test
    void committedRepairSupersedesPreparedConflictWhileWritesRemainEpochFenced() {
        ShardKVStore store = newStore("repair-orphan");
        ReplicatedMutation orphan =
                store.prepareNewMutation("request-orphan", 3, MutationKind.SET, "aborted", "hidden", "old-leader");
        ReplicatedMutation committed = orphan.toBuilder()
                .setRequestId("request-committed")
                .setEpoch(4)
                .setValue(ByteString.copyFromUtf8("recovered"))
                .setOriginNodeId("new-leader")
                .build();

        assertTrue(store.repairMutation(committed).success());
        assertEquals("recovered", store.get("aborted"));
        assertFalse(store.commitMutation(orphan).success());

        ReplicatedMutation stale = ReplicatedMutation.newBuilder()
                .setRequestId("request-stale")
                .setShardId("shard-0")
                .setEpoch(3)
                .setVersion(2)
                .setKind(MutationKind.SET)
                .setKey(ByteString.copyFromUtf8("stale"))
                .setValue(ByteString.copyFromUtf8("must-not-appear"))
                .setOriginNodeId("old-leader")
                .build();
        assertFalse(store.prepareMutation(stale).success());
        assertFalse(store.commitMutation(stale).success());
        assertEquals("(nil)", store.get("stale"));
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
