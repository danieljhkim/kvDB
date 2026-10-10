package com.danieljhkim.kvdb.kvclustercoordinator.raft.statemachine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftCommand;
import com.danieljhkim.kvdb.kvclustercoordinator.state.NodeRecord;
import com.danieljhkim.kvdb.kvclustercoordinator.state.RejectedMutationException;
import com.danieljhkim.kvdb.kvclustercoordinator.state.ShardMapDelta;
import com.danieljhkim.kvdb.kvclustercoordinator.state.ShardMapSnapshot;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class RaftStateMachineImplTest {

    @Test
    void applyFutureAcknowledgesCompletedMutationWithoutASecondLog() {
        RaftStateMachineImpl stateMachine = new RaftStateMachineImpl();

        CompletableFuture<Void> result =
                stateMachine.apply(new RaftCommand.RegisterNode("node-1", "node-1:9000", "zone-a"));

        assertTrue(result.isDone());
        assertNotNull(stateMachine.getSnapshot().getNode("node-1"));
    }

    @Test
    void failedMutationCompletesExceptionallyAndDoesNotPublishPartialState() {
        RaftStateMachineImpl stateMachine = new RaftStateMachineImpl();

        CompletableFuture<Void> result =
                stateMachine.apply(new RaftCommand.SetNodeStatus("missing", NodeRecord.NodeStatus.DEAD));

        assertThrows(CompletionException.class, result::join);
        assertTrue(stateMachine.getSnapshot().getNodes().isEmpty());
    }

    @Test
    void invalidCommittedMutationsAreRejectedWithoutPublishingStateOrNotifyingWatchers() {
        RaftStateMachineImpl stateMachine = new RaftStateMachineImpl();
        stateMachine.applySync(new RaftCommand.RegisterNode("node-1", "localhost:8001", "zone-a"));
        stateMachine.applySync(new RaftCommand.RegisterNode("node-2", "localhost:8002", "zone-a"));
        stateMachine.applySync(new RaftCommand.InitShards(1, 2));
        ShardMapSnapshot before = stateMachine.getSnapshot();
        List<ShardMapDelta> deltas = new ArrayList<>();
        stateMachine.addWatcher(deltas::add);

        List<RaftCommand> invalid = List.of(
                new RaftCommand.SetShardLeader("shard-0", 1, "node-9"),
                new RaftCommand.SetShardReplicas("shard-0", List.of()),
                new RaftCommand.SetShardReplicas("shard-0", List.of("node-1", "node-9")),
                new RaftCommand.SetShardReplicas("shard-0", List.of("node-1", "node-1")),
                new RaftCommand.RegisterNode("node-x", "nocolon", "zone-a"));
        for (RaftCommand command : invalid) {
            CompletionException failure = assertThrows(
                    CompletionException.class, () -> stateMachine.apply(command).join());
            assertInstanceOf(RejectedMutationException.class, failure.getCause(), command.describe());
        }

        assertSame(before, stateMachine.getSnapshot());
        // Two registrations and shard initialization each publish a map version. The rejected commands do not.
        assertEquals(3, stateMachine.getMapVersion());
        assertTrue(deltas.isEmpty());
    }
}
