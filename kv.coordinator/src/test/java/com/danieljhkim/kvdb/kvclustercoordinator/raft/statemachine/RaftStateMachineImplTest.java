package com.danieljhkim.kvdb.kvclustercoordinator.raft.statemachine;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftCommand;
import com.danieljhkim.kvdb.kvclustercoordinator.state.NodeRecord;
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
}
