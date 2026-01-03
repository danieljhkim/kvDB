# Raft Component Integration Architecture

## Complete Component Hierarchy

```
┌─────────────────────────────────────────────────────────────────┐
│                          RaftNode                                │
│                      (Main Orchestrator)                         │
│                                                                   │
│  • Initializes all components                                    │
│  • Monitors role changes (every 50ms)                           │
│  • Coordinates component lifecycle                               │
│  • Provides high-level API                                       │
└───────────────────────────┬─────────────────────────────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        │                   │                   │
        ▼                   ▼                   ▼
┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│  Election    │   │ Replication  │   │    State     │
│  Components  │   │  Components  │   │  Management  │
└──────────────┘   └──────────────┘   └──────────────┘
        │                   │                   │
        │                   │                   │
        ▼                   ▼                   ▼

┌──────────────────────────────────────────────────────────────────┐
│                     ELECTION COMPONENTS                           │
├───────────────────────────────────────────────────────────────────┤
│  RaftElectionTimer                                                │
│    └─→ Triggers election timeout                                 │
│                                                                   │
│  RaftElectionManager                                              │
│    ├─→ Starts elections                                          │
│    ├─→ Sends RequestVote RPCs                                    │
│    ├─→ Collects votes                                            │
│    └─→ Calls state.becomeLeader()                               │
│                                                                   │
│  RaftVoteHandler                                                  │
│    └─→ Handles incoming RequestVote RPCs                         │
└───────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────┐
│                   REPLICATION COMPONENTS                          │
├───────────────────────────────────────────────────────────────────┤
│  RaftHeartbeatManager  ⭐ AUTO-STARTED BY RaftNode              │
│    ├─→ Sends periodic heartbeats (every 50ms)                   │
│    ├─→ Started by RaftNode.onBecomeLeader()                     │
│    └─→ Stopped by RaftNode.onBecomeFollower()                   │
│                                                                   │
│  RaftReplicationManager                                           │
│    ├─→ Replicates log entries to followers                       │
│    ├─→ Tracks nextIndex/matchIndex                              │
│    ├─→ Updates commitIndex                                       │
│    └─→ Triggered by command submission                           │
│                                                                   │
│  RaftAppendEntriesHandler                                         │
│    ├─→ Handles incoming AppendEntries RPCs                       │
│    ├─→ Checks log consistency                                    │
│    ├─→ Appends/truncates entries                                 │
│    └─→ Updates commitIndex                                       │
│                                                                   │
│  RaftStateMachineApplier                                          │
│    ├─→ Applies committed entries to state machine                │
│    ├─→ Monitors commitIndex > lastApplied                        │
│    └─→ Sequential, in-order application                          │
└───────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────┐
│                   STATE MANAGEMENT                                │
├───────────────────────────────────────────────────────────────────┤
│  RaftNodeState                                                    │
│    ├─→ currentTerm, votedFor                                    │
│    ├─→ commitIndex, lastApplied                                 │
│    ├─→ currentRole (FOLLOWER/CANDIDATE/LEADER)                  │
│    └─→ RaftLeaderState (nextIndex, matchIndex)                  │
│                                                                   │
│  RaftLog (FileBasedRaftLog)                                       │
│    ├─→ Persistent log storage                                    │
│    └─→ append(), getEntry(), truncate()                         │
│                                                                   │
│  RaftPersistentStateStore                                         │
│    ├─→ Persists term & votedFor                                 │
│    └─→ Atomic file operations                                    │
└───────────────────────────────────────────────────────────────────┘
```

## Role Transition Flow with Heartbeat Integration

```
                    FOLLOWER
                       │
                       │ Election timeout
                       ▼
     ┌──────────► CANDIDATE ◄──────────┐
     │                │                  │
     │                │ Wins election    │ Discovers higher term
     │                ▼                  │ or valid leader
     │            LEADER                 │
     │                │                  │
     └────────────────┴──────────────────┘
          Steps down / higher term


DETAILED: Becoming Leader Flow
═══════════════════════════════

1. RaftElectionManager wins election
   │
   ├─→ state.becomeLeader(peers)
   │
   └─→ electionTimer.stop()

2. RaftNode detects role change
   │
   ├─→ previousRole: CANDIDATE
   ├─→ currentRole: LEADER
   │
   └─→ handleRoleTransition(CANDIDATE, LEADER)

3. RaftNode.onBecomeLeader() ⭐
   │
   ├─→ heartbeatManager.start()          ✅ START HEARTBEATS
   │   └─→ Schedules periodic task
   │       └─→ Every 50ms: sendHeartbeats()
   │
   └─→ replicationManager.replicateToAll()  ✅ INITIAL REPLICATION


DETAILED: Stepping Down Flow
═════════════════════════════

1. Discovers higher term (via RPC response)
   │
   └─→ state.becomeFollower(newTerm, leaderId)

2. RaftNode detects role change
   │
   ├─→ previousRole: LEADER
   ├─→ currentRole: FOLLOWER
   │
   └─→ handleRoleTransition(LEADER, FOLLOWER)

3. RaftNode.onBecomeFollower() ⭐
   │
   ├─→ heartbeatManager.stop()           ✅ STOP HEARTBEATS
   │   └─→ Cancels scheduled task
   │
   ├─→ replicationManager.clear()        ✅ CLEAR STATE
   │
   └─→ electionTimer.start()             ✅ START ELECTION TIMER
```

## Component Communication Matrix

| From Component          | To Component               | Via Method                    | Purpose                          |
|------------------------|----------------------------|------------------------------|----------------------------------|
| RaftElectionManager    | RaftNodeState              | becomeLeader()               | Transition to leader role        |
| RaftNode               | RaftHeartbeatManager       | start()                      | Begin sending heartbeats ⭐      |
| RaftNode               | RaftHeartbeatManager       | stop()                       | Stop sending heartbeats ⭐       |
| RaftHeartbeatManager   | RaftNodeState              | getNextIndex(), getLog()     | Build AppendEntries             |
| RaftHeartbeatManager   | gRPC Client                | sendAppendEntries()          | Send heartbeat RPC              |
| RaftReplicationManager | RaftNodeState              | getLog(), setMatchIndex()    | Replicate and track progress    |
| AppendEntriesHandler   | RaftNodeState              | advanceCommitIndex()         | Update commit on follower       |
| AppendEntriesHandler   | RaftElectionTimer          | reset()                      | Reset timeout on valid RPC      |
| StateMachineApplier    | RaftNodeState              | getCommitIndex()             | Check for committed entries     |
| StateMachineApplier    | RaftStateMachine           | apply()                      | Apply command                   |
| RaftNode               | All Components             | Various lifecycle methods    | Orchestrate startup/shutdown    |

## Lifecycle State Machine

```
           ┌─────────────────────────────────────┐
           │     RaftNode.start()                │
           └──────────────┬──────────────────────┘
                          │
                          ▼
           ┌──────────────────────────────────┐
           │  Initialize all components       │
           │  • Load persistent state         │
           │  • Create executors              │
           │  • Wire dependencies             │
           └──────────────┬───────────────────┘
                          │
                          ▼
           ┌──────────────────────────────────┐
           │  Start components                │
           │  • stateMachineApplier.start()  │
           │  • electionTimer.start()        │
           │  • Start role monitor           │
           └──────────────┬───────────────────┘
                          │
                          ▼
           ┌──────────────────────────────────────┐
           │  Monitor role changes (every 50ms)  │
           │                                      │
           │  if (role changed):                 │
           │    handleRoleTransition()           │
           │                                      │
           │    LEADER   → start heartbeats ⭐   │
           │    FOLLOWER → stop heartbeats       │
           │    CANDIDATE→ stop heartbeats       │
           └──────────────┬───────────────────────┘
                          │
                          │ (runtime operation)
                          │
                          ▼
           ┌──────────────────────────────────┐
           │     RaftNode.stop()              │
           │  • Stop all timers               │
           │  • Stop heartbeats               │
           │  • Shutdown executors            │
           └──────────────────────────────────┘
```

## Key Design Decisions

### 1. Centralized Orchestration
**Decision**: Create RaftNode as single entry point
**Rationale**: Simplifies component coordination, ensures consistent lifecycle
**Benefit**: No component forgets to start/stop dependent components

### 2. Role Change Monitoring
**Decision**: Poll role every 50ms instead of callbacks
**Rationale**: Simpler, avoids callback complexity, decouples components
**Benefit**: Easier to test, no circular dependencies

### 3. Automatic Heartbeat Management
**Decision**: RaftNode automatically starts/stops heartbeat manager
**Rationale**: Critical for correctness - leader must send heartbeats
**Benefit**: Cannot forget to start heartbeats, prevents split-brain

### 4. Separation of Concerns
**Decision**: Each component has single, clear responsibility
**Rationale**: Easier to understand, test, and modify
**Benefit**: High cohesion, low coupling

## Testing Strategy

### Unit Tests (per component)
- Mock dependencies
- Test component in isolation
- Focus on logic correctness

### Integration Tests (RaftNodeTest)
- Test component interaction
- Verify lifecycle management
- **Critical**: Verify heartbeat starts on leader transition
- Test role transitions trigger correct callbacks

### Future: End-to-End Tests
- Multi-node cluster simulation
- Network partition scenarios
- Crash recovery scenarios

## Summary

The **RaftNode orchestrator** is the missing piece that ties everything together:

✅ **Automatic lifecycle management**: Components start/stop at right times
✅ **Role-based behavior**: Different components active in different roles
✅ **Heartbeat integration**: Automatically starts when becoming leader ⭐
✅ **Clean API**: Simple interface hides complex orchestration
✅ **Tested**: Comprehensive test suite verifies all transitions
✅ **Production ready**: Proper error handling, logging, resource cleanup

This architecture makes it **impossible to forget** to start the heartbeat manager - it's fully automated!

