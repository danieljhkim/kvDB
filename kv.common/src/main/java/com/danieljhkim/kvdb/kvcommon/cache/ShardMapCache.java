package com.danieljhkim.kvdb.kvcommon.cache;

import com.danieljhkim.kvdb.proto.coordinator.ClusterState;
import com.danieljhkim.kvdb.proto.coordinator.NodeRecord;
import com.danieljhkim.kvdb.proto.coordinator.ShardMapDelta;
import com.danieljhkim.kvdb.proto.coordinator.ShardRecord;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Node-local cache of the coordinator shard map.
 *
 * <p>
 * Used to validate whether this node is a replica/leader for a shard before accepting writes.
 */
public class ShardMapCache implements Consumer<ShardMapDelta> {

    private static final Logger logger = LoggerFactory.getLogger(ShardMapCache.class);
    private final AtomicReference<ClusterState> stateRef = new AtomicReference<>();

    public boolean refreshFromFullState(ClusterState state) {
        if (state == null) {
            return false;
        }
        ClusterState old = stateRef.get();
        if (old == null || state.getMapVersion() >= old.getMapVersion()) {
            stateRef.set(state);
            return true;
        }
        return false;
    }

    @Override
    public void accept(ShardMapDelta delta) {
        if (delta == null) {
            return;
        }
        if (delta.getNewMapVersion() == 0) {
            return; // heartbeat
        }
        if (delta.hasFullState()) {
            boolean updated = refreshFromFullState(delta.getFullState());
            if (updated) {
                logger.debug(
                        "Applied full-state delta version={}",
                        delta.getFullState().getMapVersion());
            }
        }
        // incremental deltas are handled by forcing a full refresh in the watch client
    }

    public long getMapVersion() {
        ClusterState s = stateRef.get();
        return s != null ? s.getMapVersion() : 0;
    }

    public boolean isInitialized() {
        return stateRef.get() != null;
    }

    public String resolveShardId(byte[] key) {
        ClusterState state = stateRef.get();
        if (state == null || state.getShardsCount() == 0) {
            throw new IllegalStateException("No shard map available");
        }

        int numShards = state.getPartitioning().getNumShards();
        if (numShards == 0) {
            numShards = state.getShardsCount();
        }

        int hash = hashKey(key);
        int shardIndex = Math.floorMod(hash, numShards);
        return "shard-" + shardIndex;
    }

    public ShardRecord getShard(String shardId) {
        ClusterState state = stateRef.get();
        if (state == null) {
            return null;
        }
        return state.getShardsMap().get(shardId);
    }

    public boolean isReplica(String shardId, String nodeId) {
        ShardRecord shard = getShard(shardId);
        if (shard == null) {
            return false;
        }
        return shard.getReplicasList().contains(nodeId);
    }

    public Optional<String> getLeaderNodeId(String shardId) {
        ShardRecord shard = getShard(shardId);
        if (shard == null || shard.getLeader().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(shard.getLeader());
    }

    public Optional<String> getLeaderAddress(String shardId) {
        NodeRecord leaderNode = getLeaderNode(shardId);
        if (leaderNode == null || leaderNode.getAddress().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(leaderNode.getAddress());
    }

    public Optional<String> getAnyReplicaAddress(String shardId) {
        List<NodeRecord> replicas = getReplicaNodes(shardId);
        for (NodeRecord replica : replicas) {
            replica.getAddress();
            if (!replica.getAddress().isEmpty()) {
                return Optional.of(replica.getAddress());
            }
        }
        return Optional.empty();
    }

    public NodeRecord getLeaderNode(String shardId) {
        ClusterState state = stateRef.get();
        ShardRecord shard = getShard(shardId);
        if (state == null || shard == null || shard.getLeader().isEmpty()) {
            return null;
        }
        NodeRecord leaderNode = state.getNodesMap().get(shard.getLeader());
        return leaderNode;
    }

    public List<NodeRecord> getReplicaNodes(String shardId) {
        ClusterState state = stateRef.get();
        ShardRecord shard = getShard(shardId);
        if (state == null || shard == null) {
            return List.of();
        }
        return shard.getReplicasList().stream()
                .map(replicaId -> state.getNodesMap().get(replicaId))
                .filter(node -> node != null)
                .toList();
    }

    public Optional<String> getNodeAddress(String nodeId) {
        ClusterState state = stateRef.get();
        if (state == null) {
            return Optional.empty();
        }
        NodeRecord node = state.getNodesMap().get(nodeId);
        if (node == null || node.getAddress().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(node.getAddress());
    }

    private int hashKey(byte[] key) {
        if (key == null || key.length == 0) {
            return 0;
        }
        int result = 1;
        for (byte b : key) {
            result = 31 * result + b;
        }
        return result;
    }
}
