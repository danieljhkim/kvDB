package com.danieljhkim.kvdb.kvclustercoordinator.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.danieljhkim.kvdb.kvclustercoordinator.converter.ProtoConverter;
import com.danieljhkim.kvdb.kvcommon.cache.ShardMapCache;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Pins the coordinator's ResolveShard mapping to the data plane's: {@link ShardMapCache} is what the gateway and the
 * storage nodes' ShardRouter delegate to.
 */
class ShardMapSnapshotRoutingTest {

    private static final int NUM_SHARDS = 8;

    private static final Object[][] GOLDEN = {
        {new byte[0], "shard-0"},
        {"a".getBytes(StandardCharsets.UTF_8), "shard-0"},
        {"map1".getBytes(StandardCharsets.UTF_8), "shard-6"},
        {"map2".getBytes(StandardCharsets.UTF_8), "shard-7"},
        {"map3".getBytes(StandardCharsets.UTF_8), "shard-0"},
        {"map5".getBytes(StandardCharsets.UTF_8), "shard-2"},
        {"map10".getBytes(StandardCharsets.UTF_8), "shard-2"},
        {"fo3".getBytes(StandardCharsets.UTF_8), "shard-1"},
        {"user:12345".getBytes(StandardCharsets.UTF_8), "shard-5"},
        {new byte[] {0x00, (byte) 0xFF, (byte) 0xFE, 'k'}, "shard-5"},
        {new byte[] {(byte) 0x80}, "shard-7"},
        {
            new byte[] {
                (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF
            },
            "shard-1"
        },
    };

    @Test
    void coordinatorResolutionMatchesDataPlaneForFixedKeys() {
        ClusterState state = new ClusterState();
        state.registerNode("node-a", "localhost:9001", null);
        state.initializeShards(NUM_SHARDS, 1);
        ShardMapSnapshot snapshot = state.createSnapshot();

        ShardMapCache dataPlane = new ShardMapCache();
        dataPlane.refreshFromFullState(ProtoConverter.toProto(snapshot));

        for (Object[] row : GOLDEN) {
            byte[] key = (byte[]) row[0];
            String expected = (String) row[1];
            assertEquals(expected, dataPlane.resolveShardId(key), "data plane");
            assertEquals(expected, snapshot.resolveShardIdForKey(key), "coordinator shard id");
            ShardRecord shard = snapshot.resolveShardForKey(key);
            assertNotNull(shard);
            assertEquals(expected, shard.shardId(), "coordinator shard record");
        }
    }

    @Test
    void shardRecordsNoLongerPublishKeyRange() {
        ClusterState state = new ClusterState();
        state.initializeShards(NUM_SHARDS, 1);
        var proto = ProtoConverter.toProto(state.createSnapshot());
        proto.getShardsMap().values().forEach(shard -> assertFalse(shard.hasKeyRange()));
    }
}
