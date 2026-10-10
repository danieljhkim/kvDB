package com.danieljhkim.kvdb.kvnode.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.danieljhkim.kvdb.kvcommon.cache.ShardMapCache;
import com.danieljhkim.kvdb.kvcommon.sharding.ShardKeyMapper;
import com.danieljhkim.kvdb.proto.coordinator.ClusterState;
import com.danieljhkim.kvdb.proto.coordinator.PartitioningConfig;
import com.danieljhkim.kvdb.proto.coordinator.ShardRecord;
import com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/** Pins the node-side routing to the same golden shards the coordinator's ResolveShard returns. */
class ShardRouterGoldenTest {

    @Test
    void routesFixedKeysToPinnedShards() {
        ClusterState.Builder state = ClusterState.newBuilder()
                .setMapVersion(1)
                .setPartitioning(PartitioningConfig.newBuilder().setNumShards(8).build());
        for (int i = 0; i < 8; i++) {
            state.putShards(
                    "shard-" + i,
                    ShardRecord.newBuilder().setShardId("shard-" + i).build());
        }
        ShardMapCache cache = new ShardMapCache();
        cache.refreshFromFullState(state.build());
        ShardRouter router = new ShardRouter(cache, "node-a");

        assertEquals("shard-0", router.resolveShardId(ByteString.EMPTY));
        assertEquals("shard-6", router.resolveShardId("map1"));
        assertEquals("shard-7", router.resolveShardId("map2"));
        assertEquals("shard-0", router.resolveShardId("map3"));
        assertEquals("shard-2", router.resolveShardId("map5"));
        assertEquals("shard-1", router.resolveShardId("fo3"));
        assertEquals("shard-5", router.resolveShardId("user:12345"));
        assertEquals(
                "shard-5",
                router.resolveShardId(ByteString.copyFrom(new byte[] {0x00, (byte) 0xFF, (byte) 0xFE, 'k'})));
        assertEquals("shard-7", router.resolveShardId(ByteString.copyFrom(new byte[] {(byte) 0x80})));

        byte[] key = "map10".getBytes(StandardCharsets.UTF_8);
        assertEquals(ShardKeyMapper.shardId(key, 8), router.resolveShardId("map10"));
    }
}
