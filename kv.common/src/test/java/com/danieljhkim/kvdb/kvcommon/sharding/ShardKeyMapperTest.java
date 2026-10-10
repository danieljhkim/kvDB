package com.danieljhkim.kvdb.kvcommon.sharding;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Golden values for the key-to-shard function. Stored data is placed by this mapping, so these values must not change
 * without a documented data migration.
 */
class ShardKeyMapperTest {

    /** Fixed keys: hash, shard index with 8 shards, shard index with 3 shards. */
    static final Object[][] GOLDEN = {
        {new byte[0], 0, 0, 0},
        {bytes("a"), 128, 0, 2},
        {bytes("map1"), 4267478, 6, 2},
        {bytes("map2"), 4267479, 7, 0},
        {bytes("map3"), 4267480, 0, 1},
        {bytes("map5"), 4267482, 2, 0},
        {bytes("map10"), 132291866, 2, 2},
        {bytes("fo3"), 131305, 1, 1},
        {bytes("user:12345"), -984387963, 5, 0},
        {new byte[] {0x00, (byte) 0xFF, (byte) 0xFE, 'k'}, 922605, 5, 0},
        {new byte[] {(byte) 0x80}, -97, 7, 2},
        {
            new byte[] {
                (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF
            },
            -172384639,
            1,
            2
        },
        {"héllo-世界".getBytes(StandardCharsets.UTF_8), 387042087, 7, 0},
    };

    @Test
    void pinsHashAndShardForFixedKeys() {
        for (Object[] row : GOLDEN) {
            byte[] key = (byte[]) row[0];
            assertEquals((int) row[1], ShardKeyMapper.hashKey(key), "hash");
            assertEquals((int) row[2], ShardKeyMapper.shardIndex(key, 8), "8 shards");
            assertEquals((int) row[3], ShardKeyMapper.shardIndex(key, 3), "3 shards");
            assertEquals("shard-" + row[2], ShardKeyMapper.shardId(key, 8));
        }
    }

    @Test
    void nullKeyHashesLikeEmptyKey() {
        assertEquals(0, ShardKeyMapper.hashKey(null));
        assertEquals("shard-0", ShardKeyMapper.shardId(null, 8));
    }

    @Test
    void shardIndexIsNeverNegative() {
        // 31 * h + b overflows to Integer.MIN_VALUE-adjacent values for long keys; floorMod must stay in range.
        byte[] key = new byte[64];
        java.util.Arrays.fill(key, (byte) 0xFF);
        for (int shards = 1; shards <= 64; shards++) {
            int index = ShardKeyMapper.shardIndex(key, shards);
            assertEquals(true, index >= 0 && index < shards);
        }
    }

    @Test
    void rejectsNonPositiveShardCount() {
        assertThrows(IllegalArgumentException.class, () -> ShardKeyMapper.shardIndex(bytes("k"), 0));
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
