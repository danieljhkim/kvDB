package com.danieljhkim.kvdb.kvcommon.sharding;

/**
 * The single key-to-shard function shared by the coordinator, gateway and storage nodes.
 *
 * <p>
 * The mapping is {@code floorMod(h, numShards)} where {@code h} is a {@code 31 * h + b} rolling hash seeded with 1 over
 * the <em>signed</em> key bytes (empty or null keys hash to 0). Stored data is placed by this function, so changing it
 * moves existing data and requires a designed migration. The pinned golden values in {@code ShardKeyMapperTest} guard
 * against accidental change.
 *
 * <p>
 * Because the mapping is modulo-based, a shard owns an interleaved set of hashes rather than a contiguous range; the
 * shard map therefore does not publish key ranges.
 */
public final class ShardKeyMapper {

    private static final String SHARD_ID_PREFIX = "shard-";

    private ShardKeyMapper() {}

    /** Hashes the key bytes; empty or null keys hash to 0. */
    public static int hashKey(byte[] key) {
        if (key == null || key.length == 0) {
            return 0;
        }
        int result = 1;
        for (byte b : key) {
            result = 31 * result + b;
        }
        return result;
    }

    /** Returns the shard index in {@code [0, numShards)} owning the key. */
    public static int shardIndex(byte[] key, int numShards) {
        if (numShards <= 0) {
            throw new IllegalArgumentException("numShards must be positive: " + numShards);
        }
        return Math.floorMod(hashKey(key), numShards);
    }

    /** Returns the shard ID (for example {@code shard-3}) owning the key. */
    public static String shardId(byte[] key, int numShards) {
        return SHARD_ID_PREFIX + shardIndex(key, numShards);
    }
}
