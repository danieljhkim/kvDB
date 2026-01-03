package com.danieljhkim.kvdb.kvgateway.cache;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ShardRoutingFailureTrackerTest {

    @Test
    void recordAndExpire_notLeader() throws Exception {
        ShardRoutingFailureTracker tracker = new ShardRoutingFailureTracker(25);

        tracker.recordNotLeader("shard-1");
        assertTrue(tracker.isRecentlyNotLeader("shard-1"));

        Thread.sleep(35);
        assertFalse(tracker.isRecentlyNotLeader("shard-1"));
    }
}
