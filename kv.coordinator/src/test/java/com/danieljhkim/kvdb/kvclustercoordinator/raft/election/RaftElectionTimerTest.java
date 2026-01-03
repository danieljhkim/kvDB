package com.danieljhkim.kvdb.kvclustercoordinator.raft.election;

import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class RaftElectionTimerTest {

    private ScheduledExecutorService scheduler;
    private RaftConfiguration config;
    private AtomicInteger electionCallbackCount;
    private RaftElectionTimer timer;

    @BeforeEach
    void setUp() {
        scheduler = Executors.newScheduledThreadPool(1);
        config = RaftConfiguration.builder()
                .nodeId("test-node")
                .clusterMembers(Map.of("test-node", "localhost:5000"))
                .electionTimeoutMin(Duration.ofMillis(100))
                .electionTimeoutMax(Duration.ofMillis(200))
                .heartbeatInterval(Duration.ofMillis(50))
                .build();
        electionCallbackCount = new AtomicInteger(0);
        timer = new RaftElectionTimer("test-node", config, scheduler, electionCallbackCount::incrementAndGet);
    }

    @AfterEach
    void tearDown() {
        timer.stop();
        scheduler.shutdown();
    }

    @Test
    void testTimerStartsAndFires() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        timer = new RaftElectionTimer("test-node", config, scheduler, latch::countDown);

        timer.start();
        assertTrue(timer.isRunning(), "Timer should be running after start");

        boolean fired = latch.await(300, TimeUnit.MILLISECONDS);
        assertTrue(fired, "Election timeout should fire within max timeout");
    }

    @Test
    void testTimerReset() throws InterruptedException {
        timer.start();
        assertTrue(timer.isRunning());

        Thread.sleep(50);
        timer.reset();

        assertTrue(timer.isRunning(), "Timer should still be running after reset");
        assertEquals(0, electionCallbackCount.get(), "Callback should not have been called yet");
    }

    @Test
    void testTimerStop() {
        timer.start();
        assertTrue(timer.isRunning());

        timer.stop();
        assertFalse(timer.isRunning(), "Timer should not be running after stop");
    }

    @Test
    void testMultipleResets() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        timer = new RaftElectionTimer("test-node", config, scheduler, latch::countDown);

        timer.start();

        // Reset multiple times before timeout
        for (int i = 0; i < 3; i++) {
            Thread.sleep(50);
            timer.reset();
        }

        assertEquals(0, electionCallbackCount.get(), "Callback should not fire due to resets");
        assertTrue(timer.isRunning(), "Timer should still be running");
    }

    @Test
    void testRandomTimeout() {
        long min = config.getElectionTimeoutMin().toMillis();
        long max = config.getElectionTimeoutMax().toMillis();

        timer.start();
        Duration remaining = timer.getRemaining();

        long remainingMs = remaining.toMillis();
        assertTrue(remainingMs >= 0 && remainingMs <= max,
                "Remaining time should be between 0 and max timeout");
    }
}

