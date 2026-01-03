package com.danieljhkim.kvdb.kvclustercoordinator.raft.election;

import com.danieljhkim.kvdb.kvclustercoordinator.raft.RaftConfiguration;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

/**
 * Manages election timeout for Raft consensus.
 *
 * <p>The election timer triggers an election when no heartbeat is received from the leader
 * within a random timeout period (between electionTimeoutMin and electionTimeoutMax).
 * This randomization helps prevent split votes.
 *
 * <p>Thread-safety: This class is thread-safe.
 */
@Slf4j
public class RaftElectionTimer {

    private final String nodeId;
    private final RaftConfiguration config;
    private final ScheduledExecutorService scheduler;
    private final Runnable electionCallback;
    private final Random random;
    private final AtomicReference<ScheduledFuture<?>> currentTimer;

    /**
     * Creates a new election timer.
     *
     * @param nodeId the ID of this Raft node
     * @param config the Raft configuration
     * @param scheduler the scheduler for timing events
     * @param electionCallback callback to invoke when election timeout fires
     */
    public RaftElectionTimer(
            String nodeId, RaftConfiguration config, ScheduledExecutorService scheduler, Runnable electionCallback) {
        this.nodeId = nodeId;
        this.config = config;
        this.scheduler = scheduler;
        this.electionCallback = electionCallback;
        this.random = new Random();
        this.currentTimer = new AtomicReference<>();
    }

    /**
     * Starts the election timer.
     * If a timer is already running, it will be cancelled and a new one started.
     */
    public void start() {
        reset();
    }

    /**
     * Resets the election timer to a new random timeout.
     * This should be called when:
     * - Receiving a valid AppendEntries from the leader
     * - Granting a vote to a candidate
     * - Starting an election as a candidate
     */
    public void reset() {
        cancel();

        long timeoutMs = randomElectionTimeout();
        ScheduledFuture<?> newTimer = scheduler.schedule(this::onElectionTimeout, timeoutMs, TimeUnit.MILLISECONDS);

        currentTimer.set(newTimer);
        log.trace("[{}] Election timer reset to {} ms", nodeId, timeoutMs);
    }

    /**
     * Stops the election timer.
     * This should be called when transitioning to LEADER state.
     */
    public void stop() {
        cancel();
        log.debug("[{}] Election timer stopped", nodeId);
    }

    /**
     * Cancels the current timer if one is running.
     */
    private void cancel() {
        ScheduledFuture<?> timer = currentTimer.getAndSet(null);
        if (timer != null && !timer.isCancelled()) {
            timer.cancel(false);
        }
    }

    /**
     * Callback invoked when the election timeout fires.
     */
    private void onElectionTimeout() {
        log.debug("[{}] Election timeout fired, triggering election", nodeId);
        try {
            electionCallback.run();
        } catch (Exception e) {
            log.error("[{}] Error during election timeout callback", nodeId, e);
        }
    }

    /**
     * Generates a random election timeout between min and max configured values.
     *
     * @return timeout in milliseconds
     */
    private long randomElectionTimeout() {
        long minMs = config.getElectionTimeoutMin().toMillis();
        long maxMs = config.getElectionTimeoutMax().toMillis();
        long range = maxMs - minMs;
        return minMs + (long) (random.nextDouble() * range);
    }

    /**
     * Checks if the election timer is currently running.
     *
     * @return true if a timer is scheduled
     */
    public boolean isRunning() {
        ScheduledFuture<?> timer = currentTimer.get();
        return timer != null && !timer.isDone() && !timer.isCancelled();
    }

    /**
     * Returns the remaining time until the election timeout fires.
     *
     * @return remaining duration, or Duration.ZERO if not running
     */
    public Duration getRemaining() {
        ScheduledFuture<?> timer = currentTimer.get();
        if (timer == null || timer.isDone()) {
            return Duration.ZERO;
        }
        long remainingMs = timer.getDelay(TimeUnit.MILLISECONDS);
        return Duration.ofMillis(Math.max(0, remainingMs));
    }
}
