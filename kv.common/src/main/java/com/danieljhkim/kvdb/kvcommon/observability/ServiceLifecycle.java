package com.danieljhkim.kvdb.kvcommon.observability;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** Tracks whether a server can admit new RPCs and waits for admitted RPCs to drain. */
public final class ServiceLifecycle {

    private final AtomicBoolean accepting = new AtomicBoolean(true);
    private final AtomicInteger inFlight = new AtomicInteger();
    private final Object monitor = new Object();

    public boolean isAccepting() {
        return accepting.get();
    }

    public int inFlight() {
        return inFlight.get();
    }

    public boolean tryAdmit() {
        if (!accepting.get()) {
            return false;
        }
        inFlight.incrementAndGet();
        if (!accepting.get()) {
            complete();
            return false;
        }
        return true;
    }

    public void complete() {
        int remaining = inFlight.decrementAndGet();
        if (remaining <= 0) {
            synchronized (monitor) {
                monitor.notifyAll();
            }
        }
    }

    public void beginDrain() {
        accepting.set(false);
    }

    public boolean awaitDrain(Duration budget) throws InterruptedException {
        long deadline = System.nanoTime() + budget.toNanos();
        synchronized (monitor) {
            while (inFlight.get() > 0) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    return false;
                }
                TimeUnit.NANOSECONDS.timedWait(monitor, remaining);
            }
        }
        return true;
    }
}
