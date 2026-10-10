package com.danieljhkim.kvdb.kvcommon.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.Server;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class KVGrpcServerTest {

    @Test
    void runningIsSetAfterStartAndClearedAfterTermination() throws Exception {
        LatchedGrpcServer grpcServer = new LatchedGrpcServer();
        KVGrpcServer server = newServer(grpcServer);
        assertFalse(server.isRunning());

        AtomicReference<Throwable> startFailure = new AtomicReference<>();
        Thread starter = startInBackground(server, startFailure);
        try {
            grpcServer.awaitingTermination.await();
            assertTrue(server.isRunning());

            grpcServer.terminate();
            starter.join();
            assertNull(startFailure.get());
            assertFalse(server.isRunning());
        } finally {
            grpcServer.terminate();
            starter.join();
        }
    }

    @Test
    void runningIsClearedByShutdown() throws Exception {
        LatchedGrpcServer grpcServer = new LatchedGrpcServer();
        KVGrpcServer server = newServer(grpcServer);

        AtomicReference<Throwable> startFailure = new AtomicReference<>();
        Thread starter = startInBackground(server, startFailure);
        try {
            grpcServer.awaitingTermination.await();
            assertTrue(server.isRunning());

            server.shutdown();
            assertFalse(server.isRunning());
            starter.join();
            assertNull(startFailure.get());
            assertFalse(server.isRunning());
        } finally {
            grpcServer.terminate();
            starter.join();
        }
    }

    @Test
    void failedStartDoesNotPublishRunningState() throws Exception {
        LatchedGrpcServer grpcServer = new LatchedGrpcServer();
        grpcServer.startFailure = new IOException("bind failed");
        KVGrpcServer server = newServer(grpcServer);

        IOException thrown = assertThrows(IOException.class, server::start);

        assertSame(grpcServer.startFailure, thrown);
        assertFalse(server.isRunning());
        assertEquals(1, grpcServer.awaitingTermination.getCount());
    }

    private static Thread startInBackground(KVGrpcServer server, AtomicReference<Throwable> failure) {
        Thread starter = new Thread(
                () -> {
                    try {
                        server.start();
                    } catch (Throwable t) {
                        failure.set(t);
                    }
                },
                "kv-grpc-server-start-test");
        starter.start();
        return starter;
    }

    private static KVGrpcServer newServer(Server grpcServer) throws ReflectiveOperationException {
        KVGrpcServer server = new KVGrpcServer.Builder().setPort(0).build();
        Field field = KVGrpcServer.class.getDeclaredField("server");
        field.setAccessible(true);
        field.set(server, grpcServer);
        return server;
    }

    /**
     * Test double whose termination is driven by latches rather than by timing. {@link #awaitingTermination} counts
     * down once start() has reached awaitTermination(); {@link #terminate()} releases that wait as a real server
     * would when it is shut down or terminated externally.
     */
    private static final class LatchedGrpcServer extends Server {

        private final CountDownLatch awaitingTermination = new CountDownLatch(1);
        private final CountDownLatch terminated = new CountDownLatch(1);
        private volatile IOException startFailure;

        @Override
        public Server start() throws IOException {
            if (startFailure != null) {
                throw startFailure;
            }
            return this;
        }

        @Override
        public Server shutdown() {
            terminated.countDown();
            return this;
        }

        @Override
        public Server shutdownNow() {
            terminated.countDown();
            return this;
        }

        @Override
        public boolean isShutdown() {
            return terminated.getCount() == 0;
        }

        @Override
        public boolean isTerminated() {
            return terminated.getCount() == 0;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return terminated.await(timeout, unit);
        }

        @Override
        public void awaitTermination() throws InterruptedException {
            awaitingTermination.countDown();
            terminated.await();
        }

        void terminate() {
            terminated.countDown();
        }
    }
}
