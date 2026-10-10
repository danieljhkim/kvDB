package com.danieljhkim.kvdb.kvclient.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.danieljhkim.kvdb.kvclient.utils.Constants;
import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class CLIClientTest {

    @Test
    void commandIsNewlineFramedAndEndMarkerIsNotReturned() throws Exception {
        try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            CompletableFuture<Void> exchange = CompletableFuture.runAsync(() -> serveOneCommand(server));
            CLIClient client = new CLIClient();

            assertTrue(client.connect(server.getInetAddress().getHostAddress(), server.getLocalPort()));
            assertEquals("stored", client.sendCommand("KV", "SET", "alpha", "beta"));
            client.disconnect();

            exchange.get(5, TimeUnit.SECONDS);
            assertFalse(client.isConnected());
        }
    }

    @Test
    void disconnectedClientFailsClosedAndPortsAreValidated() throws IOException {
        CLIClient client = new CLIClient();

        assertEquals("Error: Not connected to server", client.sendCommand("KV PING"));
        assertThrows(IllegalArgumentException.class, () -> client.connect("localhost", 0));
        assertThrows(IllegalArgumentException.class, () -> client.connect("localhost", 65_536));
        assertThrows(NullPointerException.class, () -> client.connect(null, 7000));
    }

    @Test
    void responseSplitAcrossLinesIsReadToEndMarkerWithoutLeakingIntoNextCommand() throws Exception {
        CountDownLatch partOneConsumed = new CountDownLatch(1);
        try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            CompletableFuture<Void> exchange =
                    CompletableFuture.runAsync(() -> serveSplitResponses(server, partOneConsumed));
            CLIClient client = new CLIClient() {
                @Override
                String readResponseLine() throws IOException {
                    String line = super.readResponseLine();
                    if ("part-one".equals(line)) {
                        partOneConsumed.countDown();
                    }
                    return line;
                }
            };

            assertTrue(client.connect(server.getInetAddress().getHostAddress(), server.getLocalPort()));
            assertEquals("part-one\npart-two", client.sendCommand("KV", "GET", "alpha"));
            assertEquals("second", client.sendCommand("KV", "GET", "beta"));
            client.disconnect();

            exchange.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void eofBeforeEndMarkerFailsAndDisconnects() throws Exception {
        try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            CompletableFuture<Void> exchange = CompletableFuture.runAsync(() -> serveTruncatedResponse(server));
            CLIClient client = new CLIClient();

            assertTrue(client.connect(server.getInetAddress().getHostAddress(), server.getLocalPort()));
            assertThrows(EOFException.class, () -> client.sendCommand("KV", "PING"));
            assertFalse(client.isConnected());

            exchange.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void responseTimeoutBeforeEndMarkerFailsAndDisconnects() throws Exception {
        CountDownLatch clientFinished = new CountDownLatch(1);
        try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            CompletableFuture<Void> exchange =
                    CompletableFuture.runAsync(() -> serveStalledResponse(server, clientFinished));
            CLIClient client = new CLIClient();

            try {
                assertTrue(client.connect(server.getInetAddress().getHostAddress(), server.getLocalPort()));
                assertThrows(SocketTimeoutException.class, () -> client.sendCommand("KV", "PING"));
                assertFalse(client.isConnected());
            } finally {
                clientFinished.countDown();
            }

            exchange.get(5, TimeUnit.SECONDS);
        }
    }

    private static void serveSplitResponses(ServerSocket server, CountDownLatch partOneConsumed) {
        try (Socket socket = server.accept();
                BufferedReader reader =
                        new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))) {
            OutputStream out = socket.getOutputStream();
            assertEquals("KV GET alpha", reader.readLine());
            out.write("part-one\n".getBytes(StandardCharsets.UTF_8));
            out.flush();
            // The tail and the next frame go out only after the client has read part one, so they cannot share its
            // buffered read.
            assertTrue(partOneConsumed.await(5, TimeUnit.SECONDS));
            out.write(("part-two\n" + Constants.END_MARKER + "\nsecond\n" + Constants.END_MARKER + "\n")
                    .getBytes(StandardCharsets.UTF_8));
            out.flush();
            assertEquals("KV GET beta", reader.readLine());
        } catch (IOException | InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    private static void serveTruncatedResponse(ServerSocket server) {
        try (Socket socket = server.accept();
                BufferedReader reader =
                        new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))) {
            assertEquals("KV PING", reader.readLine());
            socket.getOutputStream().write("partial\n".getBytes(StandardCharsets.UTF_8));
            socket.getOutputStream().flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void serveStalledResponse(ServerSocket server, CountDownLatch clientFinished) {
        try (Socket socket = server.accept();
                BufferedReader reader =
                        new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))) {
            assertEquals("KV PING", reader.readLine());
            socket.getOutputStream().write("partial\n".getBytes(StandardCharsets.UTF_8));
            socket.getOutputStream().flush();
            assertTrue(clientFinished.await(5, TimeUnit.SECONDS));
        } catch (IOException | InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    private static void serveOneCommand(ServerSocket server) {
        try (Socket socket = server.accept();
                BufferedReader reader =
                        new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))) {
            assertEquals("KV SET alpha beta", reader.readLine());
            socket.getOutputStream().write(("stored\n" + Constants.END_MARKER + "\n").getBytes(StandardCharsets.UTF_8));
            socket.getOutputStream().flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
