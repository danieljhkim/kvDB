package com.danieljhkim.kvdb.kvclient.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.danieljhkim.kvdb.kvclient.utils.Constants;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
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
