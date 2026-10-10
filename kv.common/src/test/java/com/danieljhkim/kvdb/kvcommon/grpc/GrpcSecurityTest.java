package com.danieljhkim.kvdb.kvcommon.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.Server;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class GrpcSecurityTest {

    @Test
    void developmentListenersBindOnlyIpv4LoopbackForEveryServerRole() throws Exception {
        for (GrpcIdentity.Role role : new GrpcIdentity.Role[] {
            GrpcIdentity.Role.COORDINATOR, GrpcIdentity.Role.STORAGE_NODE, GrpcIdentity.Role.GATEWAY
        }) {
            assertListener(GrpcSecurityConfig.development(role, "listener-test"), "127.0.0.1");
        }
    }

    @Test
    void explicitContainerOverrideBindsWildcard() throws Exception {
        GrpcSecurityConfig config = GrpcSecurityConfig.internal(
                GrpcIdentity.Role.COORDINATOR,
                Map.of(
                        "KVDB_GRPC_SECURITY_MODE", "development-plaintext",
                        "KVDB_ENV", "test",
                        "KVDB_BIND_ADDRESS", "0.0.0.0"));
        assertListener(config, "0.0.0.0");
    }

    private static void assertListener(GrpcSecurityConfig config, String expectedHost) throws Exception {
        Server server = GrpcSecurity.serverBuilder(0, config).build().start();
        try {
            assertEquals(1, server.getListenSockets().size());
            InetSocketAddress address =
                    (InetSocketAddress) server.getListenSockets().getFirst();
            if (config.bindAddress().isAnyLocalAddress()) {
                // Netty may report the equivalent IPv6 wildcard on a dual-stack host.
                assertTrue(address.getAddress().isAnyLocalAddress());
            } else {
                assertEquals(expectedHost, address.getAddress().getHostAddress());
            }
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress("127.0.0.1", address.getPort()), 2000);
                assertTrue(socket.isConnected());
            }
        } finally {
            server.shutdownNow();
            assertTrue(server.awaitTermination(5, TimeUnit.SECONDS));
        }
    }
}
