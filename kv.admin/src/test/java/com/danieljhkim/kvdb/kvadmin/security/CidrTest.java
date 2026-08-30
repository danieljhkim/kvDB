package com.danieljhkim.kvdb.kvadmin.security;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetAddress;
import org.junit.jupiter.api.Test;

class CidrTest {

    @Test
    void ipv4AndIpv6RangesEnforceNetworkBoundaries() throws Exception {
        Cidr privateNetwork = Cidr.parse("10.20.0.0/16");
        Cidr loopbackV6 = Cidr.parse("::1/128");

        assertTrue(privateNetwork.contains(InetAddress.getByName("10.20.255.254")));
        assertFalse(privateNetwork.contains(InetAddress.getByName("10.21.0.1")));
        assertFalse(privateNetwork.contains(InetAddress.getByName("::1")));
        assertTrue(loopbackV6.contains(InetAddress.getByName("::1")));
        assertFalse(loopbackV6.contains(InetAddress.getByName("::2")));
    }

    @Test
    void invalidOrAmbiguousEntriesFailClosed() {
        assertThrows(IllegalArgumentException.class, () -> Cidr.parse(" "));
        assertThrows(IllegalArgumentException.class, () -> Cidr.parse("10.0.0.0/33"));
        assertThrows(IllegalArgumentException.class, () -> Cidr.parse("10.0.0.0/not-a-prefix"));
    }
}
