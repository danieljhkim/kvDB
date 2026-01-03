package com.danieljhkim.kvdb.kvadmin.security;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Objects;

final class Cidr {

    private final InetAddress network;
    private final int prefixLen;
    private final int totalBits;
    private final BigInteger mask;
    private final BigInteger netMasked;

    private Cidr(InetAddress network, int prefixLen) {
        this.network = network;
        this.prefixLen = prefixLen;
        this.totalBits = network.getAddress().length * 8;
        if (prefixLen < 0 || prefixLen > totalBits) {
            throw new IllegalArgumentException("CIDR prefix out of range: " + prefixLen);
        }
        this.mask = prefixToMask(totalBits, prefixLen);
        this.netMasked = toBigInt(network).and(mask);
    }

    static Cidr parse(String s) {
        Objects.requireNonNull(s, "cidr");
        String trimmed = s.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("CIDR cannot be blank");
        }

        String ipPart;
        int prefix;
        int slash = trimmed.indexOf('/');
        if (slash < 0) {
            ipPart = trimmed;
            // Exact host match (/32 or /128)
            prefix = -1;
        } else {
            ipPart = trimmed.substring(0, slash).trim();
            String prefixStr = trimmed.substring(slash + 1).trim();
            if (ipPart.isEmpty() || prefixStr.isEmpty()) {
                throw new IllegalArgumentException("Invalid CIDR entry: " + trimmed);
            }
            try {
                prefix = Integer.parseInt(prefixStr);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid CIDR prefix: " + prefixStr + " in " + trimmed, e);
            }
        }

        InetAddress ip;
        try {
            ip = InetAddress.getByName(ipPart);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("Invalid IP in allowlist: " + ipPart, e);
        }

        int bits = ip.getAddress().length * 8;
        int effectivePrefix = (prefix == -1) ? bits : prefix;
        return new Cidr(ip, effectivePrefix);
    }

    boolean contains(InetAddress candidate) {
        if (candidate == null) {
            return false;
        }
        if (candidate.getAddress().length != network.getAddress().length) {
            return false; // IPv4 vs IPv6 mismatch
        }
        BigInteger candMasked = toBigInt(candidate).and(mask);
        return candMasked.equals(netMasked);
    }

    private static BigInteger toBigInt(InetAddress addr) {
        byte[] bytes = addr.getAddress();
        return new BigInteger(1, bytes);
    }

    private static BigInteger prefixToMask(int totalBits, int prefixLen) {
        if (prefixLen == 0) {
            return BigInteger.ZERO;
        }
        BigInteger allOnes = BigInteger.ONE.shiftLeft(totalBits).subtract(BigInteger.ONE);
        BigInteger right = BigInteger.ONE.shiftLeft(totalBits - prefixLen).subtract(BigInteger.ONE);
        return allOnes.xor(right);
    }
}
