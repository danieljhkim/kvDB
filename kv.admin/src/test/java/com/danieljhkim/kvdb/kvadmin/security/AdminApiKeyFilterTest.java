package com.danieljhkim.kvdb.kvadmin.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.servlet.DispatcherType;
import jakarta.servlet.FilterChain;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class AdminApiKeyFilterTest {

    private static final String API_KEY = "expected-api-key";

    @Test
    void correctKeyContinuesAndWrongMissingBlankAndDifferentLengthKeysAreRejected() throws Exception {
        FilterResult accepted = filter(API_KEY);
        assertEquals(HttpServletResponse.SC_OK, accepted.status());
        assertTrue(accepted.continued());

        assertRejected("wrong-api-key");
        assertRejected(null);
        assertRejected(" ");
        assertRejected("short");
        assertRejected("a much longer incorrect api key");
    }

    private static void assertRejected(String provided) throws Exception {
        FilterResult rejected = filter(provided);
        assertEquals(HttpServletResponse.SC_UNAUTHORIZED, rejected.status());
        assertTrue(rejected.body().contains("\"error\":\"invalid_api_key\""));
        assertFalse(rejected.continued());
    }

    private static FilterResult filter(String provided) throws Exception {
        AtomicInteger status = new AtomicInteger(HttpServletResponse.SC_OK);
        StringWriter body = new StringWriter();
        AtomicBoolean continued = new AtomicBoolean();

        HttpServletRequest request = (HttpServletRequest) Proxy.newProxyInstance(
                HttpServletRequest.class.getClassLoader(),
                new Class<?>[] {HttpServletRequest.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getHeader" -> "X-Admin-Api-Key".equals(args[0]) ? provided : null;
                    case "getRequestURI" -> "/admin/test";
                    case "getDispatcherType" -> DispatcherType.REQUEST;
                    case "getAttribute" -> null;
                    case "setAttribute", "removeAttribute" -> null;
                    default -> null;
                });

        HttpServletResponse response = (HttpServletResponse) Proxy.newProxyInstance(
                HttpServletResponse.class.getClassLoader(),
                new Class<?>[] {HttpServletResponse.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "setStatus" -> {
                        status.set((Integer) args[0]);
                        yield null;
                    }
                    case "getWriter" -> new PrintWriter(body);
                    default -> null;
                });

        FilterChain chain = (requestArg, responseArg) -> continued.set(true);
        new AdminApiKeyFilter(API_KEY).doFilter(request, response, chain);
        return new FilterResult(status.get(), body.toString(), continued.get());
    }

    private record FilterResult(int status, String body, boolean continued) {}
}
