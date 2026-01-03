package com.danieljhkim.kvdb.kvadmin.security;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

final class JsonError {
    private JsonError() {}

    static void write(jakarta.servlet.http.HttpServletResponse resp, int status, String error, String path)
            throws IOException {
        resp.setStatus(status);
        resp.setCharacterEncoding(StandardCharsets.UTF_8.name());
        resp.setContentType("application/json");
        String safePath = path == null ? "" : path.replace("\"", "");
        String body = "{\"error\":\"" + error + "\",\"path\":\"" + safePath + "\"}";
        resp.getWriter().write(body);
        resp.getWriter().flush();
    }
}
