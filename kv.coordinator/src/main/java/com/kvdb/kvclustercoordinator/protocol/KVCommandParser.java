package com.kvdb.kvclustercoordinator.protocol;


import com.kvdb.kvcommon.protocol.CommandExecutor;

public class KVCommandParser {

    private static final String HELP_TEXT =
            """
        KV Command Parser Usage:
        SET [key] [value] - Store a key-value pair
        GET [key] - Retrieve value for a given key
        DEL [key] - Remove a key-value pair
        PING - Check connection
        SHUTDOWN [node_id]- Shut down the node
        HELP/INFO - Display this help message""";


    public String executeCommand(String[] parts, CommandExecutor executor) {
        if (parts.length == 0) return "ERR: Empty command";
        String cmd = parts[0].trim().toUpperCase();
        return switch (cmd) {
            case "HELP", "INFO" -> getHelpText();
            case "SET" -> handleSet(parts, executor);
            case "GET" -> handleGet(parts, executor);
            case "DEL" -> handleDelete(parts, executor);
            case "SHUTDOWN" -> handleShutdown(parts, executor);
            case "PING" -> handlePing();
            default -> "ERR: Unknown command";
        };
    }

    private String handleSet(String[] parts, CommandExecutor executor) {
        if (parts.length != 3) return "ERR: Usage: SET key value";
        return String.valueOf(executor.put(parts[1], parts[2]));
    }

    private String handleGet(String[] parts, CommandExecutor executor) {
        if (parts.length != 2) return "ERR: Usage: GET key";
        String value = executor.get(parts[1]);
        return value != null ? value : "(nil)";
    }

    private String handleDelete(String[] parts, CommandExecutor executor) {
        if (parts.length != 2) return "ERR: Usage: DEL key";
        return String.valueOf(executor.delete(parts[1]));
    }

    private String handleExists(String[] parts, CommandExecutor executor) {
        if (parts.length != 2) return "ERR: Usage: EXISTS key";
        return executor.exists(parts[1]) ? "1" : "0";
    }

    private String handleShutdown(String[] parts, CommandExecutor executor) {
        if (parts.length != 2) return "ERR: Usage: SHUTDOWN node_id";
        try {
            return executor.shutdown();
        } catch (UnsupportedOperationException e) {
            return "ERR: SHUTDOWN operation not supported";
        }
    }

    private String handlePing() {
        return "PONG";
    }

    public String getHelpText() {
        return HELP_TEXT;
    }
}
