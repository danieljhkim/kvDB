package com.kvdb.kvdbserver.protocol;


import com.kvdb.kvcommon.protocol.CommandExecutor;
import com.kvdb.kvcommon.protocol.CommandParser;
import com.kvdb.kvdbserver.repository.BaseRepository;
import com.kvdb.kvdbserver.repository.KVDataBaseRepository;

public class SQLCommandParser extends CommandParser {

    private static final String HELP_TEXT =
            """
        SQL Command Parser Usage:
        SQL INIT [table_name] - Initialize a new table (default if no name given)
        SQL USE [table_name] - Switch to an existing table
        SQL GET [key] - Retrieve value for a given key
        SQL SET [key] [value] - Store a key-value pair
        SQL DEL/DELETE [key] - Remove a key-value pair
        SQL CLEAR/DROP/TRUNCATE - Remove all entries from the current table
        SQL PING - Check connection to database
        SQL HELP/INFO - Display this help message""";

    private static final String ERR_NOT_INITIALIZED = "ERR: Not initialized. Run SQL INIT [table_name] first.";
    private static final String ERR_USAGE_INIT = "ERR: Usage: INIT [table_name]";
    private static final String ERR_USAGE_USE = "ERR: Usage: USE [table_name]";
    private static final String ERR_USAGE_GET = "ERR: Usage: GET [key]";
    private static final String ERR_USAGE_SET = "ERR: Usage: SET [key] [value]";
    private static final String ERR_USAGE_DEL = "ERR: Usage: DEL [key]";
    private static final String ERR_INSUFFICIENT_ARGS = "ERR: Insufficient arguments for SQL command";

    public SQLCommandParser(CommandExecutor executor) {
        super(executor);
    }
    
    public SQLCommandParser(BaseRepository repo) {
        super(new BaseRepositoryAdapter(repo));
    }

    public SQLCommandParser() {
        super(null); // No default initialization - require explicit INIT
    }

    @Override
    public String executeCommand(String[] parts) {
        // Guard against insufficient arguments
        if (parts.length < 2) {
            return ERR_INSUFFICIENT_ARGS;
        }
        
        String cmd = parts[1].trim().toUpperCase();
        
        // Require explicit initialization for non-INIT/non-HELP commands
        if (!cmd.equals("INIT") && !cmd.equals("HELP") && !cmd.equals("INFO") && executor == null) {
            return ERR_NOT_INITIALIZED;
        }
        
        return switch (cmd) {
            case "HELP", "INFO" -> HELP_TEXT;
            case "INIT" -> handleInit(parts);
            case "USE" -> handleUse(parts);
            case "GET" -> handleGet(parts);
            case "SET" -> handleSet(parts);
            case "DEL", "DELETE" -> handleDelete(parts);
            case "CLEAR", "DROP", "TRUNCATE" -> handleClear();
            case "PING" -> handlePing();
            default -> "ERR: Unknown SQL command";
        };
    }

    private String handleInit(String[] parts) {
        if (parts.length == 3) {
            KVDataBaseRepository repo = new KVDataBaseRepository(parts[2]);
            this.executor = new BaseRepositoryAdapter(repo);
            return "OK: Table initialized: " + executor.getTableName();
        }
        return ERR_USAGE_INIT;
    }

    private String handleUse(String[] parts) {
        if (parts.length != 3) return ERR_USAGE_USE;
        executor.initialize(parts[2]);
        return "OK: Table initialized: " + executor.getTableName();
    }

    private String handleGet(String[] parts) {
        if (parts.length != 3) return ERR_USAGE_GET;
        String value = executor.get(parts[2]);
        return value != null ? value : NIL_RESPONSE;
    }

    private String handleSet(String[] parts) {
        if (parts.length != 4) return ERR_USAGE_SET;
        return String.valueOf(executor.put(parts[2], parts[3]));
    }

    private String handleDelete(String[] parts) {
        if (parts.length != 3) return ERR_USAGE_DEL;
        return String.valueOf(executor.delete(parts[2]));
    }

    private String handleClear() {
        try {
            int res = executor.truncate();
            if (res == 0) return "ERR: No keys to delete";
            return "OK: " + executor.getTableName() + " cleared";
        } catch (UnsupportedOperationException e) {
            return "ERR: CLEAR operation not supported";
        }
    }

    private String handlePing() {
        return executor.isHealthy() ? "PONG" : "ERR: No connection";
    }

    @Override
    public String getHelpText() {
        return HELP_TEXT;
    }
}
