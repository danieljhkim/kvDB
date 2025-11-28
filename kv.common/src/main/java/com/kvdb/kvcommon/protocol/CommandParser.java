package com.kvdb.kvcommon.protocol;

public abstract class CommandParser {

    public static final String OK_RESPONSE = "OK";
    public static final String ERROR_RESPONSE = "ERROR";
    public static final String NIL_RESPONSE = "(nil)";
    protected CommandExecutor executor;

    public CommandParser(CommandExecutor executor) {
        this.executor = executor;
    }

    public CommandParser() {
        this(null);
    }

    /** Format an error message with consistent prefix */
    public static String formatError(String message) {
        return "ERR: " + message;
    }

    /** Format a success message with OK prefix */
    public static String formatOk(String message) {
        return OK_RESPONSE + (message.isEmpty() ? "" : ": " + message);
    }

    public CommandExecutor getCommandExecutor() {
        return executor;
    }

    public void setCommandExecutor(CommandExecutor executor) {
        this.executor = executor;
    }

    public abstract String getHelpText();

    public abstract String executeCommand(String[] args, CommandExecutor executor);

    public String process(String[] parts, CommandExecutor executor) {
        if (parts.length == 0) return formatError("Empty command");
        return executeCommand(parts, executor);
    }
}
