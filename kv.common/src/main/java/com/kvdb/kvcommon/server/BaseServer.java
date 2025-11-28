package com.kvdb.kvcommon.server;

public interface BaseServer {
    /**
     * Starts the server.
     *
     * @throws Exception if an error occurs while starting the server
     */
    void start() throws Exception;

    /** Stops the server. */
    void shutdown();

    /**
     * Checks if the server is running.
     *
     * @return true if the server is running, false otherwise
     */
    boolean isRunning();
}
