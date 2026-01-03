package com.danieljhkim.kvdb.kvcommon.handler;

import java.net.Socket;

/**
 * Factory interface for creating ClientHandler instances. Implementations can provide different types of handlers based
 * on requirements.
 */
public interface ClientHandlerFactory {

    /**
     * Creates a new Runnable client handler for the given client socket.
     *
     * @param clientSocket the client connection socket
     * @return a Runnable that handles the client connection
     */
    Runnable createHandler(Socket clientSocket);
}
