package com.kvdb.kvcommon.exception;

/** Base exception class for system-level issues in the cluster. */
public abstract class ServerException extends RuntimeException {

    /**
     * Constructs a new system exception with the specified message.
     *
     * @param message the detail message
     */
    public ServerException(String message) {
        super(message);
    }

    /**
     * Constructs a new system exception with the specified message and cause.
     *
     * @param message the detail message
     * @param cause the cause of the exception
     */
    public ServerException(String message, Throwable cause) {
        super(message, cause);
    }
}
