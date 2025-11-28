package com.kvdb.kvcommon.exception;

/** Exception thrown when no healthy nodes are available in the cluster for processing requests. */
public class NoHealthyNodesAvailable extends CodeRedException {

    private static final String DEFAULT_MESSAGE = "No healthy nodes available in the cluster";

    public NoHealthyNodesAvailable() {
        super(DEFAULT_MESSAGE);
    }

    public NoHealthyNodesAvailable(String message) {
        super(message);
    }

    public NoHealthyNodesAvailable(Throwable cause) {
        super(DEFAULT_MESSAGE, cause);
    }

    public NoHealthyNodesAvailable(String message, Throwable cause) {
        super(message, cause);
    }
}
