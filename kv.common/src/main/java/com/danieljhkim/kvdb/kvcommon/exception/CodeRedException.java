package com.danieljhkim.kvdb.kvcommon.exception;

/**
 * Exception thrown when the system is in a critical RED status indicating severe operational issues that prevent normal
 * functioning.
 */
public class CodeRedException extends ServerException {

    private static final String DEFAULT_MESSAGE = "CODE RED - critical failure detected. Immediate attention required.";

    public CodeRedException() {
        super(DEFAULT_MESSAGE);
    }

    public CodeRedException(String message) {
        super(message);
    }

    public CodeRedException(Throwable cause) {
        super(DEFAULT_MESSAGE, cause);
    }

    public CodeRedException(String message, Throwable cause) {
        super(message, cause);
    }
}
