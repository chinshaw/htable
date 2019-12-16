package com.ch.htable;

public class HBaseException extends RuntimeException {

    public HBaseException() {
    }

    public HBaseException(String message) {
        super(message);
    }

    public HBaseException(String message, Throwable cause) {
        super(message, cause);
    }
    public HBaseException(Throwable cause) {
        super(cause);
    }

    public HBaseException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
