package com.ch.htable;

public class HBaseEntityNotFoundException extends HBaseException {

    public HBaseEntityNotFoundException() {
    }

    public HBaseEntityNotFoundException(String message) {
        super(message);
    }

    public HBaseEntityNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public HBaseEntityNotFoundException(Throwable cause) {
        super(cause);
    }

    public HBaseEntityNotFoundException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
