package com.snoworca.IdxDB.exception;

public class DataBlockParseException extends RuntimeException {

    public DataBlockParseException(String message, Throwable cause) {
        super(message,cause);
    }

    public DataBlockParseException(String message) {
        super(message);
    }
}
