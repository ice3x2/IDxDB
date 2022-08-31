package com.snoworca.IDxDB.serialization;

public class InvalidTypeException extends RuntimeException {
    InvalidTypeException(String message) {
        super(message);
    }
}
