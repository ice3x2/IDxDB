package com.snoworca.IdxDB.serialization;

public class InvalidTypeException extends RuntimeException {
    InvalidTypeException(String message) {
        super(message);
    }
}
