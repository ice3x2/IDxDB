package com.snoworca.StreamDB.serialization;

public class InvalidTypeException extends RuntimeException {
    InvalidTypeException(String message) {
        super(message);
    }
}
