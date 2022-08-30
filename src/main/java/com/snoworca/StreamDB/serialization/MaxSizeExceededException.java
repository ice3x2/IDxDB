package com.snoworca.StreamDB.serialization;

public class MaxSizeExceededException extends RuntimeException {
    MaxSizeExceededException(String message) {
        super(message);
    }
}
