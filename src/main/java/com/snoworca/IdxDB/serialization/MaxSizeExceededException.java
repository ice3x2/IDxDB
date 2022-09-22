package com.snoworca.IdxDB.serialization;

public class MaxSizeExceededException extends RuntimeException {
    MaxSizeExceededException(String message) {
        super(message);
    }
}
