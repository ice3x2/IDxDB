package com.snoworca.IDxDB.serialization;

public class MaxSizeExceededException extends RuntimeException {
    MaxSizeExceededException(String message) {
        super(message);
    }
}
