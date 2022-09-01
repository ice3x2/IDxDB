package com.snoworca.IDxDB.exception;

public class UnserializableTypeException extends RuntimeException {

    UnserializableTypeException(Class<?> type) {
        super("'" + type.getName() + "' class must use the '@Serializable' annotation to enable serialization.");
    }



}
