package com.snoworca.IdxDB.exception;

import com.snoworca.cson.CSONObject;

public class MissingIndexValueException extends RuntimeException {
    public MissingIndexValueException(String indexKey, CSONObject csonObject) {
        super("Missing value of index '" + indexKey + "' in '" + csonObject.toString() + "'");

    }
}
