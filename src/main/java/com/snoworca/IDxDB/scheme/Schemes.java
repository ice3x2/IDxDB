package com.snoworca.IDxDB.scheme;

import com.snoworca.IDxDB.serialization.TypeSerializableTable;

import java.io.File;

public class Schemes {

    public final static Schemes newInstance(File file) {

        return null;
    }

    public TypeSerializableTable<?> getTableByName(String name) {
        return null;
    }

    public TypeSerializableTable<?> getTableByClassName(String className) {
        return null;
    }

    public void newScheme(Class<?> type) {

    }

    public void commit() {

    }

    public void load() {

    }

    public void close() {

    }


}
