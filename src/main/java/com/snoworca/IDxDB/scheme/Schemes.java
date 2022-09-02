package com.snoworca.IDxDB.scheme;

import com.snoworca.IDxDB.exception.UnserializableTypeException;
import com.snoworca.IDxDB.serialization.SerializableTypeTable;

import java.util.concurrent.ConcurrentHashMap;

public class Schemes {

    private ConcurrentHashMap<String, SerializableTypeTable> tableMapByClassName = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, SerializableTypeTable> tableMapByName = new ConcurrentHashMap<>();
    private CommitLoadDelegator onCommitLoadDelegator;


    public final static Schemes newInstance(CommitLoadDelegator commitLoadDelegator) {
        Schemes schemes = new Schemes();
        schemes.onCommitLoadDelegator = commitLoadDelegator;
        return schemes;
    }

    public SerializableTypeTable<?> getTableByName(String name) {
        return tableMapByName.get(name);
    }

    public SerializableTypeTable<?> getTableByClassName(String className) {
        return tableMapByClassName.get(className);
    }

    public void newScheme(Class<?> type) {
        SerializableTypeTable<?> typeTable = SerializableTypeTable.newTable(type);
        if(typeTable == null) {
            throw new UnserializableTypeException(type);
        }
        tableMapByClassName.put(typeTable.getType().getName(), typeTable);
        tableMapByName.put(typeTable.getName(), typeTable);
    }

    public void commit() {

    }

    public void clear() {
        tableMapByClassName.clear();
        tableMapByName.clear();

    }

    public void load() {

    }


    public static interface CommitLoadDelegator {
        public void onCommit(byte[] buffer);
        public byte[] getLoad();
    }


}
