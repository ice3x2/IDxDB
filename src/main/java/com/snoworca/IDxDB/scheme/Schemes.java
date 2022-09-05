package com.snoworca.IDxDB.scheme;

import com.snoworca.IDxDB.exception.UnserializableTypeException;
import com.snoworca.IDxDB.serialization.FieldInfo;
import com.snoworca.IDxDB.serialization.SerializableTypeTable;
import com.snoworca.cson.CSONArray;
import com.snoworca.cson.CSONObject;

import java.util.ArrayList;
import java.util.Collection;
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
        Collection<SerializableTypeTable> tables = tableMapByClassName.values();
        CSONArray csonArray = new CSONArray();
        for(SerializableTypeTable table : tables) {
            CSONObject csonObject = serializableTypeTableToCSonObject(table);
            csonArray.add(csonObject);
        }
        byte[] buffer = csonArray.toByteArray();
        onCommitLoadDelegator.onCommit(buffer);
    }

    private CSONObject serializableTypeTableToCSonObject(SerializableTypeTable table) {
        CSONObject csonObject = new CSONObject();
        csonObject.put("class", table.getType().getName());
        csonObject.put("name", table.getName());
        csonObject.put("ver", table.getVersion());
        CSONArray fieldsArray = new CSONArray();
        ArrayList<FieldInfo> fieldInfoList = table.getFieldInfoList();
        for(int i = 0, n = fieldInfoList.size(); i < n; ++i) {
            FieldInfo fieldInfo = fieldInfoList.get(i);
            String nickname = fieldInfo.getName();
            String fieldName = fieldInfo.getField().getName();
            byte fieldType = fieldInfo.getType();
            boolean isArray  = fieldInfo.isArray();
            boolean isCollection = fieldInfo.isCollection();
            byte componentType = fieldInfo.getComponentType();
            CSONObject fieldCSON = new CSONObject().put("name", nickname).put("nickname", fieldName)
                    .put("type", fieldType).put("isArray", isArray).put("isCollection", isCollection).put("componentType", componentType);
            fieldsArray.add(fieldCSON);
        }
        csonObject.put("fieldList", fieldsArray);
        return csonObject;
    }


    public void clear() {
        tableMapByClassName.clear();
        tableMapByName.clear();

    }

    public void load() {
        byte[] buffer = onCommitLoadDelegator.getSchemeBuffer();
        ConcurrentHashMap<String, SerializableTypeTable> tableMapByClassName = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, SerializableTypeTable> tableMapByName = new ConcurrentHashMap<>();
        CSONArray csonArray = CSONArray.parse(buffer);
        for(int i = 0, n = csonArray.size(); i < n; ++i) {
            CSONObject csonObject = csonArray.optObject(i);
            if(csonObject == null) continue;;
            try {
                Class<?> type = Class.forName(csonObject.optString("class"));
                SerializableTypeTable<?> table = SerializableTypeTable.newTable(type);
                //TODO 마이그레이션 구현해야함.
                tableMapByClassName.put(type.getName(), table);
                tableMapByName.put(table.getName(), table);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        this.tableMapByClassName = tableMapByClassName;
        this.tableMapByName = tableMapByName;
    }


    public static interface CommitLoadDelegator {
        public void onCommit(byte[] buffer);
        public byte[] getSchemeBuffer();


    }


}
