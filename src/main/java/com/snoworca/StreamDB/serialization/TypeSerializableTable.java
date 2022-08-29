package com.snoworca.StreamDB.serialization;

import com.snoworca.IDxDB.data.DataType;
import com.snoworca.IDxDB.util.ByteBufferOutputStream;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.*;

public class TypeSerializableTable<T> {

    private Class<T> type = null;
    private String name = null;
    private int bufferSize = 1024;

    private ArrayList<FieldInfo> fieldInfoList  = null;


    private TypeSerializableTable() {

    }


    public static <T> TypeSerializableTable newTable(Class<T> type) {
        TypeSerializableTable typeSerializableTable = new TypeSerializableTable();
        Serializable serializable =type.getAnnotation(Serializable.class);
        if(serializable == null) return null;
        typeSerializableTable.name = !serializable.value().isEmpty() ? serializable.value() :
                                     !serializable.name().isEmpty() ? serializable.name() : type.getName();

        typeSerializableTable.bufferSize = serializable.bufferSize();
        typeSerializableTable.type = type;
        typeSerializableTable.initFields(type);

        return typeSerializableTable;
    }

    private void initFields(Class<T> type) {
        Class<?> targetType = type;
        HashSet<String> fieldNames = new HashSet<>();
        ArrayList<FieldInfo> fieldInfoList = new ArrayList<>();
        do {
            Field[] fields = targetType.getDeclaredFields();
            for(int i = 0; i < fields.length; ++i) {
                Field field = fields[i];
                field.setAccessible(true);
                SerializeField serializeFieldAnnotation = field.getAnnotation(SerializeField.class);
                if(serializeFieldAnnotation == null) continue;
                String fieldName = serializeFieldAnnotation.value();

                if(fieldName.isEmpty()) fieldName = field.getName();
                if(fieldName.isEmpty() || fieldNames.contains(fieldName)) continue;
                fieldNames.add(fieldName);

                FieldInfo fieldInfo = new FieldInfo(field, fieldName);
                if(fieldInfo.type < 0) {
                    continue;
                }
                fieldInfoList.add(fieldInfo);
            }
            targetType = (Class<?>)targetType.getSuperclass();
        } while (targetType != Object.class);
        Collections.sort(fieldInfoList);
        this.fieldInfoList = fieldInfoList;
    }

    private static class FieldInfo implements Comparable {
        FieldInfo(Field field, String name) {
            this.field = field;
            this.name = name;
            this.type = DataType.getDataType(this.field.getType());
        }
        byte type;

        Field field;
        String name;

        @Override
        public int compareTo(Object o) {
            FieldInfo info = (FieldInfo)o;
            return name.compareTo(info.name);
        }
    }


    public ByteBuffer serialize(T obj) throws IllegalAccessException {
        Serializer serializer = new Serializer(bufferSize);
        for(int i = 0, n = fieldInfoList.size(); i < n; ++i) {
            FieldInfo fieldInfo = fieldInfoList.get(i);
            switch (fieldInfo.type) {
                case DataType.TYPE_BYTE:
                    serializer.put(fieldInfo.field.getByte(obj));
                    break;
                case DataType.TYPE_BOOLEAN:
                    serializer.put(fieldInfo.field.getBoolean(obj) ? 1 : 0);
                    break;
                case DataType.TYPE_SHORT:
                    serializer.put(fieldInfo.field.getShort(obj));
                    break;
                case DataType.TYPE_CHAR:
                    serializer.put((char)fieldInfo.field.getChar(obj));
                    break;
                case DataType.TYPE_INT:
                    serializer.put(fieldInfo.field.getInt(obj));
                    break;
                case DataType.TYPE_FLOAT:
                    serializer.put(fieldInfo.field.getFloat(obj));
                    break;
                case DataType.TYPE_LONG:
                    serializer.put(fieldInfo.field.getLong(obj));
                    break;
                case DataType.TYPE_STRING:
                    serializer.put((String)fieldInfo.field.get(obj));
                    break;
                case DataType.TYPE_BYTE_ARRAY:
                    serializer.put((byte[])fieldInfo.field.get(obj));
                    break;
            }

        }
        return serializer.getByteBuffer();
    }

    public T deserialize(List<ByteBuffer> bufferList) throws Exception, InvocationTargetException, InstantiationException, IllegalAccessException {
        Deserializer deserializer = new Deserializer(bufferList);
        Constructor<T> constructor = type.getDeclaredConstructor();
        constructor.setAccessible(true);
        T obj = constructor.newInstance();
        for(int i = 0, n = fieldInfoList.size(); i < n; ++i) {
            FieldInfo fieldInfo = fieldInfoList.get(i);
            switch (fieldInfo.type) {
                case DataType.TYPE_BYTE:
                     fieldInfo.field.set(obj, deserializer.getByte());
                    break;
                case DataType.TYPE_BOOLEAN:
                    fieldInfo.field.set(obj, deserializer.getBoolean());
                    break;
                case DataType.TYPE_SHORT:
                    fieldInfo.field.set(obj,deserializer.getShort());
                    break;
                case DataType.TYPE_CHAR:
                    fieldInfo.field.set(obj,deserializer.getChar());
                    break;
                case DataType.TYPE_INT:
                    fieldInfo.field.set(obj,deserializer.getInt());
                    break;
                case DataType.TYPE_FLOAT:
                    fieldInfo.field.set(obj,deserializer.getFloat());
                    break;
                case DataType.TYPE_LONG:
                    fieldInfo.field.set(obj,deserializer.getLong());
                    break;
                case DataType.TYPE_STRING:
                    fieldInfo.field.set(obj,deserializer.getString());
                    break;
                case DataType.TYPE_BYTE_ARRAY:
                    fieldInfo.field.set(obj,deserializer.getBuffer());
                    break;
            }

        }



        return obj;
    }

    private static Annotation findAnnotation(Annotation[] annotations,Class<? extends  Annotation> annotationType ) {
        for(int i = 0, n = annotations.length; i < n; ++i) {
            if(annotations[i].annotationType() == annotationType) {
                return annotations[i];
            }
        }
        return null;
    }

}
