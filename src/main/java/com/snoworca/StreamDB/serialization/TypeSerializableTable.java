package com.snoworca.StreamDB.serialization;

import com.snoworca.IDxDB.data.DataType;

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
                if(!initPrimitiveField(field, fieldNames, fieldInfoList)) {
                    initStringField(field, fieldNames, fieldInfoList);
                }
            }
            targetType = (Class<?>)targetType.getSuperclass();
        } while (targetType != Object.class);
        Collections.sort(fieldInfoList);
        this.fieldInfoList = fieldInfoList;
    }

    private boolean initArrayField(Field field, HashSet<String> fieldNames, ArrayList<FieldInfo> fieldInfoList) {
        ArrayColumn arrayColumnAnnotation = field.getAnnotation(ArrayColumn.class);
        if(arrayColumnAnnotation == null) return false;
        Class<?> fieldType = field.getType();
        if(!fieldType.isArray()) {
            throw new InvalidTypeException("@ArrayColumn annotation cannot be used for the field '" + field.getName() + "' of the '" + type.getName() +"' class.");
        }
        String fieldName = arrayColumnAnnotation.name();
        if(fieldName.isEmpty()) fieldName = field.getName();
        if(fieldName.isEmpty() || fieldNames.contains(fieldName)) return false;
        fieldNames.add(fieldName);

        Class<?> componentType = fieldType.getComponentType();
        StringColumn stringColumn = null;
        if(componentType.isAssignableFrom(String.class)) {
            stringColumn = componentType.getAnnotation(StringColumn.class);
            if(stringColumn == null) {
                //TODO 여기서 예외 발생.
            }
        }
        FieldInfo fieldInfo = new FieldInfo(field, fieldName);
        if(fieldInfo.type < 0 || fieldInfo.componentType < 0) {
            //TODO 여기서도 예외 발생 시켜야한다.
            return false;
        }
        fieldInfo.arraySize = arrayColumnAnnotation.maxSize();
        fieldInfo.cutOverSizeOfArray = arrayColumnAnnotation.cutOverSize();
        if(stringColumn != null) {
            fieldInfo.size = stringColumn.maxSize();
            fieldInfo.cutOverSize = stringColumn.cutOverSize();
        }
        fieldInfoList.add(fieldInfo);
        return true;
    }


    private boolean initPrimitiveField(Field field, HashSet<String> fieldNames, ArrayList<FieldInfo> fieldInfoList) {
        PrimitiveColumn primitiveColumnAnnotation = field.getAnnotation(PrimitiveColumn.class);
        if(primitiveColumnAnnotation == null) return false;
        if(!field.getType().isPrimitive()) {
            throw new InvalidTypeException("@PrimitiveColumn annotation cannot be used for the field '" + field.getName() + "' of the '" + type.getName() +"' class.");
        }
        String fieldName = primitiveColumnAnnotation.value();
        if(fieldName.isEmpty()) fieldName = field.getName();
        if(fieldName.isEmpty() || fieldNames.contains(fieldName)) return false;
        fieldNames.add(fieldName);

        FieldInfo fieldInfo = new FieldInfo(field, fieldName);
        if(fieldInfo.type < 0) {
            return false;
        }
        fieldInfoList.add(fieldInfo);
        return true;
    }

    private boolean initStringField(Field field, HashSet<String> fieldNames, ArrayList<FieldInfo> fieldInfoList) {
        StringColumn stringColumnAnnotation = field.getAnnotation(StringColumn.class);
        if(stringColumnAnnotation == null) return false;
        if(!field.getType().isAssignableFrom(String.class)) {
            throw new InvalidTypeException("@StringColumn annotation cannot be used for the field '" + field.getName() + "' of the '" + type.getName() +"' class.");
        }
        String fieldName = stringColumnAnnotation.name();
        if(fieldName.isEmpty()) fieldName = field.getName();
        if(fieldName.isEmpty() || fieldNames.contains(fieldName)) return false;
        fieldNames.add(fieldName);

        FieldInfo fieldInfo = new FieldInfo(field, fieldName);
        if(fieldInfo.type < 0) {
            return false;
        }
        fieldInfo.size = stringColumnAnnotation.maxSize();
        fieldInfo.isPrimitive = false;
        fieldInfo.cutOverSize = stringColumnAnnotation.cutOverSize();
        fieldInfoList.add(fieldInfo);
        return true;
    }


    private static class FieldInfo implements Comparable {
        FieldInfo(Field field, String name) {
            this.field = field;
            this.name = name;
            Class<?> type = this.field.getType();
            this.type = DataType.getDataType(type);
            if(type.isArray()) {
                componentType = DataType.getDataType(type.getComponentType());
                try {
                    fieldOfArrayLength = type.getField("length");
                } catch (NoSuchFieldException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        boolean isArray = false;
        int arraySize = 0;
        boolean cutOverSizeOfArray = false;
        byte type;
        byte componentType;
        boolean isPrimitive = true;
        boolean cutOverSize = true;

        Field fieldOfArrayLength;
        Field field;
        String name;

        int size = 0;

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
                    serializer.putByte(fieldInfo.field.getByte(obj));
                    System.out.print("byte->");
                    break;
                case DataType.TYPE_BOOLEAN:
                    serializer.putBoolean(fieldInfo.field.getBoolean(obj));
                    System.out.print("boolean->");
                    break;
                case DataType.TYPE_SHORT:
                    serializer.putShort(fieldInfo.field.getShort(obj));
                    System.out.print("short->");
                    break;
                case DataType.TYPE_CHAR:
                    serializer.putCharacter(fieldInfo.field.getChar(obj));
                    System.out.print("char->");
                    break;
                case DataType.TYPE_INT:
                    serializer.putInteger(fieldInfo.field.getInt(obj));
                    System.out.print("integer->");
                    break;
                case DataType.TYPE_FLOAT:
                    serializer.putFloat(fieldInfo.field.getFloat(obj));
                    System.out.print("float(" + fieldInfo.name  + ")->");
                    break;
                case DataType.TYPE_LONG:
                    serializer.putLong(fieldInfo.field.getLong(obj));
                    System.out.print("long->");
                    break;
                case DataType.TYPE_DOUBLE:
                    serializer.putDouble(fieldInfo.field.getDouble(obj));
                    System.out.print("double->");
                    break;
                case DataType.TYPE_STRING:
                    String value = (String)fieldInfo.field.get(obj);
                    if(value.length() > fieldInfo.size) {
                        throw new MaxSizeExceededException("String size exceeded the length set in '" + fieldInfo.field.getName()  +"' field variable of '" + type.getName() + "' class. (" + value.length()  + "<" + fieldInfo.size + ")(x)");
                    }
                    serializer.putString(value, fieldInfo.size);
                    break;
                case DataType.TYPE_ARRAY:
                    Object arrayObject = fieldInfo.field.get(obj);
                    int length = (int)fieldInfo.fieldOfArrayLength.get(arrayObject);
                    serializer.putInteger(length);
                    //TODO 배열 요소들을 꺼내서 버퍼에 넣어야한다.
                    break;
            }
        }
        System.out.println("end");
        ByteBuffer buffer = serializer.getByteBuffer();
        return buffer;
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
                    System.out.print("byte->");
                    break;
                case DataType.TYPE_BOOLEAN:
                    fieldInfo.field.set(obj, deserializer.getBoolean());
                    System.out.print("boolean->");
                    break;
                case DataType.TYPE_SHORT:
                    fieldInfo.field.set(obj,deserializer.getShort());
                    System.out.print("short->");
                    break;
                case DataType.TYPE_CHAR:
                    fieldInfo.field.set(obj,deserializer.getChar());
                    System.out.print("char->");
                    break;
                case DataType.TYPE_INT:
                    fieldInfo.field.set(obj,deserializer.getInt());
                    System.out.print("int->");
                    break;
                case DataType.TYPE_FLOAT:
                    fieldInfo.field.set(obj,deserializer.getFloat());
                    System.out.print("float->");
                    break;
                case DataType.TYPE_DOUBLE:
                    fieldInfo.field.set(obj,deserializer.getDouble());
                    System.out.print("double(" + fieldInfo.name  + ")->");
                    break;
                case DataType.TYPE_LONG:
                    fieldInfo.field.set(obj,deserializer.getLong());
                    System.out.print("long->");
                    break;
                case DataType.TYPE_STRING:
                    String str = deserializer.getString(fieldInfo.size);
                    fieldInfo.field.set(obj,str);
                    break;
                case DataType.TYPE_BYTE_ARRAY:
                    fieldInfo.field.set(obj,deserializer.getBuffer());
                    break;
            }
        }
        System.out.println("end");



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
