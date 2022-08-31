package com.snoworca.StreamDB.serialization;

import com.snoworca.IDxDB.data.DataType;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
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
                initFieldInfo(field, fieldNames, fieldInfoList);

            }
            targetType = (Class<?>)targetType.getSuperclass();
        } while (targetType != Object.class);
        Collections.sort(fieldInfoList);
        this.fieldInfoList = fieldInfoList;
    }



    private boolean initFieldInfo(Field field, HashSet<String> fieldNames, ArrayList<FieldInfo> fieldInfoList) {
        Column columnAnnotaion = field.getAnnotation(Column.class);
        if(columnAnnotaion == null) return false;
        String fieldName = columnAnnotaion.value();
        if(fieldName.isEmpty()) fieldName = field.getName();
        if(fieldName.isEmpty() || fieldNames.contains(fieldName)) return false;
        fieldNames.add(fieldName);
        FieldInfo fieldInfo = new FieldInfo(field, fieldName);
        if(fieldInfo.isError) {
            return false;
        }
        fieldInfoList.add(fieldInfo);
        return true;
    }


    private static class FieldInfo implements Comparable {
        FieldInfo(Field field, String name) {
            this.field = field;
            this.name = name;
            Class<?> type = this.field.getType();
            this.type = DataType.getDataType(type);
            if(this.type < 0) {
                isError = true;
                return;
            }
            this.isPrimitive = type.isPrimitive();
            if(type.isArray()) {
                isArray = true;
                componentType = DataType.getDataType(type.getComponentType());
                if(componentType < 0) {
                    isError = true;
                    //return;
                }
            } else if(Collection.class.isAssignableFrom(type)) {
                try {
                    ParameterizedType integerListType = (ParameterizedType)field.getGenericType();
                    Class<?> componentClass = (Class<?>) integerListType.getActualTypeArguments()[0];
                    componentType = DataType.getDataType(componentClass);
                    if(componentType < 0) {
                        isError = true;
                        return;
                    }
                    if(type.isInterface() && SortedSet.class.isAssignableFrom(type)) {
                        componentTypeConstructor = TreeSet.class.getConstructor();
                    }
                    else if(type.isInterface() && Set.class.isAssignableFrom(type)) {
                        componentTypeConstructor = HashSet.class.getConstructor();
                    }
                    else if(type.isInterface() && (List.class.isAssignableFrom(type) || Collection.class.isAssignableFrom(type))) {
                        componentTypeConstructor = ArrayList.class.getConstructor();
                    }
                    else {
                        componentTypeConstructor = type.getConstructor();
                    }
                } catch (Exception e) {
                    isError = true;
                    //return;
                }

            }
        }
        boolean isError = false;
        boolean isArray = false;
        boolean isCollection = false;

        int arraySize = 0;
        byte type;
        byte componentType;
        Constructor<?> componentTypeConstructor;
        boolean isPrimitive = true;

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
                    serializer.putString(value);
                    break;
                case DataType.TYPE_ARRAY:
                    Object arrayObject = fieldInfo.field.get(obj);
                    serializeArray(fieldInfo, arrayObject, serializer);
                    break;
                case DataType.TYPE_COLLECTION:
                    Object collectionObject = fieldInfo.field.get(obj);
                    serializeCollection(fieldInfo, (Collection<?>)collectionObject, serializer);
                    break;
            }
        }
        System.out.println("end");
        ByteBuffer buffer = serializer.getByteBuffer();
        return buffer;
    }

    private void serializeArray(FieldInfo fieldInfo,Object object, Serializer serializer) {
        if(object == null) {
            serializer.putInteger(-1);
            return;
        }
        switch (fieldInfo.componentType) {
            case DataType.TYPE_BYTE:
                byte[] buffer = (byte[])object;
                serializer.putInteger(buffer.length);
                serializer.putByteArray(buffer, 0, buffer.length);
                break;
            case DataType.TYPE_BOOLEAN:
                boolean[] booleans = (boolean[])object;
                byte[] booleansToBytes = new byte[booleans.length];
                for(int i = 0; i < booleans.length; ++i) {
                    booleansToBytes[i] = (byte) (booleans[i] ? 1 : 0);
                }
                serializer.putInteger(booleans.length);
                serializer.putByteArray(booleansToBytes, 0, booleansToBytes.length);
                break;
            case DataType.TYPE_SHORT:
                short[] shortBuffer = (short[])object;
                serializer.putInteger(shortBuffer.length);
                for(int i = 0; i < shortBuffer.length; ++i) {
                    serializer.putShort(shortBuffer[i]);
                }
                break;
            case DataType.TYPE_CHAR:
                char[] charBuffer = (char[])object;
                serializer.putInteger(charBuffer.length);
                for(int i = 0; i < charBuffer.length; ++i) {
                    serializer.putCharacter(charBuffer[i]);
                }
                break;
            case DataType.TYPE_INT:
                int[] intBuffer = (int[])object;
                serializer.putInteger(intBuffer.length);
                for(int i = 0; i < intBuffer.length; ++i) {
                    serializer.putInteger(intBuffer[i]);
                }
                break;
            case DataType.TYPE_FLOAT:
                float[] floatBuffer = (float[])object;
                serializer.putInteger(floatBuffer.length);
                for(int i = 0; i < floatBuffer.length; ++i) {
                    serializer.putFloat(floatBuffer[i]);
                }
                break;
            case DataType.TYPE_LONG:
                long[] longBuffer = (long[])object;
                serializer.putInteger(longBuffer.length);
                for(int i = 0; i < longBuffer.length; ++i) {
                    serializer.putLong(longBuffer[i]);
                }
                break;
            case DataType.TYPE_DOUBLE:
                double[] doubleBuffer = (double[])object;
                serializer.putInteger(doubleBuffer.length);
                for(int i = 0; i < doubleBuffer.length; ++i) {
                    serializer.putDouble(doubleBuffer[i]);
                }
                break;
            case DataType.TYPE_STRING:
                String[] stringBuffer = (String[])object;
                serializer.putInteger(stringBuffer.length);
                for(int i = 0; i < stringBuffer.length; ++i) {
                    serializer.putString(stringBuffer[i]);
                }
                break;
        }
    }


    private void serializeCollection(FieldInfo fieldInfo,Collection<?> collection, Serializer serializer) {
        if(collection == null) {
            serializer.putInteger(-1);
            return;
        }
        serializer.putInteger(collection.size());
        Iterator<?> iterator = collection.iterator();
        switch (fieldInfo.componentType) {
            case DataType.TYPE_BYTE:
                while(iterator.hasNext()) {
                    serializer.putByte((Byte)iterator.next());
                }
                break;
            case DataType.TYPE_BOOLEAN:
                while(iterator.hasNext()) {
                    serializer.putBoolean((Boolean)iterator.next());
                }
                break;
            case DataType.TYPE_SHORT:
                while(iterator.hasNext()) {
                    serializer.putShort((Short)iterator.next());
                }
                break;
            case DataType.TYPE_CHAR:
                while(iterator.hasNext()) {
                    serializer.putCharacter((Character)iterator.next());
                }
                break;
            case DataType.TYPE_INT:
                while(iterator.hasNext()) {
                    serializer.putInteger((Integer) iterator.next());
                }
                break;
            case DataType.TYPE_FLOAT:
                while(iterator.hasNext()) {
                    serializer.putFloat((Float)iterator.next());
                }
                break;
            case DataType.TYPE_LONG:
                while(iterator.hasNext()) {
                serializer.putLong((Long) iterator.next());
                }
                break;
            case DataType.TYPE_DOUBLE:
                while(iterator.hasNext()) {
                    serializer.putDouble((Double) iterator.next());
                }
                break;
            case DataType.TYPE_STRING:
                while(iterator.hasNext()) {
                    serializer.putString((String) iterator.next());
                }
                break;
        }
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
                    String str = deserializer.getString();
                    fieldInfo.field.set(obj,str);
                    break;
                case DataType.TYPE_ARRAY:
                    fieldInfo.field.set(obj,deserializeArray(fieldInfo, deserializer));
                    break;
                case DataType.TYPE_COLLECTION:
                    fieldInfo.field.set(obj,deserializeCollection(fieldInfo, deserializer));
                    break;
            }
        }
        System.out.println("end");



        return obj;
    }

    private Object deserializeArray(FieldInfo fieldInfo,Deserializer deserializer) {
        int length = deserializer.getInt();
        if(length < 0) return null;
        switch (fieldInfo.componentType) {
            case DataType.TYPE_BYTE:
                byte[] buffer = deserializer.getBuffer(length);
                return buffer;
            case DataType.TYPE_BOOLEAN:
                byte[] booleanBuffer = deserializer.getBuffer(length);
                boolean[] booleans = new boolean[length];
                for(int i = 0; i < length; ++i) {
                    booleans[i] = booleanBuffer[i] == 1;
                }
                return booleans;
            case DataType.TYPE_SHORT:
                short[] shortBuffer = new short[length];
                for(int i = 0; i < length; ++i) {
                    shortBuffer[i] = deserializer.getShort();
                }
                return shortBuffer;
            case DataType.TYPE_CHAR:
                char[] charBuffer = new char[length];
                for(int i = 0; i < length; ++i) {
                    charBuffer[i] = deserializer.getChar();
                }
                return charBuffer;
            case DataType.TYPE_INT:
                int[] intBuffer = new int[length];
                for(int i = 0; i < length; ++i) {
                    intBuffer[i] = deserializer.getInt();
                }
                return intBuffer;
            case DataType.TYPE_FLOAT:
                float[] floatBuffer = new float[length];
                for(int i = 0; i < length; ++i) {
                    floatBuffer[i] = deserializer.getFloat();
                }
                return floatBuffer;
            case DataType.TYPE_LONG:
                long[] longBuffer = new long[length];
                for(int i = 0; i < length; ++i) {
                    longBuffer[i] = deserializer.getLong();
                }
                return longBuffer;
            case DataType.TYPE_DOUBLE:
                double[] doubleBuffer = new double[length];
                for(int i = 0; i < length; ++i) {
                    doubleBuffer[i] = deserializer.getDouble();
                }
                return doubleBuffer;
            case DataType.TYPE_STRING:
                String[] stringBuffer = new String[length];
                for(int i = 0; i < length; ++i) {
                    stringBuffer[i] = deserializer.getString();
                }
                return stringBuffer;
        }
        return null;
    }



    private Collection<?> deserializeCollection(FieldInfo fieldInfo,Deserializer deserializer) {
        int length = deserializer.getInt();
        if(length < 0) return null;
        Collection collection = null;
        try {
            collection = (Collection)fieldInfo.componentTypeConstructor.newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
        switch (fieldInfo.componentType) {
            case DataType.TYPE_BYTE:
                for(int i = 0; i < length; ++i) {
                    collection.add(deserializer.getByte());
                }
                break;
            case DataType.TYPE_BOOLEAN:
                for(int i = 0; i < length; ++i) {
                    collection.add(deserializer.getBoolean());
                }
                break;
            case DataType.TYPE_SHORT:
                for(int i = 0; i < length; ++i) {
                    collection.add(deserializer.getShort());
                }
                break;
            case DataType.TYPE_CHAR:
                for(int i = 0; i < length; ++i) {
                    collection.add(deserializer.getChar());
                }
                break;
            case DataType.TYPE_INT:
                for(int i = 0; i < length; ++i) {
                    collection.add(deserializer.getInt());
                }
                break;
            case DataType.TYPE_FLOAT:
                for(int i = 0; i < length; ++i) {
                    collection.add(deserializer.getFloat());
                }
                break;
            case DataType.TYPE_LONG:
                for(int i = 0; i < length; ++i) {
                    collection.add(deserializer.getLong());
                }
                break;
            case DataType.TYPE_DOUBLE:
                for(int i = 0; i < length; ++i) {
                    collection.add(deserializer.getDouble());
                }
                break;
            case DataType.TYPE_STRING:
                for(int i = 0; i < length; ++i) {
                    collection.add(deserializer.getString());
                }
                break;
        }
        return collection;
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
