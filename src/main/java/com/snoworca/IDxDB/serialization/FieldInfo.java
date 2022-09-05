package com.snoworca.IDxDB.serialization;

import com.snoworca.IDxDB.data.DataType;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.*;

public class FieldInfo implements Comparable {
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
    private boolean isError = false;
    private boolean isArray = false;
    private boolean isCollection = false;

    private int arraySize = 0;


    private byte type;
    private byte componentType;
    private Constructor<?> componentTypeConstructor;
    private boolean isPrimitive = true;

    private Field field;
    private String name;

    public boolean isError() {
        return isError;
    }

    protected void setError(boolean error) {
        isError = error;
    }

    public boolean isArray() {
        return isArray;
    }

    protected void setArray(boolean array) {
        isArray = array;
    }

    public boolean isCollection() {
        return isCollection;
    }

    protected void setCollection(boolean collection) {
        isCollection = collection;
    }

    public int getArraySize() {
        return arraySize;
    }

    protected void setArraySize(int arraySize) {
        this.arraySize = arraySize;
    }

    public byte getType() {
        return type;
    }

    protected void setType(byte type) {
        this.type = type;
    }

    public byte getComponentType() {
        return componentType;
    }

    protected void setComponentType(byte componentType) {
        this.componentType = componentType;
    }

    public Constructor<?> getComponentTypeConstructor() {
        return componentTypeConstructor;
    }

    protected void setComponentTypeConstructor(Constructor<?> componentTypeConstructor) {
        this.componentTypeConstructor = componentTypeConstructor;
    }

    public boolean isPrimitive() {
        return isPrimitive;
    }

    protected void setPrimitive(boolean primitive) {
        isPrimitive = primitive;
    }

    public Field getField() {
        return field;
    }

    protected void setField(Field field) {
        this.field = field;
    }

    public String getName() {
        return name;
    }

    protected void setName(String name) {
        this.name = name;
    }


    @Override
    public int compareTo(Object o) {
        FieldInfo info = (FieldInfo)o;
        return name.compareTo(info.name);
    }
}
