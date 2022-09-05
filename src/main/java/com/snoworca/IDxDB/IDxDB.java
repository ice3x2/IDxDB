package com.snoworca.IDxDB;

import java.lang.reflect.ParameterizedType;

public class IDxDB {

    public <E extends Object> XList<E> getOrCreateList(String name) {
        XList<E> list = new XList<>();


        return list;
    }


}
