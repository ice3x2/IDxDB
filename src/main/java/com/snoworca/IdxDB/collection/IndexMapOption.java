package com.snoworca.IdxDB.collection;

import com.snoworca.cson.CSONObject;

public class IndexMapOption extends CollectionOption {

    public IndexMapOption(String collectionName) {
        super(IndexLinkedMap.class.getName(), collectionName);
        toCsonObject().put("accessOrder", false);
    }

    public static IndexMapOption fromCSONObject(CSONObject csonObject) {
        IndexMapOption option = new IndexMapOption(csonObject.getString("name"));
        for(String key : csonObject.keySet()) {
            option.setOption(key, csonObject.get(key));
        }
        return option;
    }


    public void setAccessOrder(boolean enable) {
            toCsonObject().put("accessOrder", enable);
    }

    public boolean isAccessOrder() {
        return toCsonObject().optBoolean("accessOrder", false);
    }




}
