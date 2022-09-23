package com.snoworca.IdxDB.collection;

import com.snoworca.cson.CSONObject;

public class IndexSetOption extends CollectionOption {

    public IndexSetOption(String collectionName) {
        super(IndexSet.class.getName(), collectionName);
    }

    public static IndexSetOption fromCSONObject(CSONObject csonObject) {
        IndexSetOption option = new IndexSetOption(csonObject.getString("name"));
        for(String key : csonObject.keySet()) {
            option.setOption(key, csonObject.get(key));
        }
        return option;
    }
    public void setMemCacheSize(int size) {
        toCsonObject().put("memCacheSize", size);
    }

    public int getMemCacheSize() {
        return toCsonObject().optInteger("memCacheSize", 100);
    }

    public void setIndex(String key, int sort) {
        CSONObject csonObject = toCsonObject();
        csonObject.put("indexKey" , key);
        csonObject.put("sort" , sort);
    }

    public String getIndexKey() {
        return toCsonObject().optString("indexKey");
    }

    public int getIndexSort() {
        return toCsonObject().optInteger("sort");
    }


}
