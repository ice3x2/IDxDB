package com.snoworca.IdxDB.collection;

import com.snoworca.cson.CSONObject;

public class CollectionOption {


    private CSONObject option = new CSONObject();


    public CollectionOption(String className, String collectionName) {
        option.put("className", className);
        option.put("name", collectionName);
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
        return toCsonObject().optInteger("sort", 1);
    }

    public String getName() {
        return option.getString("name");
    }

    public String getClassName() {
        return option.getString("className");
    }

    public void setFileStore(boolean fileStoreEnable) {
        option.put("fileStore", fileStoreEnable);
    }

    public void setOption(String key, Object value) {
        this.option.put(key, value);
    }

    public Object getOption(String key) {
        return this.option.get(key);
    }

    public boolean isFileStore() {
        return option.optBoolean("fileStore", true);
    }

    public CSONObject toCsonObject() {
        return this.option;
    }

    public long getHeadPos() {
        return this.option.optLong("headPos", -1);
    }

    public void fromCsonObject(CSONObject csonObject) {
        this.option = csonObject;
    }

    public void setMemCacheSize(int size) {
        toCsonObject().put("memCacheSize", size);
    }

    public int getMemCacheSize() {
        return toCsonObject().optInteger("memCacheSize", 100);
    }



}
