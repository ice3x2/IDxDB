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



    public void setOption(String key, Object value) {
        this.option.put(key, value);
    }



    public void setCapacityRatio(float ratio) {
        if(ratio < 0) ratio = 0.0f;
        toCsonObject().put("capacityRatio", ratio);
    }

    public float getCapacityRatio() {
        return toCsonObject().optFloat("capacityRatio", 0.3f);
    }



    public void setMemCacheIndex(boolean enable) {
        toCsonObject().put("memCacheIndex", enable);
    }

    public boolean isMemCacheIndex() {
        return toCsonObject().optBoolean("memCacheIndex", true);
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
