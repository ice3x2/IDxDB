package com.snoworca.IdxDB.collection;

import com.snoworca.cson.CSONObject;

public class IndexTreeOption extends CollectionOption {

    public IndexTreeOption(String collectionName) {
        super(IndexTree.class.getName(), collectionName);
    }

    public static IndexTreeOption fromCSONObject(CSONObject csonObject) {
        IndexTreeOption option = new IndexTreeOption(csonObject.getString("name"));
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
