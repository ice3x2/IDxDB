package com.snoworca.IdxDB.collection;

import com.snoworca.cson.CSONObject;

public class IndexSetOption extends CollectionOption {

    public IndexSetOption(String collectionName) {
        super(IndexSet.class.getName(), collectionName);
    }
    public void setFileStore(boolean fileStoreEnable) {
        toCsonObject().put("fileStore", fileStoreEnable);
    }
    public boolean isFileStore() {
        return toCsonObject().optBoolean("fileStore", true);
    }
    public static IndexSetOption fromCSONObject(CSONObject csonObject) {
        IndexSetOption option = new IndexSetOption(csonObject.getString("name"));
        for(String key : csonObject.keySet()) {
            option.setOption(key, csonObject.get(key));
        }
        return option;
    }


}
