package com.snoworca.IdxDB.collection;

public class IndexItem {

    IndexItem(StoredInfo positionInfo,int sort,String indexKey) {
        this.positionInfo = positionInfo;
        this.indexKey = indexKey;
        this.sort = sort;
    }

    private StoredInfo positionInfo;
    private Object index;
    private String indexKey;
    private int sort;

}
