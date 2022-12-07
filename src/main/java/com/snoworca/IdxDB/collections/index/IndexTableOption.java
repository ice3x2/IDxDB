package com.snoworca.IdxDB.collections.index;

import java.util.ArrayList;

public class IndexTableOption {
    private String name;
    private ArrayList<IndexInfo> indexInfos = new ArrayList<>();
    private boolean isUnique = false;
    private boolean isHashMap = false;
    private int memCacheSize = 0;
    private boolean isMemCacheIndex = true;

    private OnReadData onReadData;




    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public IndexInfo[] getIndexItems() {
        return indexInfos.toArray(new IndexInfo[indexInfos.size()]);
    }

    public void addIndexItem(IndexInfo indexInfo) {
        indexInfos.add(indexInfo);
    }

    public boolean isUnique() {
        return isUnique;
    }

    public void setUnique(boolean unique) {
        isUnique = unique;
    }

    public boolean isHashMap() {
        return isHashMap;
    }

    public void setHashMap(boolean hashMap) {
        isHashMap = hashMap;
    }

    public int getMemCacheSize() {
        return memCacheSize;
    }

    public void setMemCacheSize(int memCacheSize) {
        this.memCacheSize = memCacheSize;
    }

    public OnReadData getOnReadData() {
        return onReadData;
    }

    public void setOnReadData(OnReadData onReadData) {
        this.onReadData = onReadData;
    }

    public boolean isMemCacheIndex() {
        return isMemCacheIndex;
    }

    public void setMemCacheIndex(boolean memCacheIndex) {
        isMemCacheIndex = memCacheIndex;
    }





}
