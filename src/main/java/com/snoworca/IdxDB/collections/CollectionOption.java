package com.snoworca.IdxDB.collections;

public class CollectionOption {
    private String idKey;
    private boolean idAscending = true;
    private boolean idHashMap = true;


    public CollectionOption setIdKey(String idKey) {
        this.idKey = idKey;
        return this;
    }

    public CollectionOption setIdAscendingSort(boolean ascending) {
        this.idAscending = ascending;
        return this;
    }

    public CollectionOption enableIdHashMap(boolean enable) {
        this.idHashMap = enable;
        return this;
    }

    public boolean isIdAscending() {
        return idAscending;
    }

    public String getIdKey() {
        return idKey;
    }

    public boolean isIdHashMap() {
        return idHashMap;
    }
}
