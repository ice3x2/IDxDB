package com.snoworca.IdxDB.collection;

public class IndexTable {
    private String name;
    private boolean isUnique = false;
    private IndexItem[] indexItems;

    private int hash = 0;

    public String getName() {
        return name;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }


}
