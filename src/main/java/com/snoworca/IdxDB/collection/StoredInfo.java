package com.snoworca.IdxDB.collection;

import com.snoworca.cson.CSONObject;

class StoredInfo  {
    private long position;
    private int capacity;
    private CSONObject csonObject;


    public StoredInfo(long position, int capacity) {
        this.position = position;
        this.capacity = capacity;
    }

    public StoredInfo(long position, int capacity, CSONObject csonObject) {
        this.position = position;
        this.capacity = capacity;
        this.csonObject = csonObject;
    }

    public long getPosition() {
        return position;
    }

    public int getCapacity() {
        return capacity;
    }

    public CSONObject getCsonObject() {
        return csonObject;
    }
}
