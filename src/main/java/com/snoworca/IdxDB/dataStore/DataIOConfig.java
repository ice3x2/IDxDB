package com.snoworca.IdxDB.dataStore;

public class DataIOConfig {
    private int readerCapacity = 3;
    public DataIOConfig() {

    }

    public DataIOConfig setReaderCapacity(int readerCapacity) {
        this.readerCapacity = readerCapacity;
        return this;
    }

    public int getReaderCapacity() {
        return readerCapacity;
    }
}
