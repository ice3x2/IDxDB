package com.snoworca.IdxDB.dataStore;

import com.snoworca.IdxDB.CompressionType;

public class DataIOConfig {
    private int readerCapacity = 3;
    private CompressionType compressionType;
    public DataIOConfig() {

    }


    public void setCompressionType(CompressionType compressionType) {
        this.compressionType = compressionType;
    }

    public CompressionType getCompressionType() {
        return compressionType == null ? CompressionType.NONE : compressionType;
    }

    public DataIOConfig setReaderCapacity(int readerCapacity) {
        this.readerCapacity = readerCapacity;
        return this;
    }

    public int getReaderCapacity() {
        return readerCapacity;
    }
}
