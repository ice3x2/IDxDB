package com.snoworca.IdxDB.store;

import com.snoworca.IdxDB.CompressionType;

public class DataStoreOptions {
    private int readerSize = 3;
    private CompressionType compressionType;
    private float capacityRatio = 0.3f;

    private int iterableBufferSize = 1024 * 1024 * 32;


    public DataStoreOptions() {

    }

    public DataStoreOptions setIterableBufferSize(int iterableBufferSize) {
        this.iterableBufferSize = iterableBufferSize;
        return this;
    }

    public DataStoreOptions setCapacityRatio(float capacityRatio) {
        this.capacityRatio = capacityRatio;
        return this;
    }

    public float getCapacityRatio() {
        return capacityRatio;
    }


    public DataStoreOptions setCompressionType(CompressionType compressionType) {
        this.compressionType = compressionType;
        return this;
    }

    public int getIterableBufferSize() {
        return iterableBufferSize;
    }

    public CompressionType getCompressionType() {
        return compressionType == null ? CompressionType.NONE : compressionType;
    }

    public DataStoreOptions setReaderSize(int readerSize) {
        this.readerSize = readerSize;
        return this;
    }

    public int getReaderSize() {
        return readerSize;
    }
}
