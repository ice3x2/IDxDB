package com.snoworca.IdxDB.store;

import java.nio.ByteBuffer;

/**
 * copilot 인공지능님이 만들어주심.
 */
public class DataBlockHeader {
    // prefix(1), compressionType(1), collection ID(4), capacity(4)
    final static int HEADER_SIZE = 1 + 1 + 4 + 4;

    final static int HEADER_IDX_PREFIX = 0;

    final static int HEADER_IDX_COMPRESSION_TYPE = 1;

    final static int HEADER_COLLECTION_ID = 2;

    final static int HEADER_CAPACITY = 6;

    public final static byte PREFIX = 'X';

    public final static int DELETED_ID = -1;

    private int collectionId = 0;
    private int capacity = 0;
    private byte compressionType = 0;

    public boolean isDeleted() {
        return collectionId == DELETED_ID;
    }

    public DataBlockHeader(int collectionId, int capacity, byte compressionType) {
        this.collectionId = collectionId;
        this.capacity = capacity;
        this.compressionType = compressionType;
    }

    public int getCollectionId() {
        return collectionId;
    }

    public int getCapacity() {
        return capacity;
    }

    public byte getCompressionType() {
        return compressionType;
    }

    public void setCollectionId(int collectionId) {
        this.collectionId = collectionId;
    }

    public void setCapacity(int capacity) {
        this.capacity = capacity;
    }

    public void setCompressionType(byte compressionType) {
        this.compressionType = compressionType;
    }

    public byte[] toBytes() {
        byte[] buffer = new byte[HEADER_SIZE];
        buffer[HEADER_IDX_PREFIX] = PREFIX;
        buffer[HEADER_IDX_COMPRESSION_TYPE] = this.compressionType;
        buffer[HEADER_COLLECTION_ID] = (byte) (this.collectionId >> 24);
        buffer[HEADER_COLLECTION_ID + 1] = (byte) (this.collectionId >> 16);
        buffer[HEADER_COLLECTION_ID + 2] = (byte) (this.collectionId >> 8);
        buffer[HEADER_COLLECTION_ID + 3] = (byte) (this.collectionId);
        buffer[HEADER_CAPACITY] = (byte) (this.capacity >> 24);
        buffer[HEADER_CAPACITY + 1] = (byte) (this.capacity >> 16);
        buffer[HEADER_CAPACITY + 2] = (byte) (this.capacity >> 8);
        buffer[HEADER_CAPACITY + 3] = (byte) (this.capacity);
        return buffer;
    }

    public void write(ByteBuffer byteBuffer) {
        byteBuffer.clear();
        byteBuffer.put(PREFIX);
        byteBuffer.put(this.compressionType);
        byteBuffer.putInt(this.collectionId);
        byteBuffer.putInt(this.capacity);
        byteBuffer.flip();
    }

    // java.nio.ByteBuffer객체를 입력 받아서 DataBlockHeader 를 생성한다.
    public static DataBlockHeader fromByteBuffer(ByteBuffer byteBuffer) {
        byte prefix = byteBuffer.get();
        if (prefix != PREFIX) {
            throw new RuntimeException("Invalid data block header");
        }
        byte compressionType = byteBuffer.get();
        int collectionId = byteBuffer.getInt();
        int capacity = byteBuffer.getInt();
        return new DataBlockHeader(collectionId, capacity, compressionType);
    }





    public static DataBlockHeader fromByteBuffer(byte[] buffer) {
        if(buffer[HEADER_IDX_PREFIX] != PREFIX) {
            throw new IllegalArgumentException("Invalid data block header.");
        }
        byte compressionType = buffer[HEADER_IDX_COMPRESSION_TYPE];
        int collectionId = (buffer[HEADER_COLLECTION_ID] << 24) | (buffer[HEADER_COLLECTION_ID + 1] << 16) | (buffer[HEADER_COLLECTION_ID + 2] << 8) | (buffer[HEADER_COLLECTION_ID + 3]);
        int capacity = (buffer[HEADER_CAPACITY] << 24) | (buffer[HEADER_CAPACITY + 1] << 16) | (buffer[HEADER_CAPACITY + 2] << 8) | (buffer[HEADER_CAPACITY + 3]);
        return new DataBlockHeader(collectionId, capacity, compressionType);
    }

}
