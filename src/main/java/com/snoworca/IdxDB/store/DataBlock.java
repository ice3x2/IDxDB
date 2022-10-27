package com.snoworca.IdxDB.store;

import com.snoworca.IdxDB.CompressionType;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class DataBlock {

    private DataBlockHeader header;
    private byte[] data;
    private long position  = -1;

    private DataBlock() {

    }

    public static DataBlock createWriteDataBlock(int collectionID,byte[] data) {
        DataBlock dataBlock = new DataBlock();
        dataBlock.header = new DataBlockHeader(collectionID,data.length, (byte) 0);
        dataBlock.data = data;
        dataBlock.position = -1;
        return dataBlock;
    }



    protected DataBlock(DataBlockHeader header) {
        this.header = header;

    }



    public DataBlockHeader getHeader() {
        return header;
    }

    public byte[] getData() {
        return data;
    }



    public void setData(byte[] data) {
        this.data = data;

    }

    public int getCollectionId() {
        return header.getCollectionId();
    }

    public int getCapacity() {
        return header.getCapacity();
    }

    public byte getCompressionType() {
        return header.getCompressionType();
    }

    public void setCollectionId(int collectionId) {
        header.setCollectionId(collectionId);
    }

    public void setCapacity(int capacity) {
        header.setCapacity(capacity);
    }

    public void setCompressionType(byte compressionType) {
        header.setCompressionType(compressionType);
    }

    public void setPosition(long position) {
        this.position = position;
    }

    public long getPosition() {
        return position;
    }

    public static DataBlock fromByteBuffer(DataBlockHeader header, byte[] buffer) {
        DataBlock dataBlock = new DataBlock(header);
        dataBlock.setData(buffer);
        return dataBlock;
    }

    public static DataBlock fromByteBuffer(DataBlockHeader header, ByteBuffer byteBuffer) {
        DataBlock dataBlock = new DataBlock();
        dataBlock.header = header;
        dataBlock.data = new byte[header.getCapacity()];
        byteBuffer.get(dataBlock.data);
        return dataBlock;
    }

    public static DataBlock fromByteBuffers(DataBlockHeader header, ByteBuffer... byteBuffer) {
        DataBlock dataBlock = new DataBlock();
        dataBlock.header = header;
        int capacity =  header.getCapacity();
        dataBlock.data = new byte[capacity];
        int offset = 0;
        for (int i = 0, n = byteBuffer.length; i < n; i++) {
            ByteBuffer buffer = byteBuffer[i];
            int length = buffer.remaining();
            length = Math.min(length, capacity - offset);
            if(length <= 0) break;
            buffer.get(dataBlock.data, offset, length);
            offset += length;
        }
        return dataBlock;
    }

    public byte[] toBytes() {
        byte[] buffer = new byte[DataBlockHeader.HEADER_SIZE + data.length];
        System.arraycopy(header.toBytes(), 0, buffer, 0, DataBlockHeader.HEADER_SIZE);
        System.arraycopy(data, 0, buffer, DataBlockHeader.HEADER_SIZE, data.length);
        return buffer;
    }







}
