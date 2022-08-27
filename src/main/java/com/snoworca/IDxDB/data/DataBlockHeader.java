package com.snoworca.IDxDB.data;

import com.snoworca.IDxDB.exception.DataBlockParseException;
import com.snoworca.IDxDB.util.NumberBufferConverter;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

public class DataBlockHeader {
    private final static AtomicLong TOP_ID = new AtomicLong(Long.MIN_VALUE);

    final static int HEADER_SIZE = 1 + 8 + 1 + 4;

    final static int HEADER_IDX_PREFIX = 0;
    final static int HEADER_IDX_ID = 1;
    final static int HEADER_IDX_TYPE = 9;
    final static int HEADER_IDX_LEN = 10;

    public final static byte PREFIX = 0x64; /** D */

    private long ID;

    private int length;

    private byte type = DataType.TYPE_NULL;

    public long getID() {
        return ID;
    }

    public static void setTopID(long ID) {
        TOP_ID.set(ID);
    }

    @Override
    public boolean equals(Object eq) {
        if(eq == this) return true;
        if(eq instanceof DataBlockHeader) {
            DataBlockHeader eqHeader = ((DataBlockHeader)eq);
            return ID == eqHeader.ID && length == eqHeader.length && type == eqHeader.type;
        }
        return false;
    }

    DataBlockHeader(byte type, int length) {
        this.ID = TOP_ID.getAndIncrement();
        this.type = type;
        this.length = length;
        if(!DataType.checkNumberTypeLength(this.type, this.length)) {
            throw new DataBlockParseException("Size of the data defined as a primitive type is different.");
        }
    }

    private DataBlockHeader() {
    }


    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public byte getType() {
        return type;
    }

    public void setType(byte type) {
        this.type = type;
    }



    void writeBuffer(byte[] buffer) {
        buffer[HEADER_IDX_PREFIX] = PREFIX;
        NumberBufferConverter.fromLong(this.ID, buffer, HEADER_IDX_ID);
        buffer[HEADER_IDX_TYPE] = this.type;
        NumberBufferConverter.fromInt(this.length, buffer, HEADER_IDX_LEN);
    }


    /*

    public static DataBlockHeader fromBuffer(byte[] headerBuffer) {

        if(headerBuffer[HEADER_IDX_PREFIX] != PREFIX) {
            throw  new DataBlockParseException("Data block header parsing error: Invalid prefix value.");
        }
        DataBlockHeader dataHeader = new DataBlockHeader();
        dataHeader.ID = NumberBufferConverter.toLong(headerBuffer, HEADER_IDX_ID);
        dataHeader.type = headerBuffer[HEADER_IDX_TYPE];
        dataHeader.length = NumberBufferConverter.toInt(headerBuffer, HEADER_IDX_LEN);
        if(!DataType.checkNumberTypeLength(dataHeader.type, dataHeader.length)) {
            throw new DataBlockParseException("Size of the data defined as a primitive type is different.");
        }

        return dataHeader;

    }*/


    public static DataBlockHeader fromByteBuffer(ByteBuffer headerBuffer) {

        if(headerBuffer.get() != PREFIX) {
            throw  new DataBlockParseException("Data block header parsing error: Invalid prefix value.");
        }
        DataBlockHeader dataHeader = new DataBlockHeader();
        dataHeader.ID =  headerBuffer.getLong();
        dataHeader.type = headerBuffer.get();
        dataHeader.length = headerBuffer.getInt();
        if(!DataType.checkNumberTypeLength(dataHeader.type, dataHeader.length)) {
            throw new DataBlockParseException("Size of the data defined as a primitive type is different.");
        }

        return dataHeader;

    }

}

