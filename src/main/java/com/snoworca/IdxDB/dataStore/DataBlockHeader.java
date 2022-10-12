package com.snoworca.IdxDB.dataStore;

import com.snoworca.IdxDB.exception.DataBlockParseException;
import com.snoworca.IdxDB.util.NumberBufferConverter;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

public class DataBlockHeader {

    // prefix(1) type(1), len(4), prev(8), next(8)
    final static int HEADER_SIZE = 1 + 1 +4 + 8 + 8;

    final static int HEADER_IDX_PREFIX = 0;
    final static int HEADER_IDX_TYPE = 1;
    final static int HEADER_IDX_LEN = 2;

    final static int HEADER_IDX_PREV = 6;

    final static int HEADER_IDX_NEXT = 14;

    public final static byte PREFIX = 0x64; /** D */

    private long prev = -1;
    private long next = -1;

    private int length = 0;



    private byte type = DataType.TYPE_NULL;

    @Override
    public boolean equals(Object eq) {
        if(eq == this) return true;
        if(eq instanceof DataBlockHeader) {
            DataBlockHeader eqHeader = ((DataBlockHeader)eq);
            return length == eqHeader.length && type == eqHeader.type;
        }
        return false;
    }

    DataBlockHeader(byte type, int length) {
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

    public long getPrev() {
        return prev;
    }

    public long getNext() {
        return next;
    }

    public void setPrev(long prev) {
        this.prev = prev;
    }

    public void setNext(long next) {
        this.next = next;
    }

    void writeBuffer(byte[] buffer) {
        buffer[HEADER_IDX_PREFIX] = PREFIX;
        buffer[HEADER_IDX_TYPE] = this.type;
        NumberBufferConverter.fromInt(this.length, buffer, HEADER_IDX_LEN);
        NumberBufferConverter.fromLong(this.prev, buffer, HEADER_IDX_PREV);
        NumberBufferConverter.fromLong(this.next, buffer, HEADER_IDX_NEXT);
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

        int prefix = headerBuffer.get();
        if(prefix != PREFIX) {
            throw  new DataBlockParseException("Data block header parsing error: Invalid prefix value. (" + DataBlockHeader.PREFIX + " != " + prefix + ")");
        }
        DataBlockHeader dataHeader = new DataBlockHeader();

        dataHeader.type = headerBuffer.get();
        dataHeader.length = headerBuffer.getInt();
        if(!DataType.checkNumberTypeLength(dataHeader.type, dataHeader.length)) {
            throw new DataBlockParseException("Size of the data defined as a primitive type is different.");
        }
        dataHeader.prev = headerBuffer.getLong();
        dataHeader.next = headerBuffer.getLong();


        return dataHeader;

    }

}

