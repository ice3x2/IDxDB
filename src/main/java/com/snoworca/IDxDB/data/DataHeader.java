package com.snoworca.IDxDB.data;

import com.snoworca.IDxDB.util.NumberBufferConverter;

import java.util.concurrent.atomic.AtomicLong;

public class DataHeader {
    private final static AtomicLong TOP_ID = new AtomicLong(Long.MIN_VALUE);

    private final static int HEADER_SIZE = 1 + 8 + 1 + 4;

    public final static byte PREFIX = 0x64; /** D */

    private long ID;

    private int length;

    private byte type = DataType.TYPE_NULL;

    public long getID() {
        return ID;
    }

    public void setID(long ID) {
        this.ID = ID;
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


    public byte[] toBuffer() {

        byte[] buffer = new byte[HEADER_SIZE];
        buffer[0] = PREFIX;
        NumberBufferConverter.fromLong(this.ID, buffer, 1);
        buffer[9] = this.type;
        NumberBufferConverter.fromInt(this.length, buffer, 10);
        return buffer;
    }

    public static DataHeader fromBuffer(byte[] headerBuffer) {

        if(headerBuffer[0] != PREFIX) {
            //TODO 예외발생시켜야한다.
        }
        DataHeader dataHeader = new DataHeader();
        dataHeader.ID = NumberBufferConverter.toLong(headerBuffer, 1);
        dataHeader.type = headerBuffer[9];
        dataHeader.length = NumberBufferConverter.toInt(headerBuffer, 10);

        return dataHeader;

    }

}

