package com.snoworca.IdxDB.dataStore;



import com.snoworca.IdxDB.exception.DataBlockParseException;
import com.snoworca.IdxDB.util.NumberBufferConverter;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class DataBlock {
    private DataBlockHeader header;
    private boolean isCapacityFixed = true;
    private byte[] data;

    private long pos = -1;

    private DataBlock() {}



    public static DataBlock newNullDataBlock() {
        DataBlock dataPayload = new DataBlock();
        dataPayload.header = new DataBlockHeader(DataType.TYPE_NULL, 0, 0);
        dataPayload.data = new byte[0];
        return dataPayload;
    }



    public static DataBlock newDataBlock(Byte value) {
        DataBlock dataPayload = new DataBlock();
        dataPayload.header = new DataBlockHeader(DataType.TYPE_BYTE, 1,0);
        dataPayload.data = new byte[]{value};
        return dataPayload;
    }

    public static DataBlock newDataBlock(Character value) {
        DataBlock dataPayload = new DataBlock();
        dataPayload.header = new DataBlockHeader(DataType.TYPE_CHAR, 2,0);
        dataPayload.data = NumberBufferConverter.fromChar(value);
        return dataPayload;
    }

    public static DataBlock newDataBlock(Short value) {
        DataBlock dataPayload = new DataBlock();
        dataPayload.header = new DataBlockHeader(DataType.TYPE_SHORT, 2,0);
        dataPayload.data = NumberBufferConverter.fromShort(value);
        return dataPayload;
    }

    public static DataBlock newDataBlock(Integer value) {
        DataBlock dataPayload = new DataBlock();
        dataPayload.header = new DataBlockHeader(DataType.TYPE_INT, 4,0);
        dataPayload.data = NumberBufferConverter.fromInt(value);
        return dataPayload;
    }

    public static DataBlock newDataBlock(Float value) {
        DataBlock dataPayload = new DataBlock();
        dataPayload.header = new DataBlockHeader(DataType.TYPE_FLOAT, 4,0);
        dataPayload.data = NumberBufferConverter.fromFloat(value);
        return dataPayload;
    }

    public static DataBlock newDataBlock(Long value) {
        DataBlock dataPayload = new DataBlock();
        dataPayload.header = new DataBlockHeader(DataType.TYPE_LONG, 8,0);
        dataPayload.data = NumberBufferConverter.fromLong(value);
        return dataPayload;
    }

    public static DataBlock newDataBlock(Double value) {
        DataBlock dataPayload = new DataBlock();
        dataPayload.header = new DataBlockHeader(DataType.TYPE_DOUBLE, 8,0);
        dataPayload.data = NumberBufferConverter.fromDouble(value);
        return dataPayload;
    }

    public static DataBlock newDataBlock(Boolean value) {
        DataBlock dataPayload = new DataBlock();
        dataPayload.header = new DataBlockHeader(DataType.TYPE_BOOLEAN, 1,0);
        dataPayload.data = new byte[]{value ? (byte)1 : (byte)0};
        return dataPayload;
    }

    public static DataBlock newDataBlock(CharSequence value) {
        return newDataBlock(value, 0);
    }

    private static void setDataAndLengthExtension(DataBlock dataPayload, byte[] dataBuffer) {
        int limit = dataPayload.header.getCapacity();
        if(limit == dataBuffer.length) {
            dataPayload.data = dataBuffer;
        } else {
            dataPayload.data = new byte[limit];
            System.arraycopy(dataBuffer, 0, dataPayload.data, 0, dataBuffer.length);
            dataPayload.isCapacityFixed = false;
        }
    }

    public static DataBlock newDataBlock(CharSequence value, float capacity) {
        DataBlock dataPayload = new DataBlock();
        byte[] dataBuffer = value.toString().getBytes(StandardCharsets.UTF_8);
        dataPayload.header = new DataBlockHeader(DataType.TYPE_STRING, dataBuffer.length,capacity);
        setDataAndLengthExtension(dataPayload, dataBuffer);
        return dataPayload;
    }

    public static DataBlock newDataBlock(byte[] buffer, float capacity) {
        DataBlock dataPayload = new DataBlock();
        dataPayload.header = new DataBlockHeader(DataType.TYPE_ARRAY, buffer.length, capacity);
        setDataAndLengthExtension(dataPayload, buffer);
        return dataPayload;
    }

    public byte[] toBuffer() {
        byte[] buffer = new byte[DataBlockHeader.HEADER_SIZE + data.length];
        header.writeBuffer(buffer);
        System.arraycopy(data, 0, buffer, DataBlockHeader.HEADER_SIZE, data.length);
        return buffer;
    }



    public byte[] getPayload() {
        return data;
    }

    public byte[] getData() {
        if(isCapacityFixed) {
            return data;
        } else {
            byte[] buffer = new byte[header.getLength()];
            System.arraycopy(data, 0, buffer, 0, buffer.length);
            return buffer;
        }
    }


    public Object getValue() {
        int type = header.getType();
        switch (type) {
            case DataType.TYPE_BOOLEAN:
                return data[0] == 1;
            case DataType.TYPE_BYTE:
                return data[0];
            case DataType.TYPE_CHAR:
                return NumberBufferConverter.toChar(data);
            case DataType.TYPE_SHORT:
                return NumberBufferConverter.toShort(data);
            case DataType.TYPE_INT:
                return NumberBufferConverter.toInt(data);
            case DataType.TYPE_FLOAT:
                return NumberBufferConverter.toFloat(data);
            case DataType.TYPE_LONG:
                return NumberBufferConverter.toLong(data);
            case DataType.TYPE_DOUBLE:
                return NumberBufferConverter.toDouble(data);
            case DataType.TYPE_STRING:
                return new String(data, StandardCharsets.UTF_8);
            case DataType.TYPE_ARRAY:
                return data;
        }

        return null;
    }

    void setPos(long pos) {
        this.pos = pos;
    }

    public long getPos() {
        return this.pos;
    }


    public void changeData(byte[] buffer) {
        System.arraycopy(buffer, 0, data, 0, buffer.length);
        this.header.setLength(buffer.length);

    }


    public static DataBlock parseData(DataBlockHeader header, ByteBuffer... byteBuffer) {
        DataBlock dataBlock = new DataBlock();
        dataBlock.header = header;
        int limit = header.getCapacity();
        int len = header.getLength();
        int remaining = byteBuffer[0].remaining();
        for(int i = 1; i < byteBuffer.length; ++i) {
            remaining += byteBuffer[i].remaining();
        }
        if(len > remaining) {
            throw new DataBlockParseException("Data size defined in the header and the actual data size do not match.(" + limit + " > " + (remaining) + ")");
        }
        dataBlock.data = new byte[limit];
        int offset = 0;
        for(int i = 0; i < byteBuffer.length; ++i) {
            int readLen = Math.min(byteBuffer[i].remaining(), limit - offset);
            byteBuffer[i].get(dataBlock.data,offset, readLen);
            offset += readLen;
        }
        dataBlock.isCapacityFixed = limit == len;
        return dataBlock;
    }

    /*public static DataBlock parseData(DataBlockHeader header, byte[] buffer) {
        return parseData(header, buffer, 0);
    }*/


    public DataBlockHeader getHeader() {
        return this.header;
    }



}
