package com.snoworca.IDxDB.data;

import com.snoworca.IDxDB.exception.DataBlockParseException;
import com.snoworca.IDxDB.util.NumberBufferConverter;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class DataBlock {
    private DataBlockHeader header;
    private byte[] data;

    private DataBlock() {}

    public static DataBlock newNullDataBlock() {
        DataBlock dataPayload = new DataBlock();
        dataPayload.header = new DataBlockHeader(DataType.TYPE_NULL, 0);
        dataPayload.data = new byte[0];
        return dataPayload;
    }

    public static DataBlock newDataBlock(Byte value) {
        DataBlock dataPayload = new DataBlock();
        dataPayload.header = new DataBlockHeader(DataType.TYPE_BYTE, 1);
        dataPayload.data = new byte[]{value};
        return dataPayload;
    }

    public static DataBlock newDataBlock(Character value) {
        DataBlock dataPayload = new DataBlock();
        dataPayload.header = new DataBlockHeader(DataType.TYPE_CHAR, 2);
        dataPayload.data = NumberBufferConverter.fromChar(value);
        return dataPayload;
    }

    public static DataBlock newDataBlock(Short value) {
        DataBlock dataPayload = new DataBlock();
        dataPayload.header = new DataBlockHeader(DataType.TYPE_SHORT, 2);
        dataPayload.data = NumberBufferConverter.fromShort(value);
        return dataPayload;
    }

    public static DataBlock newDataBlock(Integer value) {
        DataBlock dataPayload = new DataBlock();
        dataPayload.header = new DataBlockHeader(DataType.TYPE_INT, 4);
        dataPayload.data = NumberBufferConverter.fromInt(value);
        return dataPayload;
    }

    public static DataBlock newDataBlock(Float value) {
        DataBlock dataPayload = new DataBlock();
        dataPayload.header = new DataBlockHeader(DataType.TYPE_FLOAT, 4);
        dataPayload.data = NumberBufferConverter.fromFloat(value);
        return dataPayload;
    }

    public static DataBlock newDataBlock(Long value) {
        DataBlock dataPayload = new DataBlock();
        dataPayload.header = new DataBlockHeader(DataType.TYPE_LONG, 8);
        dataPayload.data = NumberBufferConverter.fromLong(value);
        return dataPayload;
    }

    public static DataBlock newDataBlock(Double value) {
        DataBlock dataPayload = new DataBlock();
        dataPayload.header = new DataBlockHeader(DataType.TYPE_DOUBLE, 8);
        dataPayload.data = NumberBufferConverter.fromDouble(value);
        return dataPayload;
    }

    public static DataBlock newDataBlock(Boolean value) {
        DataBlock dataPayload = new DataBlock();
        dataPayload.header = new DataBlockHeader(DataType.TYPE_BOOLEAN, 1);
        dataPayload.data = new byte[]{value ? (byte)1 : (byte)0};
        return dataPayload;
    }

    public static DataBlock newDataBlock(CharSequence value) {
        DataBlock dataPayload = new DataBlock();
        byte[] dataBuffer = value.toString().getBytes(StandardCharsets.UTF_8);
        dataPayload.header = new DataBlockHeader(DataType.TYPE_STRING, dataBuffer.length);
        dataPayload.data = dataBuffer;
        return dataPayload;
    }

    public static DataBlock newDataBlock(byte[] buffer) {
        DataBlock dataPayload = new DataBlock();
        dataPayload.header = new DataBlockHeader(DataType.TYPE_BYTE_ARRAY, buffer.length);
        dataPayload.data = buffer;
        return dataPayload;
    }

    public byte[] toBuffer() {
        byte[] buffer = new byte[DataBlockHeader.HEADER_SIZE +  header.getLength()];
        header.writeBuffer(buffer);
        System.arraycopy(data, 0, buffer, DataBlockHeader.HEADER_SIZE, data.length);
        return buffer;
    }


    public static DataBlock parseData(DataBlockHeader header, byte[] buffer, int offset) {
        DataBlock dataBlock = new DataBlock();
        dataBlock.header = header;
        int len = header.getLength();
        if(len != buffer.length - offset) {
            throw new DataBlockParseException("Data size defined in the header and the actual data size do not match.(" + len + " != " + (buffer.length - offset) + ")");
        }

        if(offset != 0) {
            dataBlock.data = buffer;
        }
        else {
            dataBlock.data = Arrays.copyOfRange(buffer, offset, len);
        }
        return dataBlock;
    }

    public static DataBlock parseData(DataBlockHeader header, byte[] buffer) {
        return parseData(header, buffer, 0);
    }



}
