package com.snoworca.StreamDB.serialization;

import com.snoworca.IDxDB.util.ByteBufferOutputStream;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class Serializer {

    private ByteBufferOutputStream byteBufferOutputStream;


    Serializer(int capacity) {
        byteBufferOutputStream = new ByteBufferOutputStream(capacity);
    }


    Serializer() {
        byteBufferOutputStream = new ByteBufferOutputStream(4096);
    }

    public Serializer putByte(Byte value) {
        byteBufferOutputStream.write(value);
        return this;
    }

    public Serializer putBoolean(Boolean value) {
        byteBufferOutputStream.write(value ? 1 : 0);
        return this;
    }
    public Serializer putShort(Short value) {
        byteBufferOutputStream.writeShort(value);
        return this;
    }
    public Serializer putCharacter(Character value) {
        byteBufferOutputStream.writeChar(value);
        return this;
    }
    public Serializer putInteger(Integer value) {
        byteBufferOutputStream.writeInt(value);
        return this;
    }
    public Serializer putFloat(Float value) {
        byteBufferOutputStream.writeFloat(value);
        return this;
    }
    public Serializer putLong(Long value) {
        byteBufferOutputStream.writeLong(value);
        return this;
    }

    public Serializer putDouble(Double value) {
        byteBufferOutputStream.writeDouble(value);
        return this;
    }

    public Serializer putString(String value, int maxLength) {
        if(maxLength <= 0) return this;
        maxLength = maxLength * 2;
        byte[] buffer = value.getBytes(StandardCharsets.UTF_16);
        int writeLength = Math.min(buffer.length, maxLength);
        putInteger(writeLength);
        byteBufferOutputStream.write(buffer, 0, writeLength);
        if(writeLength == maxLength) return this;
        byte[] leftBuffer = new byte[maxLength - writeLength];
        byteBufferOutputStream.write(leftBuffer, 0, leftBuffer.length);
        return this;
    }

    public Serializer putByteArray(byte[] value) {
        if(value == null) {
            byteBufferOutputStream.writeInt(-1);
            return this;
        } else if(value.length == 0) {
            byteBufferOutputStream.writeInt(0);
            return this;
        }
        byteBufferOutputStream.writeInt(value.length);
        byteBufferOutputStream.write(value, 0, value.length);
        return this;
    }

    public ByteBuffer getByteBuffer() {
        return byteBufferOutputStream.toByteBuffer();
    }



}