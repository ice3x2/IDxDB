package com.snoworca.StreamDB.serialization;

import com.snoworca.IDxDB.util.ByteBufferOutputStream;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class Serializer {

    private ByteBufferOutputStream byteBufferOutputStream;


    Serializer(int capacity) {
        byteBufferOutputStream = new ByteBufferOutputStream(capacity);
    }


    Serializer() {
        byteBufferOutputStream = new ByteBufferOutputStream(4096);
    }

    public Serializer put(Byte value) {
        byteBufferOutputStream.write(value);
        return this;
    }

    public Serializer put(Boolean value) {
        byteBufferOutputStream.write(value ? 1 : 0);
        return this;
    }
    public Serializer put(Short value) {
        byteBufferOutputStream.writeShort(value);
        return this;
    }
    public Serializer put(Character value) {
        byteBufferOutputStream.writeChar(value);
        return this;
    }
    public Serializer put(Integer value) {
        byteBufferOutputStream.writeInt(value);
        return this;
    }
    public Serializer put(Float value) {
        byteBufferOutputStream.writeFloat(value);
        return this;
    }
    public Serializer put(Long value) {
        byteBufferOutputStream.writeLong(value);
        return this;
    }

    public Serializer put(Double value) {
        byteBufferOutputStream.writeDouble(value);
        return this;
    }

    public Serializer put(String value) {
        if(value == null) {
            byteBufferOutputStream.writeInt(-1);
        }  else if(value.isEmpty()) {
            byteBufferOutputStream.writeInt(0);
            return this;
        }
        byte[] buffer = value.getBytes(StandardCharsets.UTF_8);
        byteBufferOutputStream.writeInt(buffer.length);
        byteBufferOutputStream.write(buffer, 0, buffer.length);
        return this;
    }

    public Serializer put(byte[] value) {
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
        return byteBufferOutputStream.getByteBuffer();
    }



}
