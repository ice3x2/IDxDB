package com.snoworca.StreamDB.serialization;

import com.snoworca.IDxDB.util.NumberBufferConverter;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class Deserializer {

    private ArrayList<ByteBuffer> byteBufferList = new ArrayList<>();
    private ByteBuffer currentBuffer = null;
    private int currentIndex = 0;

    private int remaining = 0;

    Deserializer(List<ByteBuffer> list) {
        if(list instanceof  ArrayList) {
            byteBufferList = (ArrayList<ByteBuffer>) list;
        } else {
            byteBufferList = new ArrayList<>(list);
        }
        currentBuffer = byteBufferList.get(currentIndex);
        remaining = currentBuffer.remaining();
    }

    public byte getByte() {
        if(remaining <= 0) {
            currentBuffer = byteBufferList.get(++currentIndex);
            remaining = currentBuffer.remaining();
        }
        byte value = currentBuffer.get();
        --remaining;
        return value;
    }

    public boolean getBoolean() {
        if(remaining <= 0) {
            currentBuffer = byteBufferList.get(++currentIndex);
            remaining = currentBuffer.remaining();
        }
        byte value = currentBuffer.get();
        --remaining;
        return value == 1;
    }

    private byte[] getBridgeBuffer(int size) {
        byte[] buffer = new byte[size];
        currentBuffer.get(buffer, 0, remaining);
        currentBuffer = byteBufferList.get(++currentIndex);
        currentBuffer.get(buffer, remaining, size - remaining);
        remaining = currentBuffer.remaining();
        return buffer;
    }

    public short getShort() {
        if(remaining < 2) {
            return NumberBufferConverter.toShort(getBridgeBuffer(2));
        }
        short value = currentBuffer.getShort();
        remaining -= 2;
        return value;
    }

    public char getChar() {
        if(remaining < 2) {
            return (char)NumberBufferConverter.toShort(getBridgeBuffer(2));
        }
        char value = (char) currentBuffer.getShort();
        remaining -= 2;
        return value;

    }

    public int getInt() {
        if(remaining < 4) {
            return NumberBufferConverter.toInt(getBridgeBuffer(4));
        }
        int value = currentBuffer.getInt();
        remaining -= 4;
        return value;

    }

    public float getFloat() {
        if(remaining < 4) {
            return NumberBufferConverter.toFloat(getBridgeBuffer(4));
        }
        float value = currentBuffer.getFloat();
        remaining -= 4;
        return value;
    }

    public long getLong() {
        if(remaining < 8) {
            return NumberBufferConverter.toLong(getBridgeBuffer(8));
        }
        long value = currentBuffer.getLong();
        remaining -= 8;
        return value;
    }

    public double getDouble() {
        if(remaining < 8) {
            return NumberBufferConverter.toDouble(getBridgeBuffer(8));
        }
        double value = currentBuffer.getDouble();
        remaining -= 8;
        return value;
    }

    public String getString() {
        int bufferLen = getInt();
        if(bufferLen == -1) {
            return null;
        }
        byte[] buffer = new byte[bufferLen];
        currentBuffer.get(buffer,0,bufferLen);
        remaining -= bufferLen;
        return new String(buffer, StandardCharsets.UTF_8);
    }


    public byte[] getBuffer(int len) {
        if(remaining < len) {
            byte[] buffer = getBridgeBuffer(len);
            return buffer;
        }
        byte[] buffer = new byte[len];
        currentBuffer.get(buffer, 0, len);
        remaining -= len;
        return buffer;
    }

    public byte[] getBuffer() {
        int len = getInt();
        if(len < 0) return null;
        if(len == 0) {
            return new byte[0];
        }
        if(remaining < len) {
            byte[] buffer = getBridgeBuffer(len);
            return buffer;
        }
        byte[] buffer = new byte[len];
        currentBuffer.get(buffer, 0, len);
        remaining -= len;
        return buffer;
    }







}
