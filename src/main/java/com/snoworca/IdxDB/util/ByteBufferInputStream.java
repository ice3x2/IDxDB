package com.snoworca.IdxDB.util;

import java.io.InputStream;
import java.nio.ByteBuffer;

public class ByteBufferInputStream extends InputStream  {
    private ByteBuffer[] byteBuffers;
    private int currentBufferIndex = 0;

    public ByteBufferInputStream(ByteBuffer... byteBuffers) {
        this.byteBuffers = byteBuffers;
    }

    @Override
    public int read() {
        if(currentBufferIndex >= byteBuffers.length) {
            return -1;
        }
        ByteBuffer currentBuffer = byteBuffers[currentBufferIndex];
        if(currentBuffer.hasRemaining()) {
            return currentBuffer.get() & 0xFF;
        } else {
            currentBufferIndex++;
            return read();
        }
    }

    @Override
    public int read(byte[] b) {
        return read(b, 0, b.length);
    }

    public int getInteger() {
        byte[] b = new byte[4];
        read(b);
        return ByteBuffer.wrap(b).getInt();
    }

    public long getLong() {
        byte[] b = new byte[8];
        read(b);
        return ByteBuffer.wrap(b).getLong();
    }

    public byte getByte() {
        byte[] b = new byte[1];
        read(b);
        return b[0];
    }



    @Override
    public int read(byte[] b, int off, int len) {
        if(currentBufferIndex >= byteBuffers.length) {
            return -1;
        }
        ByteBuffer currentBuffer = byteBuffers[currentBufferIndex];
        if(currentBuffer.hasRemaining()) {
            int readSize = Math.min(len, currentBuffer.remaining());
            currentBuffer.get(b, off, readSize);
            return readSize;
        } else {
            currentBufferIndex++;
            return read(b, off, len);
        }
    }

    @Override
    public int available() {
        int available = 0;
        for(int i = currentBufferIndex; i < byteBuffers.length; i++) {
            available += byteBuffers[i].remaining();
        }
        return available;
    }

    @Override
    public void close() {
        byteBuffers = null;
    }

    @Override
    public boolean markSupported() {
        return false;
    }



    @Override
    public synchronized void reset() {
        throw new UnsupportedOperationException();
    }




}
