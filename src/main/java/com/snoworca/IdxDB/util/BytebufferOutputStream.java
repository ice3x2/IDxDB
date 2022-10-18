package com.snoworca.IdxDB.util;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;

public class BytebufferOutputStream extends OutputStream  {

    private ArrayList<ByteBuffer> byteBuffers = new ArrayList<>();
    private int cacheSize = 512;

    public BytebufferOutputStream(int cacheSize) {
        this.cacheSize = cacheSize;
    }


    private ByteBuffer getCurrentBuffer() {
        if(byteBuffers.isEmpty() || !byteBuffers.get(byteBuffers.size() - 1).hasRemaining()) {
            byteBuffers.add(ByteBuffer.allocateDirect(cacheSize));
        }
        return byteBuffers.get(byteBuffers.size() - 1);
    }

    @Override
    public void write(byte b[], int off, int len) throws IOException {
        int writeSize = len;
        int writeOffset = off;
        while(writeSize > 0) {
            ByteBuffer currentBuffer = getCurrentBuffer();
            int writeSizeInBuffer = Math.min(writeSize, currentBuffer.remaining());
            currentBuffer.put(b, writeOffset, writeSizeInBuffer);
            writeSize -= writeSizeInBuffer;
            writeOffset += writeSizeInBuffer;
        }

    }


    @Override
    public void write(int b) throws IOException {
        ByteBuffer currentBuffer = getCurrentBuffer();
        if(currentBuffer.hasRemaining()) {
            currentBuffer.put((byte) b);
        } else {
            byteBuffers.add(ByteBuffer.allocateDirect(cacheSize));
            write(b);
        }
    }

    public ByteBuffer[] getByteBuffers() {
        return byteBuffers.toArray(new ByteBuffer[byteBuffers.size()]);
    }


}
