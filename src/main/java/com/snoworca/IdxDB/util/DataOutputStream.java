package com.snoworca.IdxDB.util;

import java.io.IOException;
import java.io.OutputStream;

public class DataOutputStream extends OutputStream {

    private final OutputStream outputStream;
    private final boolean isCloseAndClose;

    public DataOutputStream(OutputStream outputStream) {
        this.outputStream = outputStream;
        isCloseAndClose = false;
    }

    private DataOutputStream(OutputStream outputStream, boolean isCloseAndClose) {
        this.outputStream = outputStream;
        this.isCloseAndClose = isCloseAndClose;
    }

    public void writeBoolean(boolean v) throws IOException {
        outputStream.write(v ? 1 : 0);
    }

    public void writeByte(int v) throws IOException {
        outputStream.write(v);
    }

    public void writeShort(short v) throws IOException {
        outputStream.write((v >>> 8) & 0xFF);
        outputStream.write((v >>> 0) & 0xFF);
    }

    public void writeChar(char v) throws IOException {
        outputStream.write((v >>> 8) & 0xFF);
        outputStream.write((v >>> 0) & 0xFF);
    }

    public void writeString(String v) throws IOException {
        byte[] bytes = v.getBytes();
        writeInt(bytes.length);
        outputStream.write(bytes);
    }

    public void writeInt(int[] v) throws IOException {
        writeInt(v.length);
        for(int i = 0; i < v.length; i++) {
            writeInt(v[i]);
        }
    }

    public void writeChar(char[] v) throws IOException {
        writeInt(v.length);
        for(int i = 0; i < v.length; i++) {
            writeChar(v[i]);
        }
    }

    public void writeLong(long[] v) throws IOException {
        writeInt(v.length);
        for(int i = 0; i < v.length; i++) {
            writeLong(v[i]);
        }
    }



    public void writeLong(long v) throws IOException {
        outputStream.write((int) (v >>> 56) & 0xFF);
        outputStream.write((int) (v >>> 48) & 0xFF);
        outputStream.write((int) (v >>> 40) & 0xFF);
        outputStream.write((int) (v >>> 32) & 0xFF);
        outputStream.write((int) (v >>> 24) & 0xFF);
        outputStream.write((int) (v >>> 16) & 0xFF);
        outputStream.write((int) (v >>> 8) & 0xFF);
        outputStream.write((int) (v >>> 0) & 0xFF);
    }

    public void writeInt(int v) throws IOException {
        outputStream.write((v >>> 24) & 0xFF);
        outputStream.write((v >>> 16) & 0xFF);
        outputStream.write((v >>> 8) & 0xFF);
        outputStream.write((v >>> 0) & 0xFF);
    }

    public void writeFloat(float v) throws IOException {
        writeInt(Float.floatToIntBits(v));
    }

    public void writeDouble(double v) throws IOException {
        writeLong(Double.doubleToLongBits(v));
    }

    @Override
    public void write(byte[] b) throws IOException {
        outputStream.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        outputStream.write(b, off, len);
    }

    @Override
    public void write(int b) throws IOException {
        outputStream.write(b);
    }

    @Override
    public void flush() throws IOException {
        outputStream.flush();
    }

    @Override
    public void close() throws IOException {
        if(isCloseAndClose) {
            flush();
            outputStream.close();
        }

    }
}
