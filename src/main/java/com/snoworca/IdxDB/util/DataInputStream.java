package com.snoworca.IdxDB.util;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.IllegalFormatConversionException;

public class DataInputStream extends InputStream {

    private final InputStream inputStream;

    public DataInputStream(InputStream inputStream) {
        this.inputStream = inputStream;
    }

    public int readInt() throws IOException {
        byte[] bytes = new byte[4];
        int read = inputStream.read(bytes);
        if(read != 4) {
            throw new IndexOutOfBoundsException("read int error");
        }
        return (bytes[0] & 0xFF) << 24 | (bytes[1] & 0xFF) << 16 | (bytes[2] & 0xFF) << 8 | (bytes[3] & 0xFF);
    }

    public long readLong() throws IOException {
        byte[] bytes = new byte[8];
        int read = inputStream.read(bytes);
        if(read != 8) {
            throw new IndexOutOfBoundsException("read long error");
        }
        return new BigInteger(bytes).longValue();
    }

    public byte readByte() throws IOException {
        return (byte) inputStream.read();
    }

    public float readFloat() throws IOException {
        byte[] bytes = new byte[4];
        int read = inputStream.read(bytes);
        if(read != 4) {
            throw new IndexOutOfBoundsException("read float error");
        }
        return Float.intBitsToFloat((bytes[0] & 0xFF) << 24 | (bytes[1] & 0xFF) << 16 | (bytes[2] & 0xFF) << 8 | (bytes[3] & 0xFF));
    }

    public double readDouble() throws IOException {
        byte[] bytes = new byte[8];
        int read = inputStream.read(bytes);
        if(read != 8) {
            throw new IndexOutOfBoundsException("read double error");
        }
        return Double.longBitsToDouble(new BigInteger(bytes).longValue());
    }

    public String readString() throws IOException {
        int length = readInt();
        byte[] bytes = new byte[length];
        int read = inputStream.read(bytes);
        if(read != length) {
            throw new IndexOutOfBoundsException("read string error");
        }
        return new String(bytes);
    }

    public boolean readBoolean() throws IOException {
        return inputStream.read() == 1;
    }

    public char readChar() throws IOException {
        byte[] bytes = new byte[2];
        int read = inputStream.read(bytes);
        if(read != 2) {
            throw new IndexOutOfBoundsException("read char error");
        }
        return (char) ((bytes[0] & 0xFF) << 8 | (bytes[1] & 0xFF));
    }

    public short readShort() throws IOException {
        byte[] bytes = new byte[2];
        int read = inputStream.read(bytes);
        if(read != 2) {
            throw new IndexOutOfBoundsException("read short error");
        }
        return (short) ((bytes[0] & 0xFF) << 8 | (bytes[1] & 0xFF));
    }

    public byte[] readByteArray() throws IOException {
        int length = readInt();
        byte[] bytes = new byte[length];
        int read = inputStream.read(bytes);
        if(read != length) {
            throw new IndexOutOfBoundsException("read bytes error");
        }
        return bytes;
    }

    public int readIntArray() throws IOException {
        int length = readInt();
        int[] ints = new int[length];
        for(int i = 0; i < length; i++) {
            ints[i] = readInt();
        }
        return length;
    }

    public long readLongArray() throws IOException {
        int length = readInt();
        long[] longs = new long[length];
        for(int i = 0; i < length; i++) {
            longs[i] = readLong();
        }
        return length;
    }


    public int read(byte[] b) throws IOException {
        return inputStream.read(b);
    }

    public int read(byte[] b, int off, int len) throws IOException {
        return inputStream.read(b, off, len);
    }

    public long skip(long n) throws IOException {
        return inputStream.skip(n);
    }

    public int available() throws IOException {
        return inputStream.available();
    }

    public void close() throws IOException {
        inputStream.close();
    }

    public synchronized void mark(int readlimit) {
        inputStream.mark(readlimit);
    }

    public synchronized void reset() throws IOException {
        inputStream.reset();
    }

    public boolean markSupported() {
        return inputStream.markSupported();
    }

    public int read() throws IOException {
        return inputStream.read();
    }







}
