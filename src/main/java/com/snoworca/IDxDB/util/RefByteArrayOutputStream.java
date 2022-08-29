package com.snoworca.IDxDB.util;

import java.io.ByteArrayOutputStream;

public class RefByteArrayOutputStream extends ByteArrayOutputStream {


    public RefByteArrayOutputStream() {
        super();
    }

    public RefByteArrayOutputStream(byte[] buffer) {
        //super(buffer.length);
        buf = buffer;
    }

    public RefByteArrayOutputStream(int size) {
        super(size);
    }

    public void skip(int len) {
        this.count += len;
    }


    public byte[] getByteArray() {
        return buf;
    }
}
