package com.snoworca.IDxDB.data;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class DataWriter {

    private File dataFile;
    FileOutputStream fos;
    private FileChannel fileChannel;
    private long fileLength;

    public DataWriter(File file) {
        this.dataFile = file;
        this.fileLength = file.length();
    }

    public long length() {
        return fileLength;
    }

    public void open() throws IOException {
        if(!this.dataFile.exists()) this.dataFile.createNewFile();
        fos = new FileOutputStream(dataFile);
        fileChannel = fos.getChannel();
    }

    public void close() {
        try {
            fileChannel.close();
        } catch (Exception ignored) {}
        try {
            fos.close();
        } catch (Exception ignored) {}
    }

    public void write(DataBlock block) throws IOException {
        byte[] buffer =  block.toBuffer();
        block.setPos(fileLength);
        fileChannel.write(ByteBuffer.wrap(buffer));
        this.fileLength += buffer.length;

    }

}
