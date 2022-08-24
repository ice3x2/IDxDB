package com.snoworca.IDxDB.data;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class DataReader {


    private File dataFile;
    private RandomAccessFile randomAccessFile;
    private FileChannel fileChannel;


    public DataReader(File file) {
        this.dataFile = file;
    }

    public void readAndApplyTopID() throws IOException {
        long len = dataFile.length();
        long startPos = len - DataBlockHeader.HEADER_SIZE;

        randomAccessFile.seek(startPos);
        ByteBuffer buffer = ByteBuffer.wrap(new byte[(int)(len - startPos)]);
        fileChannel.read(buffer);
        byte[] raw = buffer.array();

        DataBlockHeader dataHeader = DataBlockHeader.fromBuffer(raw);
        DataBlock.parseData(dataHeader, raw);
        //randomAccessFile.read()
        //DataHeader.fromBuffer()

    }

    public void open() throws IOException {
        if(!this.dataFile.exists()) this.dataFile.createNewFile();
        randomAccessFile = new RandomAccessFile(this.dataFile, "r");
        fileChannel = randomAccessFile.getChannel();
    }

    public void close() {
        try {
            fileChannel.close();
        } catch (Exception ignored) {}
        try {
            randomAccessFile.close();
        } catch (Exception ignored) {}
    }

    public void write(DataBlock block) throws IOException {
        fileChannel.write(ByteBuffer.wrap(block.toBuffer()));
    }

}
