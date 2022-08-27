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

    private ByteBuffer headerBuffer = ByteBuffer.allocateDirect(DataBlockHeader.HEADER_SIZE);
    private ByteBuffer cachedBuffer;

    private long currentPos = 0;

    private int cachedBufferSize = 1024 * 512;


    public DataReader(File file) {
        this.dataFile = file;
    }

    public DataReader setCachedBufferSize(int size) {
        cachedBufferSize = size;
        cachedBuffer = ByteBuffer.allocateDirect(size);
        return this;
    }



    public DataBlock read() throws IOException {
        headerBuffer.clear();
        fileChannel.read(headerBuffer);
        headerBuffer.flip();
        DataBlockHeader dataBlockHeader = DataBlockHeader.fromByteBuffer(headerBuffer);
        int len = dataBlockHeader.getLength();
        ByteBuffer dataBuffer = cachedBuffer;
        if(len > cachedBufferSize) {
            dataBuffer = ByteBuffer.allocate(len);
        }
        dataBuffer.clear();
        dataBuffer.limit(len);
        fileChannel.read(dataBuffer);
        dataBuffer.flip();
        DataBlock dataBlock = DataBlock.parseData(dataBlockHeader, dataBuffer);
        dataBlock.setPos(currentPos);
        currentPos += DataBlockHeader.HEADER_SIZE + len;
        return dataBlock;

    }



    public void seek(long pos) throws IOException {
        if(this.currentPos == pos) {
            return;
        }
        randomAccessFile.seek(pos);
        this.currentPos = pos;
    }

    public long getPos() {
        return currentPos;
    }


    public void open() throws IOException {
        if(!this.dataFile.exists()) this.dataFile.createNewFile();
        randomAccessFile = new RandomAccessFile(this.dataFile, "r");
        fileChannel = randomAccessFile.getChannel();
        if(cachedBuffer == null) {
            cachedBuffer = ByteBuffer.allocateDirect(cachedBufferSize);
        }
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
