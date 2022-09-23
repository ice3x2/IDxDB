package com.snoworca.IdxDB.dataStore;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class DataReader {

    private final static int HEADER_BUFFER_SIZE = 512;

    private File dataFile;
    private RandomAccessFile randomAccessFile;
    private FileChannel fileChannel;

    private final ByteBuffer headerBuffer = ByteBuffer.allocateDirect(HEADER_BUFFER_SIZE);
    private ByteBuffer cachedBuffer;

    private long currentPos = 0;
    private long fileSize = 0;

    private int cachedBufferSize = 1024 * 512;


    public DataReader(File file) {
        this.dataFile = file;
        this.fileSize = file.length();
    }

    public DataReader setCachedBufferSize(int size) {
        cachedBufferSize = size;
        cachedBuffer = ByteBuffer.allocateDirect(size);
        return this;
    }



    public DataBlock read() throws IOException {
        try {
            return read(false);
        } catch (Exception e) {
            reopen();
            return read(true);
        }
    }

    private DataBlock read(boolean retry) throws IOException {
        headerBuffer.clear();

        int read =  fileChannel.read(headerBuffer);

        if(read < DataBlockHeader.HEADER_SIZE && !retry) {
            reopen();
            return read(true);
        }
        headerBuffer.flip();
        DataBlockHeader dataBlockHeader = DataBlockHeader.fromByteBuffer(headerBuffer);
        int len = dataBlockHeader.getLength();
        ByteBuffer dataBuffer = cachedBuffer;
        if(len > cachedBufferSize + HEADER_BUFFER_SIZE) {
            dataBuffer = ByteBuffer.allocate(len - HEADER_BUFFER_SIZE + DataBlockHeader.HEADER_SIZE);
        }
        DataBlock dataBlock = null;
        if(len > HEADER_BUFFER_SIZE - DataBlockHeader.HEADER_SIZE) {
            dataBuffer.clear();
            dataBuffer.limit(len - HEADER_BUFFER_SIZE + DataBlockHeader.HEADER_SIZE);
            fileChannel.read(dataBuffer);
            dataBuffer.flip();
            dataBlock = DataBlock.parseData(dataBlockHeader, headerBuffer, dataBuffer);
        } else {
            dataBlock = DataBlock.parseData(dataBlockHeader, headerBuffer);
        }
        dataBlock.setPos(currentPos);
        return dataBlock;
    }

    private void reopen() throws IOException {
        close();
        open();
        seek(currentPos);

    }



    public void seek(long pos) throws IOException {
        if(this.currentPos == pos) {
            return;
        }
        if(pos >= randomAccessFile.length()) {
            reopen();
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
        fileSize = randomAccessFile.length();
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


}
