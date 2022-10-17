package com.snoworca.IdxDB.dataStore;

import com.snoworca.IdxDB.CompressionType;
import com.snoworca.IdxDB.util.CompressionUtil;

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

    private byte[] decompress(CompressionType compressionType, byte[] buffer) {
        if(compressionType == CompressionType.GZIP) {
            return CompressionUtil.decompressGZIP(buffer, 0, buffer.length);
        } else if(compressionType == CompressionType.Deflater) {
            return CompressionUtil.decompressDeflate(buffer, 0, buffer.length);
        } else if(compressionType == CompressionType.SNAPPY) {
            return CompressionUtil.decompressSnappy(buffer, 0, buffer.length);
        }
        return buffer;

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
        CompressionType compressionType = dataBlockHeader.getCompressionType();
        int len = dataBlockHeader.getLength();
        ByteBuffer dataBuffer = cachedBuffer;
        boolean isWrapBuffer = false;
        byte[] decompressed = null;
        if(compressionType != CompressionType.NONE) {
            decompressed = new byte[len];
        }
        else if(len > cachedBufferSize + HEADER_BUFFER_SIZE) {
            if(decompressed != null) {
                isWrapBuffer = true;
                dataBuffer = ByteBuffer.wrap(decompressed);
            }  else {
                dataBuffer = ByteBuffer.allocate(len - HEADER_BUFFER_SIZE + DataBlockHeader.HEADER_SIZE);
            }
        }
        DataBlock dataBlock = null;
        if(len > HEADER_BUFFER_SIZE - DataBlockHeader.HEADER_SIZE) {
            dataBuffer.clear();
            dataBuffer.limit(len - HEADER_BUFFER_SIZE + DataBlockHeader.HEADER_SIZE);
            fileChannel.read(dataBuffer);
            dataBuffer.flip();
            if(compressionType == CompressionType.NONE) {
                dataBlock = DataBlock.parseData(dataBlockHeader, headerBuffer, dataBuffer);
            } else if(isWrapBuffer){
                decompressed = decompress(compressionType, decompressed);
                dataBlock = DataBlock.parseData(dataBlockHeader, headerBuffer, ByteBuffer.wrap(decompressed));
            } else {
                dataBuffer.get(decompressed, 0, decompressed.length);
                decompressed = decompress(compressionType, decompressed);
                dataBlock = DataBlock.parseData(dataBlockHeader, headerBuffer, ByteBuffer.wrap(decompressed));
            }

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
