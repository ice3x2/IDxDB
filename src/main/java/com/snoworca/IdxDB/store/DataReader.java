package com.snoworca.IdxDB.store;

import com.snoworca.IdxDB.CompressionType;
import com.snoworca.IdxDB.util.CompressionUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentLinkedQueue;

public class DataReader {


    private ConcurrentLinkedQueue<ByteBuffer> bufferPool = new ConcurrentLinkedQueue<>();

    private File dataFile;
    private RandomAccessFile randomAccessFile;
    private FileChannel fileChannel;

    private long currentPos = 0;
    private long fileSize = 0;

    private int cachedBufferSize = 512;

    DataReader(int cachedBufferSize, File file) {
        this.cachedBufferSize = cachedBufferSize;
        this.dataFile = file;
    }

    // 버퍼풀에서 버퍼를 가져온다.
    private ByteBuffer getBuffer() {
        ByteBuffer buffer = bufferPool.poll();
        if (buffer == null) {
            buffer = ByteBuffer.allocateDirect(cachedBufferSize);
        }
        buffer.clear();
        return buffer;
    }

    // 버퍼풀에 버퍼를 반환한다.
    private void returnBuffer(ByteBuffer buffer) {
        bufferPool.offer(buffer);
    }




    public DataReader(File file) {
        this.dataFile = file;
        this.fileSize = file.length();
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

    private void readBytes(ByteBuffer[] bufferPool, byte[] buffer) {
        int readSize = 0;
        for(int i = 0, n = bufferPool.length; i < n;++i) {
            ByteBuffer bufferItem = bufferPool[i];
            int readSizeInBuffer = Math.min(buffer.length - readSize, bufferItem.remaining());
            bufferItem.get(buffer, readSize, readSizeInBuffer);
            readSize += readSizeInBuffer;
        }

    }




    private DataBlock read(boolean retry) throws IOException {
        ByteBuffer headerBuffer = getBuffer();
        int read =  fileChannel.read(headerBuffer);
        if(read < DataBlockHeader.HEADER_SIZE && !retry) {
            reopen();
            return read(true);
        }
        headerBuffer.flip();
        DataBlockHeader dataBlockHeader = DataBlockHeader.fromByteBuffer(headerBuffer);
        byte compressionByteType = dataBlockHeader.getCompressionType();
        CompressionType compressionType = CompressionType.fromValue(compressionByteType);
        int capacity = dataBlockHeader.getCapacity();
        DataBlock dataBlock = null;
        if(dataBlockHeader.getCapacity() <= headerBuffer.remaining()) {
            if(compressionType == CompressionType.NONE) {
                dataBlock = DataBlock.fromByteBuffer(dataBlockHeader, headerBuffer);
            } else {
                byte[] buffer = new byte[capacity];
                headerBuffer.get(buffer, 0, buffer.length);
                byte[] decompressed = decompress(compressionType, buffer);
                dataBlock = DataBlock.fromByteBuffer(dataBlockHeader,decompressed);
            }
            returnBuffer(headerBuffer);
        } else {
            int totalReadSize = capacity + DataBlockHeader.HEADER_SIZE;
            int arraySize = (totalReadSize / cachedBufferSize) + (totalReadSize % cachedBufferSize > 0 ? 1 : 0);
            ByteBuffer[] byteBuffers = new ByteBuffer[arraySize];
            byteBuffers[0] = headerBuffer;
            for(int i = 1, n = byteBuffers.length;i < n; ++i) {
                byteBuffers[i] = getBuffer();
            }
            fileChannel.read(byteBuffers, 1, arraySize - 1);
            for(int i = 1, n = byteBuffers.length;i < n; ++i) {
                byteBuffers[i].flip();
            }
            byteBuffers[0].position(DataBlockHeader.HEADER_SIZE);
            if(compressionType == CompressionType.NONE) {
                dataBlock = DataBlock.fromByteBuffers(dataBlockHeader,byteBuffers);
                dataBlock.setPosition(currentPos);
            } else {
                byte[] buffer = new byte[capacity];
                readBytes(byteBuffers, buffer);
                byte[] decompressed = decompress(compressionType, buffer);
                dataBlock = DataBlock.fromByteBuffer(dataBlockHeader,decompressed);
            }
            for(int i = 0, n = byteBuffers.length;i < n; ++i) {
                returnBuffer(byteBuffers[i]);
            }
        }
        dataBlock.setPosition(currentPos);
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
