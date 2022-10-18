package com.snoworca.IdxDB.store;

import com.snoworca.IdxDB.CompressionType;
import com.snoworca.IdxDB.util.CompressionUtil;
import com.snoworca.IdxDB.util.NumberBufferConverter;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class DataWriter {

    private File dataFile;
    private RandomAccessFile randomAccessFile;
    private FileChannel writeChannel;

    private CompressionType compressionType = CompressionType.NONE;

    private AtomicLong dataLength = new AtomicLong(0);
    private AtomicLong fileSize = new AtomicLong(0);

    private ReentrantLock lock = new ReentrantLock();
    private boolean isLock = false;

    DataWriter(File file, CompressionType compressionType) {
        this.dataFile = file;
        this.dataLength.set(file.length());
        this.fileSize.set(file.length());
        this.compressionType = compressionType;
    }

    public long length() {
        return dataLength.get();
    }
    public long getFileSize() {
        return fileSize.get();
    }

    public boolean writeable() {
        return isLock;
    }


    private byte[] compress(byte[] buffer) {
        if(compressionType == CompressionType.GZIP) {
            return CompressionUtil.compressGZIP(buffer);
        }
        else if(compressionType == CompressionType.Deflater) {
            return CompressionUtil.compressDeflate(buffer);
        }
        else if(compressionType == CompressionType.SNAPPY) {
            return CompressionUtil.compressSnappy(buffer);
        }
        return buffer;

    }


    public void open() throws IOException {
        lock.lock();
        isLock = true;
        try {
            boolean isNew = !this.dataFile.exists();
            if (isNew) {
                this.dataFile.createNewFile();
            }
            randomAccessFile = new RandomAccessFile(this.dataFile, "rw");
            randomAccessFile.seek(randomAccessFile.length());
            writeChannel = randomAccessFile.getChannel();
            fileSize.set(this.dataFile.length());
            this.dataLength.set(fileSize.get());
        } catch (IOException e) {
            throw e;
        } finally {
            isLock = false;
            lock.unlock();
        }
    }



    public void close() {
        lock.lock();
        isLock = true;
        try {
            writeChannel.close();
            randomAccessFile.close();
        } catch (Exception ignored) {}
        isLock = false;
        lock.unlock();
    }

    public void replace(long pos, byte[] buffer) throws IOException {
        lock.lock();
        isLock = true;
        try {
            randomAccessFile.seek(pos);
            writeChannel.write(ByteBuffer.wrap(buffer));
        } finally {
            isLock = false;
            lock.unlock();
        }
    }

    public long changeData(DataBlock dataBlock, byte[] buffer) throws IOException {
        lock.lock();
        isLock = true;
        try {
            long pos = dataBlock.getPosition();
            buffer = compress(buffer);
            if(buffer.length <= dataBlock.getHeader().getCapacity()) {
                replace(pos + DataBlockHeader.HEADER_SIZE, buffer);
            } else {
                lock.unlock();
                dataBlock.setData(buffer);
            }
            dataBlock.setData(buffer);

        } finally {
            isLock = false;
            lock.unlock();
        }
    }

    public long write(int collectionID,byte[] buffer,float capacityRatio) throws IOException {
        buffer = compress(buffer);
        int capacity = (int)(buffer.length * (capacityRatio + 1.0f));
        DataBlock dataBlock = new DataBlock();
        dataBlock.setHeader(new DataBlockHeader(collectionID,capacity, (byte)compressionType.getValue()));
        dataBlock.setData(buffer);
        write(dataBlock);
        return dataBlock.getPosition();
    }

    private void write(DataBlock block) throws IOException {
        lock.lock();
        isLock = true;
        try {
            block.toBytes()

            writeChannel.write(ByteBuffer.wrap(block));
            this.dataLength.addAndGet(DataBlockHeader.HEADER_SIZE);

            byte[] dataBuffer = block.getPayload();;
            dataBuffer = compress(dataBuffer);
            randomAccessFile.seek(this.dataLength.get());
            writeChannel.write(ByteBuffer.wrap(dataBuffer));
            this.dataLength.addAndGet(dataBuffer.length);
        } finally {
            isLock = false;
            lock.unlock();
        }
    }

}
