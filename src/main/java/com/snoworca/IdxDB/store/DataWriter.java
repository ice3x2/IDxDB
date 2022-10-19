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

    private final File dataFile;
    private RandomAccessFile randomAccessFile;
    private FileChannel writeChannel;

    private CompressionType compressionType = CompressionType.NONE;

    private final AtomicLong dataLength = new AtomicLong(0);
    private final AtomicLong fileSize = new AtomicLong(0);
    private float capacityRatio = 0.3f;

    private final ReentrantLock lock = new ReentrantLock();
    private boolean isLock = false;

    DataWriter(File file,float capacityRatio,  CompressionType compressionType) {
        this.dataFile = file;
        this.dataLength.set(file.length());
        this.fileSize.set(file.length());
        this.compressionType = compressionType;
        this.capacityRatio = capacityRatio;
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

    private void replace(long pos, byte[] buffer) throws IOException {
        randomAccessFile.seek(pos);
        writeChannel.write(ByteBuffer.wrap(buffer));
    }

    void unlink(long pos) throws IOException {
        if(pos < 0) return;
        lock.lock();
        isLock = true;
        try {
            randomAccessFile.seek(pos + DataBlockHeader.HEADER_COLLECTION_ID);
            ByteBuffer buffer = ByteBuffer.allocate(4);
            buffer.putInt(-1);
            buffer.flip();
            writeChannel.write(buffer);
        } catch (IOException e) {
            throw e;
        } finally {
            isLock = false;
            lock.unlock();
        }

    }

    DataBlock changeData(DataBlock dataBlock, byte[] buffer) throws IOException {
        long pos = dataBlock.getPosition();
        buffer = compress(buffer);
        if(pos < 0) {
            return write(dataBlock.getCollectionId(), buffer, true);
        }
        lock.lock();
        isLock = true;
        try {
            if(buffer.length <= dataBlock.getHeader().getCapacity()) {
                replace(pos + DataBlockHeader.HEADER_SIZE, buffer);
                dataBlock.setData(buffer);
                return dataBlock;
            }
        } finally {
            isLock = false;
            lock.unlock();
        }
        return write(dataBlock.getCollectionId(), buffer, true);
    }


    DataBlock write(int collectionID,byte[] buffer) throws IOException {
        return write(collectionID, buffer, false);
    }

    private DataBlock write(int collectionID,byte[] buffer, boolean compressed) throws IOException {
        if(!compressed) {
            buffer = compress(buffer);
        }
        int capacity = (int)(buffer.length * (capacityRatio + 1.0f));
        DataBlock dataBlock = new DataBlock();
        dataBlock.setHeader(new DataBlockHeader(collectionID,capacity, (byte)compressionType.getValue()));
        if(capacity > buffer.length) {
            byte[] newBuffer = new byte[capacity];
            System.arraycopy(buffer,0,newBuffer,0,buffer.length);
            buffer = newBuffer;
        }
        dataBlock.setData(buffer);
        write(dataBlock);
        return dataBlock;
    }

    private void write(DataBlock block) throws IOException {
        lock.lock();
        isLock = true;
        try {
            byte[] buffer = block.toBytes();
            randomAccessFile.seek(this.dataLength.get());
            writeChannel.write(ByteBuffer.wrap(buffer));
            block.setPosition(this.dataLength.get());
            this.dataLength.addAndGet(buffer.length);
        } finally {
            isLock = false;
            lock.unlock();
        }
    }

}
