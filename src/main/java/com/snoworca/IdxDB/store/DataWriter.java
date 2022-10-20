package com.snoworca.IdxDB.store;

import com.snoworca.IdxDB.CompressionType;
import com.snoworca.IdxDB.util.CompressionUtil;

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

    private final EmptyBlockPositionPool emptyBlockPositionPool;

    DataWriter(File file,float capacityRatio,  CompressionType compressionType, EmptyBlockPositionPool emptyBlockPositionPool) {
        this.dataFile = file;
        this.dataLength.set(file.length());
        this.fileSize.set(file.length());
        this.compressionType = compressionType;
        this.capacityRatio = capacityRatio;
        this.emptyBlockPositionPool = emptyBlockPositionPool;
    }


    ReentrantLock getLock() {
        return lock;
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

    private void replaceSpot(long pos, byte[] buffer) throws IOException {
        randomAccessFile.seek(pos);
        writeChannel.write(ByteBuffer.wrap(buffer));
    }

    void unlink(long pos, int capacity) throws IOException {
        if(pos < 0) return;
        lock.lock();
        isLock = true;
        try {
            randomAccessFile.seek(pos + DataBlockHeader.HEADER_COLLECTION_ID);
            ByteBuffer buffer = ByteBuffer.allocate(4);
            buffer.putInt(DataBlockHeader.DELETED_ID);
            buffer.flip();
            writeChannel.write(buffer);
            emptyBlockPositionPool.offer(pos,capacity);
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
        if(buffer.length <= dataBlock.getHeader().getCapacity()) {
            lock.lock();
            isLock = true;
            try {
                replaceSpot(pos + DataBlockHeader.HEADER_SIZE, buffer);
                dataBlock.setData(buffer);
                return dataBlock;
            } finally {
                isLock = false;
                lock.unlock();
            }
        }
        unlink(pos, dataBlock.getHeader().getCapacity());
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
        lock.lock();
        isLock = true;
        EmptyBlockPositionPool.EmptyBlockInfo emptyBlockInfo;
        try {
            emptyBlockInfo = emptyBlockPositionPool.obtain(capacity);
        } finally {
            isLock = false;
            lock.unlock();
        }
        if(emptyBlockInfo != null) {
            long pos = emptyBlockInfo.getPosition();
            DataBlockHeader header =  new DataBlockHeader(collectionID, emptyBlockInfo.getCapacity(), (byte)compressionType.getValue());
            replace(pos, header, buffer);
            DataBlock dataBlock = new DataBlock(header);
            dataBlock.setData(buffer);
            dataBlock.setPosition(pos);
            return dataBlock;
        }

        DataBlock dataBlock = new DataBlock(new DataBlockHeader(collectionID,capacity, (byte)compressionType.getValue()));
        if(capacity > buffer.length) {
            byte[] newBuffer = new byte[capacity];
            System.arraycopy(buffer,0,newBuffer,0,buffer.length);
            buffer = newBuffer;
        }
        dataBlock.setData(buffer);
        write(dataBlock);
        return dataBlock;
    }

    private void replace(long pos, DataBlockHeader header, byte[] data) throws IOException {
        lock.lock();
        isLock = true;
        try {
            randomAccessFile.seek(pos);
            writeChannel.write(ByteBuffer.wrap(header.toBytes()));
            randomAccessFile.seek(pos + DataBlockHeader.HEADER_SIZE);
            writeChannel.write(ByteBuffer.wrap(data));
        } finally {
            isLock = false;
            lock.unlock();
        }
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
