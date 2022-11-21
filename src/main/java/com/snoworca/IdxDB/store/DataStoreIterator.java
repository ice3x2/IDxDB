package com.snoworca.IdxDB.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantLock;

class DataStoreIterator implements Iterator<DataBlock> {

    private static Logger LOG = LoggerFactory.getLogger(DataStoreIterator.class);

    private final File file;
    private RandomAccessFile randomAccessFile;
    private FileChannel fileChannel;
    private long nextPosition = 0;
    private long currentPosition = 0;
    private final int bufferSize;
    private int lastRemaining = 0;
    private ByteBuffer byteBuffer;

    private DataBlock next;

    private boolean isEnd = false;
    private boolean isInit = false;
    private final long fileLength;

    private final ReentrantLock lock;
    private final EmptyBlockPositionPool emptyBlockPositionPool;


    DataStoreIterator(File file, int bufferSize,EmptyBlockPositionPool emptyBlockPositionPool, ReentrantLock lock) {
        this.file = file;
        this.bufferSize = bufferSize;
        this.fileLength = file.length();
        this.lock = lock;
        this.emptyBlockPositionPool = emptyBlockPositionPool;
    }

    private void open() throws IOException {
        if(LOG.isDebugEnabled()) {
            LOG.debug("DataStoreIterator.open");
        }
        lock.lock();
        try {
            randomAccessFile = new RandomAccessFile(file, "r");
            randomAccessFile.seek(0);
            byteBuffer = ByteBuffer.allocateDirect(bufferSize);
            fileChannel = randomAccessFile.getChannel();
            byteBuffer.flip();
        } finally {
            lock.unlock();
        }
    }

    private boolean nextBufferRead() throws IOException {
        if(isEnd) return false;
        currentPosition = nextPosition - lastRemaining;
        lock.lock();
        try {
            byteBuffer.clear();
            randomAccessFile.seek(currentPosition);
            int readSize = fileChannel.read(byteBuffer);
            nextPosition = currentPosition + readSize;
            if(LOG.isDebugEnabled()) {
                LOG.debug("DataStoreIterator.nextBufferRead : " + currentPosition + " ~ " + nextPosition + "(" + (int)(((float)nextPosition / fileLength) * 100)  + "%)");
            }
            byteBuffer.flip();
        } finally {
            lock.unlock();
        }
        if(byteBuffer.remaining() == 0) {
            isEnd = true;
            return false;
        }
        return true;
    }




    private DataBlockHeader nextHeaderBlock() throws IOException {
        int remaining = byteBuffer.remaining();
        lastRemaining = remaining;
        if(remaining < DataBlockHeader.HEADER_SIZE) {
            if(!nextBufferRead()) return null;
            return nextHeaderBlock();
        }
        DataBlockHeader dataBlockHeader = DataBlockHeader.fromByteBuffer(byteBuffer);
        int capacity = dataBlockHeader.getCapacity();
        remaining = byteBuffer.remaining();
        if(remaining < capacity) {
            if(!nextBufferRead()) return null;
            return nextHeaderBlock();
        }
        return dataBlockHeader;
    }


    private DataBlock nextDataBlock() throws IOException {
        DataBlockHeader dataBlockHeader = nextHeaderBlock();
        if(dataBlockHeader == null) {
            return null;
        }
        int capacity = dataBlockHeader.getCapacity();
        while(dataBlockHeader.isDeleted()) {
            lock.lock();
            try {
                emptyBlockPositionPool.offer(currentPosition, capacity);
            } finally {
                lock.unlock();
            }
            byteBuffer.position(byteBuffer.position() + capacity);
            currentPosition += DataBlockHeader.HEADER_SIZE + capacity;
            dataBlockHeader = nextHeaderBlock();
            if(dataBlockHeader == null) return null;
            capacity = dataBlockHeader.getCapacity();
        }
        byte[] data = new byte[capacity];
        byteBuffer.get(data);
        DataBlock dataBlock = new DataBlock(dataBlockHeader);
        dataBlock.setData(data);
        dataBlock.setPosition(currentPosition);
        currentPosition += DataBlockHeader.HEADER_SIZE + capacity;
        return dataBlock;
    }


    @Override
    public boolean hasNext() {
        if(!isInit) {
            if(!file.isFile() || !file.canRead()) {
                return false;
            }
            try {
                open();
                next = nextDataBlock();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            isInit = true;
        }
        return !isEnd;
    }

    @Override
    public DataBlock next() {
        DataBlock dataBlock = next;
        try {
            next = nextDataBlock();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return dataBlock;
    }
}
