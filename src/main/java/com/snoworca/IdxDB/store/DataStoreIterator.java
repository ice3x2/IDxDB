package com.snoworca.IdxDB.store;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantLock;

class DataStoreIterator implements Iterator<DataBlock> {

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

    private final ReentrantLock lock;
    private final EmptyBlockPositionPool emptyBlockPositionPool;


    DataStoreIterator(File file, int bufferSize,EmptyBlockPositionPool emptyBlockPositionPool, ReentrantLock lock) {
        this.file = file;
        this.bufferSize = bufferSize;
        this.lock = lock;
        this.emptyBlockPositionPool = emptyBlockPositionPool;
    }

    private void open() throws IOException {
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

    private void nextBufferRead() throws IOException {
        if(isEnd) return;
        currentPosition = nextPosition - lastRemaining;
        lock.lock();
        try {
            byteBuffer.clear();
            randomAccessFile.seek(currentPosition);
            fileChannel.read(byteBuffer);
            nextPosition = currentPosition + bufferSize;
            byteBuffer.flip();
        } finally {
            lock.unlock();
        }
    }

    private DataBlock nextDataBlockAndNextBuffer() throws IOException {
        nextBufferRead();
        if(byteBuffer.remaining() == 0) {
            isEnd = true;
            return null;
        }
        return nextDataBlock();
    }

    private DataBlock nextDataBlock() throws IOException {
        int remaining = byteBuffer.remaining();
        lastRemaining = remaining;
        if(remaining < DataBlockHeader.HEADER_SIZE) {
            return nextDataBlockAndNextBuffer();
        }
        DataBlockHeader dataBlockHeader = DataBlockHeader.fromByteBuffer(byteBuffer);
        int capacity = dataBlockHeader.getCapacity();
        remaining = byteBuffer.remaining();
        if(remaining < capacity) {
            return nextDataBlockAndNextBuffer();
        }
        if(dataBlockHeader.isDeleted()) {
            lock.lock();
            try {
                emptyBlockPositionPool.offer(currentPosition, dataBlockHeader.getCapacity());
            } finally {
                lock.unlock();
            }
            byteBuffer.position(byteBuffer.position() + capacity);
            currentPosition += DataBlockHeader.HEADER_SIZE + capacity;
            return nextDataBlock();
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
