package com.snoworca.IdxDB.store;

import com.snoworca.IdxDB.CompressionType;
import com.snoworca.IdxDB.util.CompressionUtil;
import com.snoworca.IdxDB.util.RefByteArrayOutputStream;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class DataWriter {

    private final static int DEFAULT_BUFFER_LEN = 512 * 1024;

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
            return CompressionUtil.compressGZIP(buffer,true);
        }
        else if(compressionType == CompressionType.Deflater) {
            return CompressionUtil.compressDeflate(buffer,true);
        }
        else if(compressionType == CompressionType.SNAPPY) {
            return CompressionUtil.compressSnappy(buffer,true);
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

    private void replaceSpot(long pos, byte[] buffer, int offset, int len) throws IOException {
        randomAccessFile.seek(pos);
        writeChannel.write(ByteBuffer.wrap(buffer,offset,len));
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
        if(buffer.length <= dataBlock.getCapacity()) {
            lock.lock();
            isLock = true;
            try {
                replaceSpot(pos + DataBlockHeader.HEADER_SIZE, buffer, 0, buffer.length);
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

    private byte[] extendBufferToCapacity(byte[] buffer, int capacity) {
        if(capacity == buffer.length) {
            return buffer;
        }
        byte[] newBuffer = new byte[capacity];
        System.arraycopy(buffer, 0, newBuffer, 0, buffer.length);
        return newBuffer;
    }

    private int calcCapacity(int len) {
        int capacity = (int) (len * (1.0f + capacityRatio));
        return capacity;
    }

    private void initInputDataBlock(DataBlock dataBlock) {
        byte[] buffer = dataBlock.getData();
        dataBlock.setOriginDataCapacity(buffer.length);
        int capacity = calcCapacity(buffer.length);
        buffer = extendBufferToCapacity(buffer, capacity);
        buffer = compress(buffer);
        capacity = buffer.length;
        dataBlock.setCapacity(capacity);
        dataBlock.setData(buffer);
        dataBlock.setCompressionType((byte)compressionType.getValue());
    }


    void write(DataBlock[] dataBlocks) throws IOException {
        int size = 0;

        ArrayDeque<byte[]> originalBuffers = new ArrayDeque<>();
        ArrayList<DataBlock> listOfDataBlockToNewArea = new ArrayList<>();
        RefByteArrayOutputStream byteArrayOutputStream = new RefByteArrayOutputStream(DEFAULT_BUFFER_LEN);
        for(int i = 0; i < dataBlocks.length; ++i) {
            DataBlock dataBlock = dataBlocks[i];
            originalBuffers.addLast(dataBlock.getData());
            initInputDataBlock(dataBlock);
            EmptyBlockPositionPool.EmptyBlockInfo emptyInfo = obtainEmptyBlockPosition(dataBlock.getCapacity());
            if(emptyInfo != null) {
                dataBlock.setCapacity(emptyInfo.getCapacity());
                dataBlock.setPosition(emptyInfo.getPosition());
                replace(dataBlock.getPosition(), dataBlock.getHeader(), dataBlock.getData());
                dataBlock.setData(originalBuffers.pollLast());
            }
            else {
                listOfDataBlockToNewArea.add(dataBlock);
                byte[] buffer = dataBlock.toBytes();
                size += buffer.length;
                byteArrayOutputStream.write(buffer, 0, buffer.length);
            }
        }
        size = byteArrayOutputStream.size();
        lock.lock();
        long pos = this.dataLength.getAndAdd(size);
        replaceSpot(pos, byteArrayOutputStream.getBuffer(),0, size);
        lock.unlock();
        for(int i = 0, n = listOfDataBlockToNewArea.size();i < n; ++i) {
            DataBlock dataBlock = listOfDataBlockToNewArea.get(i);
            dataBlock.setPosition(pos);
            pos += dataBlock.getCapacity() + DataBlockHeader.HEADER_SIZE;
            dataBlock.setData(originalBuffers.pollFirst());
        }

    }

    private EmptyBlockPositionPool.EmptyBlockInfo obtainEmptyBlockPosition(int capacity) {
        lock.lock();
        try {
            isLock = true;
            return emptyBlockPositionPool.obtain(capacity);
        } finally {
            isLock = false;
            lock.unlock();
        }
    }


    private DataBlock write(int collectionID,byte[] buffer, boolean compressed) throws IOException {

        buffer = extendBufferToCapacity(buffer, calcCapacity(buffer.length));
        int originCapacity = buffer.length;
        if(!compressed) {
            buffer = compress(buffer);
        }
        int capacity = buffer.length;

        EmptyBlockPositionPool.EmptyBlockInfo emptyBlockInfo = obtainEmptyBlockPosition(capacity);

        if(emptyBlockInfo != null) {
            long pos = emptyBlockInfo.getPosition();
            DataBlockHeader header =  new DataBlockHeader(collectionID, emptyBlockInfo.getCapacity(), (byte)compressionType.getValue());
            replace(pos, header, buffer);
            DataBlock dataBlock = new DataBlock(header);
            dataBlock.setData(buffer);
            dataBlock.setPosition(pos);
            dataBlock.setOriginDataCapacity(originCapacity);
            return dataBlock;
        }

        DataBlock dataBlock = new DataBlock(new DataBlockHeader(collectionID,capacity, (byte)compressionType.getValue()));
        dataBlock.setCapacity(capacity);
        dataBlock.setData(buffer);
        write(dataBlock);
        dataBlock.setOriginDataCapacity(originCapacity);
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
