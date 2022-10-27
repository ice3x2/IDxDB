package com.snoworca.IdxDB.store;

import com.snoworca.IdxDB.CompressionType;
import com.snoworca.IdxDB.exception.AccessOutOfRangePositionDataException;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class DataStore implements Iterable<DataBlock> {

    private final File file;
    private final LinkedList<DataReader> dataReaderDeque = new LinkedList<>();
    private final Object dataReaderDequeMonitor = new Object();
    private final DataWriter dataWriter;
    private final DataStoreOptions config;

    private final EmptyBlockPositionPool emptyBlockPositionPool = new EmptyBlockPositionPool();

    public DataStore(File file) {
        this(file, new DataStoreOptions());
    }


    int getEmptyBlockPositionPoolSize() {
        ReentrantLock lock = dataWriter.getLock();
        lock.lock();
        try {
            return emptyBlockPositionPool.size();
        }finally {
            lock.unlock();
        }

    }


    public DataStore(File file, DataStoreOptions config) {
        CompressionType compressionType = config.getCompressionType();
        AtomicInteger availableReaders = new AtomicInteger(0);
        this.file = file;
        float capacityRatio = config.getCapacityRatio();
        dataWriter = new DataWriter(file, capacityRatio, compressionType, emptyBlockPositionPool);
        this.config = config;
        availableReaders.set(this.config.getReaderSize());
    }


    public void open() throws IOException {
        if(!this.file.exists()) {
            this.file.createNewFile();
        }
        dataWriter.open();
        initDataReaders();
    }

    private void initDataReaders() throws IOException {
        synchronized (dataReaderDequeMonitor) {
            for (int i = 0, n = config.getReaderSize(); i < n; ++i) {
                DataReader reader = new DataReader(file);
                reader.open();
                dataReaderDeque.add(reader);
            }
        }
    }

    public DataReader obtainDataReader() {
        synchronized (dataReaderDequeMonitor) {
            if(dataReaderDeque.isEmpty()) {
                try {
                    dataReaderDequeMonitor.wait();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            if(dataReaderDeque.isEmpty()) {
                return obtainDataReader();
            }
            return dataReaderDeque.pollFirst();
        }
    }

    public void releaseDataReader(DataReader dataReader) {
        synchronized (dataReaderDequeMonitor) {
            dataReaderDeque.offerLast(dataReader);
            dataReaderDequeMonitor.notify();
        }
    }

    public DataBlock get(long pos) throws IOException {
        //noinspection DuplicatedCode
        if(pos < 0) return null;
        long fileLength = dataWriter.length();
        if(pos >= fileLength) {
            throw new AccessOutOfRangePositionDataException(fileLength, pos);
        }
        DataReader reader = obtainDataReader();
        try {
            reader.seek(pos);
            DataBlock dataBlock = null;
            try {
                dataBlock = reader.read();
            } catch (RuntimeException e) {
                System.err.println(pos);
                throw e;
            }
            releaseDataReader(reader);
            return dataBlock;
        } catch (IOException | RuntimeException e) {
            throw e;
        }
    }

    public byte[] getData(long pos) throws IOException {
        if(pos < 0) return null;
        return get(pos).getData();
    }



    public DataBlock replaceOrWrite(int collectionID, byte[] buffer, long pos) throws IOException {
        if(pos < 0) {
            return write(collectionID, buffer);
        }
        DataBlock dataBlock = get(pos);
        if(dataBlock == null) {
            return write(collectionID, buffer);
        }
        return dataWriter.changeData(dataBlock, buffer);
    }

    public DataBlock write(int collectionID, byte[] buffer) throws IOException {
        DataBlock block = dataWriter.write(collectionID, buffer);
        return block;
    }

    public void write(DataBlock[] dataBlocks) throws IOException {
        dataWriter.write(dataBlocks);
    }


    public void unlink(long pos) throws IOException {
        DataBlock dataBlock = get(pos);
        if(dataBlock == null) return;
        dataWriter.unlink(pos, dataBlock.getCapacity());
    }

    public void unlink(long pos, int capacity) throws IOException {
         dataWriter.unlink(pos, capacity);
    }





    public void close() {
        try {
            dataWriter.close();
            Iterator<DataReader> iterator = dataReaderDeque.iterator();
            while (iterator.hasNext()) {
                DataReader reader = iterator.next();
                reader.close();
            }
            dataReaderDeque.clear();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
        }
    }


    @Override
    public Iterator<DataBlock> iterator() {
        return new DataStoreIterator(file, this.config.getIterableBufferSize(),emptyBlockPositionPool,  dataWriter.getLock());
    }
}
