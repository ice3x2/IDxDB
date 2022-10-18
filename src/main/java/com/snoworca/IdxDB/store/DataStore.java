package com.snoworca.IdxDB.store;

import com.snoworca.IdxDB.CompressionType;
import com.snoworca.IdxDB.exception.AccessOutOfRangePositionDataException;
import com.snoworca.IdxDB.util.NumberBufferConverter;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;

public class DataStore {

    private File file;
    private LinkedList<DataReader> dataReaderDeque = new LinkedList<>();
    private final Object dataReaderDequeMonitor = new Object();
    private DataWriter dataWriter;
    private DataIOConfig config;
    private AtomicInteger availableReaders = new AtomicInteger(0);

    private CompressionType compressionType = CompressionType.NONE;



    public DataStore(File file) {
        this(file, new DataIOConfig());
    }


    public DataStore(File file,DataIOConfig config) {
        compressionType = config.getCompressionType();
        this.file = file;
        dataWriter = new DataWriter(file,compressionType);
        this.config = config;
        availableReaders.set(this.config.getReaderCapacity());

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
            for (int i = 0, n = config.getReaderCapacity(); i < n; ++i) {
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
        if(pos < 0) return null;
        long fileLength = dataWriter.length();
        if(pos >= fileLength) {
            throw new AccessOutOfRangePositionDataException(fileLength, pos);
        }
        DataReader reader = obtainDataReader();
        try {
            if(pos >= fileLength) {
                return null;
            }
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



    public DataBlock writeOrReplace(byte[] buffer, long pos, float capacityRatio) throws IOException {
        if(pos < 0) {
            return write(buffer, capacityRatio);
        }
        DataBlock dataBlock = get(pos);
        DataBlockHeader dataBlockHeader = dataBlock.getHeader();
        int capacity = dataBlockHeader.getCapacity();
        if(capacity <  buffer.length) {
            unlink(pos);
            return write(buffer, capacityRatio);
        }
        dataWriter.changeData(dataBlock, buffer);
        return dataBlock;
    }

    public long write(byte[] buffer, float capacityRatio) throws IOException {
        DataBlock block = dataWriter.write();
        return block;
    }

    public void unlink(long pos) throws IOException {


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


}
