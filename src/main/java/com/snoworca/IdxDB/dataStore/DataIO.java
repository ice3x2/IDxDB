package com.snoworca.IdxDB.dataStore;


import com.snoworca.IdxDB.exception.AccessOutOfRangePositionDataException;
import com.snoworca.IdxDB.util.NumberBufferConverter;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class DataIO {

    private File file;
    private LinkedList<DataReader> dataReaderDeque = new LinkedList<>();
    private final Object dataReaderDequeMonitor = new Object();
    private DataWriter dataWriter;
    private DataIOConfig config;
    private AtomicInteger availableReaders = new AtomicInteger(0);



    public DataIO(File file) {
        this(file, new DataIOConfig());
    }


    public DataIO(File file,DataIOConfig config) {
        this.file = file;
        dataWriter = new DataWriter(file);
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

    public DataBlock getFirstBlock() throws IOException {
        return get(0);
    }


    public DataBlock find(long ID) {

        return null;
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


    public DataBlock writeNull()  throws IOException {
        return write(DataBlock.newNullDataBlock());
    }

    public DataBlock write(Byte value)  throws IOException {
        return write(DataBlock.newDataBlock(value));
    }

    public DataBlock write(Character value)  throws IOException {
        return write(DataBlock.newDataBlock(value));
    }

    public DataBlock write(Short value)  throws IOException {
        return write(DataBlock.newDataBlock(value));
    }

    public DataBlock write(Integer value)  throws IOException {
        return write(DataBlock.newDataBlock(value));
    }

    public DataBlock write(Float value)  throws IOException {
        return write(DataBlock.newDataBlock(value));
    }

    public DataBlock write(Long value)  throws IOException {
        return write(DataBlock.newDataBlock(value));
    }

    public DataBlock write(Double value)  throws IOException {
        return write(DataBlock.newDataBlock(value));
    }

    public DataBlock write(Boolean value)  throws IOException {
        return write(DataBlock.newDataBlock(value));
    }

    public DataBlock write(CharSequence value)  throws IOException {
        return write(DataBlock.newDataBlock(value));
    }


    public DataBlock write(Byte value,long prevPos)  throws IOException {
        DataBlock dataBlock = DataBlock.newDataBlock(value);
        dataBlock.getHeader().setPrev(prevPos);
        return write(dataBlock);
    }

    public DataBlock write(Character value,long prevPos)  throws IOException {
        DataBlock dataBlock = DataBlock.newDataBlock(value);
        dataBlock.getHeader().setPrev(prevPos);
        return write(dataBlock);
    }

    public DataBlock write(Short value,long prevPos)  throws IOException {
        DataBlock dataBlock = DataBlock.newDataBlock(value);
        dataBlock.getHeader().setPrev(prevPos);
        return write(dataBlock);
    }

    public DataBlock write(Integer value,long prevPos)  throws IOException {
        DataBlock dataBlock = DataBlock.newDataBlock(value);
        dataBlock.getHeader().setPrev(prevPos);
        return write(dataBlock);
    }

    public DataBlock write(Float value,long prevPos)  throws IOException {
        DataBlock dataBlock = DataBlock.newDataBlock(value);
        dataBlock.getHeader().setPrev(prevPos);
        return write(dataBlock);
    }

    public DataBlock write(Long value,long prevPos)  throws IOException {
        DataBlock dataBlock = DataBlock.newDataBlock(value);
        dataBlock.getHeader().setPrev(prevPos);
        return write(dataBlock);
    }

    public DataBlock write(Double value,long prevPos)  throws IOException {
        DataBlock dataBlock = DataBlock.newDataBlock(value);
        dataBlock.getHeader().setPrev(prevPos);
        return write(dataBlock);
    }

    public DataBlock write(Boolean value,long prevPos)  throws IOException {
        DataBlock dataBlock = DataBlock.newDataBlock(value);
        dataBlock.getHeader().setPrev(prevPos);
        return write(dataBlock);
    }

    public DataBlock write(CharSequence value,long prevPos)  throws IOException {
        DataBlock dataBlock = DataBlock.newDataBlock(value);
        dataBlock.getHeader().setPrev(prevPos);
        return write(dataBlock);
    }


    public void unlink(long pos) throws IOException {
        DataBlock dataBlock = get(pos);
        //DataBlock nextBlock = null;
        //DataBlock prevBlock = null;
        long currentPos = dataBlock.getPos();
        long prevPos = dataBlock.getHeader().getPrev();
        long nextPos = dataBlock.getHeader().getNext();
        setPrevPos(currentPos, -1);
        setNextPos(currentPos, -1);
        if(nextPos > -1)  {
            setPrevPos(nextPos, prevPos);
        }
        if(prevPos > -1)  {
            setNextPos(prevPos, nextPos);
        }



    }




    public DataBlock write(byte[] buffer) throws IOException {
        DataBlock block = write(DataBlock.newDataBlock(buffer));
        return block;
    }
    private DataBlock write(DataBlock dataBlock) throws IOException {
        try {
            dataWriter.write(dataBlock);
            return dataBlock;
        } catch (IOException | RuntimeException e) {
            throw e;
        } finally {
        }
    }

    public void setNextPos(long currentPos, long nextPos) throws IOException {
        dataWriter.replace(currentPos + DataBlockHeader.HEADER_IDX_NEXT, NumberBufferConverter.fromLong(nextPos));
    }

    public void setPrevPos(long currentPos, long prevPos) throws IOException {
        dataWriter.replace(currentPos + DataBlockHeader.HEADER_IDX_PREV, NumberBufferConverter.fromLong(prevPos));
    }

    public Iterator<DataBlock> iterator(long pos) {
        return new DataBlockIterable(pos, this).iterator();
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
