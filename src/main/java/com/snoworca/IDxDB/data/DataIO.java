package com.snoworca.IDxDB.data;

import com.snoworca.IDxDB.util.NumberBufferConverter;

import javax.xml.crypto.Data;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 *
 */
public class DataIO {

    private int readerCapacity = 3;
    private ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private File file;
    private ConcurrentLinkedDeque<DataReader> dataReaderDeque = new ConcurrentLinkedDeque<>();
    private DataWriter dataWriter;

    public DataIO(File file) {
        this.file = file;
        dataWriter = new DataWriter(file);
    }

    public void open() throws IOException {
        if(!this.file.exists()) {
            this.file.createNewFile();
        }
        dataWriter.open();
        initDataReaders();
    }

    private void initDataReaders() throws IOException {
        for(int i = 0; i < readerCapacity; ++i) {
            DataReader reader = new DataReader(file);
            reader.open();
            dataReaderDeque.add(reader);
        }
    }

    public DataBlock getFirstBlock() throws IOException {
        return get(0);
    }


    public DataBlock find(long ID) {

        return null;
    }

    public DataBlock get(long pos) throws IOException {
        DataReader reader = dataReaderDeque.pollFirst();
        ReentrantReadWriteLock.ReadLock readLock = readWriteLock.readLock();
        readLock.lock();
        try {
            if(pos >= dataWriter.length()) {
                return null;
            }
            reader.seek(pos);
            DataBlock dataBlock = reader.read();
            dataReaderDeque.offerLast(reader);
            return dataBlock;
        } catch (IOException | RuntimeException e) {
            throw e;
        } finally {
            readLock.unlock();
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

    public DataBlock write(byte[] buffer) throws IOException {
        return write(DataBlock.newDataBlock(buffer));
    }
    private DataBlock write(DataBlock dataBlock) throws IOException {
        ReentrantReadWriteLock.WriteLock lock = readWriteLock.writeLock();
        lock.lock();
        try {
            dataWriter.write(dataBlock);
            return dataBlock;
        } catch (IOException | RuntimeException e) {
            throw e;
        } finally {
            lock.unlock();
        }
    }

    public void close() {
        ReentrantReadWriteLock.WriteLock lock = readWriteLock.writeLock();
        lock.lock();
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
            lock.unlock();
        }

    }



    public DataBlock next(DataBlock dataBlock) throws IOException {
       long nextPos = dataBlock.getPos() + DataBlockHeader.HEADER_SIZE + dataBlock.getHeader().getLength();
       return get(nextPos);
    }


}
