package com.snoworca.IdxDB.store;

import com.snoworca.IdxDB.CompressionType;
import com.snoworca.cson.CSONArray;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DataStoreTest {

    final static char[] charSet = new char[] {
            '0','1','2','3','4','5','6','7','8','9',
            'a','b','c','d','e','f','g','h','i','j','k','l','m',
            'n','o','p','q','r','s','t','u','v','w','x','y','z',
            'A','B','C','D','E','F','G','H','I','J','K','L','M',
            'N','O','P','Q','R','S','T','U','V','W','X','Y','Z',
            '~','!','@','#','$','%','^','&','*','(',')','-','_','+','=','|','[',']','{','}',';',':',',','.','/'
    };
    public static String getRandomString(int length) {
        int index = 0;

        StringBuffer sb = new StringBuffer();
        for (int i=0; i<length; i++) {
            index = (int) (charSet.length * Math.random());
            sb.append(charSet[index]);
        }
        return sb.toString();
    }


    @Test
    void dataStoreTestForSmallData() throws IOException {
        File file = new File("test.dat");
        file.delete();
        DataStore dataStore = new DataStore(file);
        dataStore.open();
        dataStore.write(1, getRandomString(100).getBytes());
        byte[] buffer = getRandomString(100).getBytes();
        long pos = dataStore.write(1, buffer).getPosition();
        DataBlock readBuffer = dataStore.get(pos);
        byte[] readData = readBuffer.getData();
        for(int i = 0, n = buffer.length; i < n; ++i) {
            assertEquals(buffer[i], readData[i]);
        }
        dataStore.close();
        file.delete();
    }

    @Test
    void dataStoreTestForSizeOfAmbiguous() throws IOException {
        File file = new File("test.dat");
        file.delete();
        DataStore dataStore = new DataStore(file);
        dataStore.open();
        byte[] buffer = new byte[512 + DataBlockHeader.HEADER_SIZE];
        Random random = new Random(System.currentTimeMillis());
        random.nextBytes(buffer);
        dataStore.write(1, buffer);
        random.nextBytes(buffer);
        long pos = dataStore.write(1, buffer).getPosition();
        DataBlock readBuffer = dataStore.get(pos);
        byte[] readData = readBuffer.getData();
        for(int i = 0, n = buffer.length; i < n; ++i) {
            assertEquals(buffer[i], readData[i]);
        }
        dataStore.close();
        file.delete();
    }


    @Test
    void dataStoreTestForBigData() throws IOException {
        File file = new File("test.dat");
        file.delete();
        DataStore dataStore = new DataStore(file);
        dataStore.open();
        dataStore.write(1, getRandomString(10000).getBytes());
        byte[] buffer = getRandomString(10000).getBytes();
        long pos = dataStore.write(1, buffer).getPosition();
        DataBlock readBuffer = dataStore.get(pos);
        byte[] readData = readBuffer.getData();
        for(int i = 0, n = buffer.length; i < n; ++i) {
            assertEquals(buffer[i], readData[i]);
        }
        dataStore.close();
        file.delete();
    }


    @Test
    void unlinkTest() throws IOException {
        File file = new File("test.dat");
        file.delete();
        DataStore dataStore = new DataStore(file);
        dataStore.open();
        dataStore.write(1, getRandomString(10000).getBytes());
        byte[] buffer = getRandomString(10000).getBytes();
        DataBlock dataBlock = dataStore.write(100, buffer);
        long pos = dataBlock.getPosition();
        int capacity = dataBlock.getCapacity();
        DataBlock readBuffer = dataStore.get(pos);
        assertEquals(100, readBuffer.getCollectionId());
        dataStore.unlink(pos, dataBlock.getCapacity());
        readBuffer = dataStore.get(pos);
        assertEquals(-1, readBuffer.getCollectionId());

        byte[] newData = new byte[]{1,2,3,4,5,6,7,8,9,10};
        dataBlock = dataStore.write(100, newData);
        dataBlock = dataStore.get(dataBlock.getPosition());
        assertEquals(pos, dataBlock.getPosition());
        assertEquals(capacity, dataBlock.getCapacity());
        assertEquals(100, dataBlock.getCollectionId());
        for(int i = 0; i < newData.length; ++i) {
            assertEquals(newData[i], dataBlock.getData()[i]);
        }
        dataStore.close();
        file.delete();
    }

    @Test
    void multiWriteTest() throws IOException {
        File file = new File("test.dat");
        file.delete();
        DataStore dataStore = new DataStore(file);
        dataStore.open();
        ArrayList<DataBlock> dataBlocks = new ArrayList<>();
        ArrayList<CSONArray> datas = new ArrayList<>();
        dataStore.write(100, getRandomString(10000).getBytes());
        for(int i = 0; i < 10000; ++i) {
            CSONArray csonArray = new CSONArray().push(getRandomString(1000));
            datas.add(csonArray);
           dataBlocks.add(DataBlock.createWriteDataBlock(1, csonArray.toByteArray()));
        }
        dataStore.write(dataBlocks.toArray(new DataBlock[dataBlocks.size()]));
        assertEquals(dataBlocks.size(), datas.size());
        for(int i = 0, n = dataBlocks.size(); i < n; ++i) {
            DataBlock dataBlock = dataBlocks.get(i);
            assertEquals(dataBlock.getData().length, dataBlock.getCapacity());
            assertEquals(new CSONArray(dataBlock.getData()).get(0), datas.get(i).get(0));
        }
        Random random = new Random(System.currentTimeMillis());
        Iterator<DataBlock> iterator = dataBlocks.iterator();
        int removeCount = 0;
        while(iterator.hasNext()) {
            DataBlock dataBlock = iterator.next();
            if(random.nextInt(3)  == 0) {
                dataStore.unlink(dataBlock.getPosition(), dataBlock.getCapacity());
                iterator.remove();
                ++removeCount;
            }
        }
        dataBlocks.clear();
        datas.clear();
        assertEquals(removeCount, dataStore.getEmptyBlockPositionPoolSize());
        for(int i = 0, n = removeCount * 2; i < n; ++i) {
            CSONArray csonArray = new CSONArray().push(getRandomString(1000));
            datas.add(csonArray);
            dataBlocks.add(DataBlock.createWriteDataBlock(1, csonArray.toByteArray()));
        }
        dataStore.write(dataBlocks.toArray(new DataBlock[dataBlocks.size()]));
        assertEquals(0, dataStore.getEmptyBlockPositionPoolSize());

        for(int i = 0, n = dataBlocks.size(); i < n; ++i) {
            DataBlock dataBlock = dataBlocks.get(i);
            assertEquals(dataBlock.getData().length, dataBlock.getCapacity());
            assertEquals(new CSONArray(dataBlock.getData()).get(0), datas.get(i).get(0));
        }

        dataStore.close();
        file.delete();
    }


    @Test
    void changeDataTest() throws IOException {
        File file = new File("test.dat");
        file.delete();
        DataStore dataStore = new DataStore(file, new DataStoreOptions().setCapacityRatio(0.3f).setCompressionType(CompressionType.Deflater));
        dataStore.open();
        Random random = new Random(System.currentTimeMillis());
        dataStore.write(1, getRandomString(random.nextInt(10000) + 1).getBytes());
        byte[] buffer = getRandomString(10000).getBytes();
        long pos = dataStore.write(1, buffer).getPosition();
        DataBlock readBlock = dataStore.get(pos);
        byte[] readData = readBlock.getData();
        for(int i = 0, n = buffer.length; i < n; ++i) {
            assertEquals(buffer[i], readData[i]);
        }
        buffer = getRandomString(13000).getBytes();
        long changePos = dataStore.replaceOrWrite(1, buffer, readBlock.getPosition()).getPosition();
        assertEquals(pos,changePos);

        readBlock = dataStore.get(changePos);
        readData = readBlock.getData();
        for(int i = 0, n = 13000; i < n; ++i) {
            assertEquals(buffer[i], readData[i]);
        }

        dataStore.close();
        System.out.println(file.length());
        file.delete();
    }

    @Test
    public void syncTest() {

        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        Object monitor = new Object();
        long start = System.currentTimeMillis();
        long k = 0;
        ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
        for(int i = 0; i < 10000000; ++i) {
            readLock.lock();
            ++k;
            readLock.unlock();
        }
        System.out.println(System.currentTimeMillis() - start);
        start = System.currentTimeMillis();
        k = 0;
        for(int i = 0; i < 10000000; ++i) {

                ++k;

        }

        System.out.println(System.currentTimeMillis() - start);
    }

}