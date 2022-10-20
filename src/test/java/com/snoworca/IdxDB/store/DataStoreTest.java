package com.snoworca.IdxDB.store;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

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
    void changeDataTest() throws IOException {
        File file = new File("test.dat");
        file.delete();
        DataStoreOptions options = new DataStoreOptions();
        DataStore dataStore = new DataStore(file, new DataStoreOptions().setCapacityRatio(0.3f));
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
        file.delete();
    }

}