package com.snoworca.IdxDB.store;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DataPartitionTest {

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
        DataPartition dataPartition = new DataPartition(file);
        dataPartition.open();
        dataPartition.write(1, getRandomString(100).getBytes());
        byte[] buffer = getRandomString(100).getBytes();
        long pos = dataPartition.write(1, buffer);
        DataBlock readBuffer = dataPartition.get(pos);
        byte[] readData = readBuffer.getData();
        for(int i = 0, n = buffer.length; i < n; ++i) {
            assertEquals(buffer[i], readData[i]);
        }
        dataPartition.close();
        file.delete();
    }

    @Test
    void dataStoreTestForSizeOfAmbiguous() throws IOException {
        File file = new File("test.dat");
        file.delete();
        DataPartition dataPartition = new DataPartition(file);
        dataPartition.open();
        byte[] buffer = new byte[512 + DataBlockHeader.HEADER_SIZE];
        Random random = new Random(System.currentTimeMillis());
        random.nextBytes(buffer);
        dataPartition.write(1, buffer);
        random.nextBytes(buffer);
        long pos = dataPartition.write(1, buffer);
        DataBlock readBuffer = dataPartition.get(pos);
        byte[] readData = readBuffer.getData();
        for(int i = 0, n = buffer.length; i < n; ++i) {
            assertEquals(buffer[i], readData[i]);
        }
        dataPartition.close();
        file.delete();
    }


    @Test
    void dataStoreTestForBigData() throws IOException {
        File file = new File("test.dat");
        file.delete();
        DataPartition dataPartition = new DataPartition(file);
        dataPartition.open();
        dataPartition.write(1, getRandomString(10000).getBytes());
        byte[] buffer = getRandomString(10000).getBytes();
        long pos = dataPartition.write(1, buffer);
        DataBlock readBuffer = dataPartition.get(pos);
        byte[] readData = readBuffer.getData();
        for(int i = 0, n = buffer.length; i < n; ++i) {
            assertEquals(buffer[i], readData[i]);
        }
        dataPartition.close();
        file.delete();
    }


    @Test
    void unlinkTest() throws IOException {
        File file = new File("test.dat");
        file.delete();
        DataPartition dataPartition = new DataPartition(file);
        dataPartition.open();
        dataPartition.write(1, getRandomString(10000).getBytes());
        byte[] buffer = getRandomString(10000).getBytes();
        long pos = dataPartition.write(100, buffer);
        DataBlock readBuffer = dataPartition.get(pos);
        assertEquals(100, readBuffer.getCollectionId());
        dataPartition.unlink(pos);
        readBuffer = dataPartition.get(pos);
        assertEquals(-1, readBuffer.getCollectionId());
        dataPartition.close();
        file.delete();

    }


    @Test
    void changeDataTest() throws IOException {
        File file = new File("test.dat");
        file.delete();
        DataStoreOptions options = new DataStoreOptions();
        DataPartition dataPartition = new DataPartition(file, new DataStoreOptions().setCapacityRatio(0.3f));
        dataPartition.open();
        Random random = new Random(System.currentTimeMillis());
        dataPartition.write(1, getRandomString(random.nextInt(10000) + 1).getBytes());
        byte[] buffer = getRandomString(10000).getBytes();
        long pos = dataPartition.write(1, buffer);
        DataBlock readBlock = dataPartition.get(pos);
        byte[] readData = readBlock.getData();
        for(int i = 0, n = buffer.length; i < n; ++i) {
            assertEquals(buffer[i], readData[i]);
        }
        buffer = getRandomString(13000).getBytes();
        long changePos = dataPartition.replaceOrWrite(1, buffer, readBlock.getPosition());
        assertEquals(pos,changePos);

        readBlock = dataPartition.get(changePos);
        readData = readBlock.getData();
        for(int i = 0, n = 13000; i < n; ++i) {
            assertEquals(buffer[i], readData[i]);
        }
        dataPartition.close();
        file.delete();
    }

}