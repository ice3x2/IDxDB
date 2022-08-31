package com.snoworca.IDxDB.data;

import org.junit.jupiter.api.Test;

import javax.xml.crypto.Data;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.*;

class DataIOTest {

    private static String makeRandomString() {
        int leftLimit = 97; // letter 'a'
        int rightLimit = 122; // letter 'z'
        int targetStringLength = 2000 - 13;///ThreadLocalRandom.current().nextInt(3000) + 5;
        Random random = new Random();
        StringBuilder buffer = new StringBuilder(targetStringLength);
        for (int i = 0; i < targetStringLength; i++) {
            int randomLimitedInt = leftLimit + (int)(random.nextFloat() * (rightLimit - leftLimit + 1));
            buffer.append((char) randomLimitedInt);
        }
        String generatedString = buffer.toString();
        return generatedString;
    }

    @Test
    void limitReader() throws IOException {
        String randomString = makeRandomString();
        File file = new File("./testdata3");
        file.delete();
        DataIO dataIO = new DataIO(file);
        dataIO.open();

        ArrayList<DataBlock> dataBlocks = new ArrayList<>();

        long start = System.currentTimeMillis();
        for (int i = 0; i < 10000; ++i) {
            DataBlock wroteDataBlock = dataIO.write(randomString);
            dataBlocks.add(wroteDataBlock);

        }

        Runnable testRunner = new Runnable() {
            @Override
            public void run() {


            }
        };
        file.delete();


    }


    @Test
    void writeAndRandomGet() throws IOException {
        String randomString = makeRandomString();
        File file = new File("./testdata2");
        file.delete();
        DataIO dataIO = new DataIO(file);
        dataIO.open();

        ArrayList<DataBlock> dataBlocks = new ArrayList<>();
        HashMap<Long, DataBlock> dataBlockHashMap = new HashMap<>();


        for (int i = 0; i < 1000; ++i) {
            DataBlock wroteDataBlock = dataIO.write(randomString);
            dataBlocks.add(wroteDataBlock);
            dataBlockHashMap.put(wroteDataBlock.getPos(), wroteDataBlock);
            /*if( i % 10000 == 0) {
                System.out.println( (i / 10000) + "번 완료" );
            }*/
        }

        Collections.shuffle(dataBlocks);
        long start = System.currentTimeMillis();
        for(int i = 0, n = dataBlocks.size(); i < n; ++i) {
            DataBlock block = dataBlocks.get(i);
            long pos = block.getPos();
            DataBlock loadedBlock = dataIO.get(pos);
            assertArrayEquals(loadedBlock.getData(), block.getData());
            assertEquals(loadedBlock.getPos(), block.getPos());
            assertEquals(loadedBlock.getHeader(), block.getHeader());
        }

        System.out.println("랜덤 읽기 속도: " + (System.currentTimeMillis() - start) + "ms");
        file.delete();


    }


    @Test
    void writeAndNext() throws IOException {

        String randomString = makeRandomString();
        File file = new File("./testdata");
        file.delete();
        DataIO dataIO = new DataIO(file);
        dataIO.open();


        ArrayList<DataBlock> dataBlocks = new ArrayList<>();

        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
            DataBlock wroteDataBlock = dataIO.write(randomString);
            dataBlocks.add(wroteDataBlock);
            /*if( i % 10000 == 0) {
                System.out.println( (i / 10000) + "번 완료" );
            }*/

        }
        System.out.println("순차 쓰기속도: " + (System.currentTimeMillis() - start) + "ms");
        System.out.println("파일사이즈: " + (file.length() / 1024f / 1024f) + "mb");
        start = System.currentTimeMillis();
        DataBlock firstBlock = dataIO.getFirstBlock();
        DataBlock nextBlock = firstBlock;
        for(int i = 0, n = dataBlocks.size(); i < n; ++i) {
            assertArrayEquals(nextBlock.getData(), dataBlocks.get(i).getData());
            assertEquals(nextBlock.getPos(), dataBlocks.get(i).getPos());
            assertEquals(nextBlock.getHeader(), dataBlocks.get(i).getHeader());
            nextBlock = dataIO.next(nextBlock);
        }

        System.out.println("순차 읽기 속도: " + (System.currentTimeMillis() - start) + "ms");

        dataIO.close();

        file.delete();
    }

    @Test
    void start() {
    }

    @Test
    void next() {
    }
}