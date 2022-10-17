package com.snoworca.IdxDB.dataStore;


import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class DataIOTest {

    private static String makeRandomString() {
        int leftLimit = 97; // letter 'a'
        int rightLimit = 122; // letter 'z'
        int targetStringLength = ThreadLocalRandom.current().nextInt(3000) + 3000;
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
    void zeroPosBugTest() throws IOException {
        File file = new File("./zeroPosBugTest.dat");
        file.delete();
        DataIO dataIO = new DataIO(file);
        dataIO.open();
        long posZero = dataIO.write(makeRandomString()).getPos();
        long posOne = dataIO.write(makeRandomString()).getPos();
        dataIO.setNextPos(posZero, posOne);
        dataIO.setPrevPos(posOne, posZero);
        dataIO.close();

        dataIO = new DataIO(file);
        dataIO.open();

        DataBlock dataBlock = dataIO.get(0);
        assertEquals(dataBlock.getHeader().getNext(), posOne);
        file.delete();


    }


    @Test
    void writeAndRandomGet() throws IOException {

        File file = new File("./testdata112.dat");
        file.delete();
        DataIO dataIO = new DataIO(file);
        dataIO.open();

        ArrayList<DataBlock> dataBlocks = new ArrayList<>();
        HashMap<Long, DataBlock> dataBlockHashMap = new HashMap<>();

        int testCase = 100000;
        long start = System.currentTimeMillis();
        for (int i = 0; i < testCase; ++i) {
            DataBlock wroteDataBlock = null;
            if(ThreadLocalRandom.current().nextInt(10) == 0) {
                wroteDataBlock = dataIO.write(ThreadLocalRandom.current().nextBoolean());
            } else {
                wroteDataBlock = dataIO.write(makeRandomString());
            }
            dataBlocks.add(wroteDataBlock);
            dataBlockHashMap.put(wroteDataBlock.getPos(), wroteDataBlock);
        }
        System.out.println("쓰기 속도: " + (System.currentTimeMillis() - start) + "ms");
        start = System.currentTimeMillis();
        Collections.shuffle(dataBlocks);
        System.out.println("섞기 완료" + (System.currentTimeMillis() - start) + "ms");
        start = System.currentTimeMillis();
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
    void writeAndRandomGetOnMultiThreadShuffleTime() throws IOException, InterruptedException {

        File file = new File("./random.dat");
        file.delete();
        DataIO dataIO = new DataIO(file);
        dataIO.open();

        ConcurrentLinkedDeque<DataBlock> writeBlocks = new ConcurrentLinkedDeque<>();
        ConcurrentHashMap<Long, DataBlock> dataBlockMap = new ConcurrentHashMap<>();

        Executor writeExecutor = Executors.newFixedThreadPool(3);
        Executor readExecutor = Executors.newFixedThreadPool(32);

        final int testCase = 100000;
        AtomicInteger writeCounter = new AtomicInteger(testCase);
        AtomicInteger readCounter = new AtomicInteger(testCase);

        AtomicLong start = new AtomicLong(System.currentTimeMillis());
        for (int i = 0, n = writeCounter.get(); i < n; ++i) {
            writeExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        DataBlock wroteDataBlock = null;
                        if (ThreadLocalRandom.current().nextInt(10) == 0) {
                            wroteDataBlock = dataIO.write(ThreadLocalRandom.current().nextBoolean());
                        } else {
                            wroteDataBlock = dataIO.write(makeRandomString());
                        }
                        writeBlocks.add(wroteDataBlock);
                        dataBlockMap.put(wroteDataBlock.getPos(), wroteDataBlock);
                        writeCounter.decrementAndGet();
                        if(writeCounter.get() <= 0) {
                            System.out.println("멀티스레드 쓰기 속도: " + (System.currentTimeMillis() - start.get()) + "ms");
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }

        start.set(System.currentTimeMillis());
        while(readCounter.get() > 0) {
            final DataBlock dataBlock = writeBlocks.pollFirst();
            if(dataBlock == null) {
                Thread.sleep(5);
                continue;
            }
            readExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        if (dataBlock != null) {
                            DataBlock originBlock = dataBlockMap.get(dataBlock.getPos());
                            long pos = dataBlock.getPos();
                            DataBlock loadedBlock = dataIO.get(pos);
                            assertArrayEquals(loadedBlock.getData(), originBlock.getData());
                            assertEquals(loadedBlock.getPos(), originBlock.getPos());
                            assertEquals(loadedBlock.getHeader(), originBlock.getHeader());
                            readCounter.decrementAndGet();
                            if(readCounter.get() <= 0) {
                                System.out.println("멀티스레드 읽기 속도: " + (System.currentTimeMillis() - start.get()) + "ms");
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                }
            });
        }


        file.delete();
    }


    @Test
    void writeAndRandomGetOnMultiThread() throws IOException, InterruptedException {

        File file = new File("./random.dat");
        file.delete();
        DataIO dataIO = new DataIO(file);
        dataIO.open();

        ConcurrentLinkedDeque<DataBlock> writeBlocks = new ConcurrentLinkedDeque<>();
        ConcurrentHashMap<Long, DataBlock> dataBlockMap = new ConcurrentHashMap<>();

        Executor writeExecutor = Executors.newFixedThreadPool(3);
        Executor readExecutor = Executors.newFixedThreadPool(32);

        final int testCase = 100000;
        AtomicInteger writeCounter = new AtomicInteger(testCase);
        AtomicInteger readCounter = new AtomicInteger(testCase);

        AtomicLong start = new AtomicLong(System.currentTimeMillis());
        for (int i = 0, n = writeCounter.get(); i < n; ++i) {
            writeExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        DataBlock wroteDataBlock = null;
                        if (ThreadLocalRandom.current().nextInt(10) == 0) {
                            wroteDataBlock = dataIO.write(ThreadLocalRandom.current().nextBoolean());
                        } else {
                            wroteDataBlock = dataIO.write(makeRandomString());
                        }
                        writeBlocks.add(wroteDataBlock);
                        dataBlockMap.put(wroteDataBlock.getPos(), wroteDataBlock);
                        writeCounter.decrementAndGet();
                        if(writeCounter.get() <= 0) {
                            System.out.println("멀티스레드 쓰기 속도: " + (System.currentTimeMillis() - start.get()) + "ms");
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }


        while(writeCounter.get() != 0) {
            Thread.sleep(1);
        }

        ArrayList<DataBlock> shuffled = new ArrayList<>(writeBlocks);
        Collections.shuffle(shuffled);
        writeBlocks.clear();
        writeBlocks.addAll(shuffled);

        start.set(System.currentTimeMillis());
        while(readCounter.get() > 0) {
            final DataBlock dataBlock = writeBlocks.pollFirst();
            if(dataBlock == null) {
                continue;
            }
            readExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        if (dataBlock != null) {
                            DataBlock originBlock = dataBlockMap.get(dataBlock.getPos());
                            long pos = dataBlock.getPos();
                            DataBlock loadedBlock = dataIO.get(pos);
                            assertArrayEquals(loadedBlock.getData(), originBlock.getData());
                            assertEquals(loadedBlock.getPos(), originBlock.getPos());
                            assertEquals(loadedBlock.getHeader(), originBlock.getHeader());
                            readCounter.decrementAndGet();
                            if(readCounter.get() <= 0) {
                                System.out.println("멀티스레드 읽기 속도: " + (System.currentTimeMillis() - start.get()) + "ms");
                            }
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }


        file.delete();
    }


    @Test
    void dataChangeTest() throws IOException {
        File file = new File("./dataChangeTest.dat");
        file.delete();
        String randomString = makeRandomString();
        DataIO dataIO = new DataIO(file);
        dataIO.open();

        DataBlock block = dataIO.writeOrReplace(randomString.getBytes(),-1,1.0f);
        long pos = block.getPos();
        int len = block.getHeader().getLength();
        block = dataIO.writeOrReplace("test".getBytes(), pos, 1.0f);
        assertEquals(pos, block.getPos());
        assertNotEquals(len, block.getHeader().getLength());
        len = block.getHeader().getLength();
        assertEquals(new String(dataIO.get(pos).getData()), "test");


        block = dataIO.writeOrReplace("1es1".getBytes(), pos, 1.0f);
        assertEquals(pos, block.getPos());
        assertEquals(len, block.getHeader().getLength());
        assertEquals(new String(dataIO.get(block.getPos()).getData()), "1es1");

        block = dataIO.writeOrReplace((randomString + randomString + randomString).getBytes(), pos, 1.0f);
        assertNotEquals(pos, block.getPos());
        assertEquals(block.getHeader().getCapacity(), (randomString + randomString + randomString).getBytes().length * 2);





        dataIO.close();
        file.delete();
    }

    @Test
    void next() throws IOException {
        File file = new File("./linked.dat");
        file.delete();
        DataIO dataIO = new DataIO(file);
        dataIO.open();

        LinkedList<DataBlock> dataBlocks = new LinkedList<>();

        int testCase = 10000;
        long start = System.currentTimeMillis();
        long startPos= 0;
        long lastPos = -1;
        for (int i = 0; i < testCase; ++i) {
            DataBlock wroteDataBlock = null;
            if(ThreadLocalRandom.current().nextInt(10) == 0) {
                wroteDataBlock = dataIO.write(ThreadLocalRandom.current().nextBoolean(), lastPos);
            } else {
                wroteDataBlock = dataIO.write(makeRandomString(), lastPos);
            }
            if(lastPos > -1) {
                dataIO.setNextPos(lastPos, wroteDataBlock.getPos());
            } else {
                startPos = wroteDataBlock.getPos();
            }
            lastPos = wroteDataBlock.getPos();

            dataBlocks.add(wroteDataBlock);
        }
        System.out.println("쓰기 속도: " + (System.currentTimeMillis() - start) + "ms");
        start = System.currentTimeMillis();

        dataIO.close();

        dataIO = new DataIO(file);
        dataIO.open();

        Iterator<DataBlock> iteratorCache = dataBlocks.iterator();
        Iterator<DataBlock> iteratorStore = dataIO.iterator(startPos);
        int count = 0;
        while(iteratorStore.hasNext()) {
            DataBlock block = iteratorStore.next();
            DataBlock blockOrigin = iteratorCache.next();
            if(count % 100 == 0 && count != 0) {
                iteratorStore.remove();
                iteratorCache.remove();
            }
            assertArrayEquals(blockOrigin.getData(), block.getData());
            ++count;
        }



        iteratorCache = dataBlocks.iterator();
        iteratorStore = dataIO.iterator(startPos);
        while(iteratorStore.hasNext()) {
            DataBlock block = iteratorStore.next();
            DataBlock blockOrigin = iteratorCache.next();
            assertArrayEquals(blockOrigin.getData(), block.getData());
        }





    }
}