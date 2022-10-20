package com.snoworca.IdxDB.store;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class EmptyBlockPositionPoolTest {
    @Test
    public void test() {
        EmptyBlockPositionPool pool = new EmptyBlockPositionPool();
        DataBlock dataBlock = new DataBlock(new DataBlockHeader(100,32, (byte) 0));
        pool.push(dataBlock);

        dataBlock = new DataBlock(new DataBlockHeader(100,33, (byte) 0));
        pool.push(dataBlock);

        dataBlock = new DataBlock(new DataBlockHeader(1111,33, (byte) 0));
        pool.push(dataBlock);

        dataBlock = new DataBlock(new DataBlockHeader(1,22, (byte) 0));
        pool.push(dataBlock);

        DataBlock popBlock = pool.obtain(32);
        assertEquals(32, popBlock.getCapacity());


        popBlock = pool.obtain(35);
        assertEquals(null, popBlock);

        popBlock = pool.obtain(20);
        assertEquals(22, popBlock.getCapacity());
        assertEquals(1, popBlock.getCollectionId());

        popBlock = pool.obtain(33);
        assertEquals(33, popBlock.getCapacity());
        assertEquals(100, popBlock.getCollectionId());

        popBlock = pool.obtain(30);
        assertEquals(33, popBlock.getCapacity());
        assertEquals(1111, popBlock.getCollectionId());

        popBlock = pool.obtain(30);
        assertEquals(null, popBlock);

        assertTrue(pool.isEmpty());
    }

    @Test
    public void limitRatioTest() {
        EmptyBlockPositionPool pool = new EmptyBlockPositionPool(1);
        DataBlock dataBlock = new DataBlock(new DataBlockHeader(100,10, (byte) 0));
        pool.push(dataBlock);

        dataBlock = new DataBlock(new DataBlockHeader(100,20, (byte) 0));
        pool.push(dataBlock);
        dataBlock = new DataBlock(new DataBlockHeader(100,30, (byte) 0));
        pool.push(dataBlock);


        assertEquals(10, pool.obtain(5).getCapacity());
        assertEquals(null, pool.obtain(5));
        assertEquals(20, pool.obtain(10).getCapacity());
        assertEquals(null, pool.obtain(10));


    }

}