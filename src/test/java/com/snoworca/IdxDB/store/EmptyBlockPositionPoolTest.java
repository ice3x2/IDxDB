package com.snoworca.IdxDB.store;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class EmptyBlockPositionPoolTest {
    @Test
    public void test() {
        EmptyBlockPositionPool pool = new EmptyBlockPositionPool();
        EmptyBlockPositionPool.EmptyBlockInfo emptyBlockInfo = new EmptyBlockPositionPool.EmptyBlockInfo(0, 32);
        pool.offer(emptyBlockInfo);

        emptyBlockInfo = new EmptyBlockPositionPool.EmptyBlockInfo(100, 33);
        pool.offer(emptyBlockInfo);

        emptyBlockInfo = new EmptyBlockPositionPool.EmptyBlockInfo(1111, 33);
        pool.offer(emptyBlockInfo);

        emptyBlockInfo = new EmptyBlockPositionPool.EmptyBlockInfo(1230, 22);
        pool.offer(emptyBlockInfo);

        EmptyBlockPositionPool.EmptyBlockInfo popBlock = pool.obtain(32);
        assertEquals(32, popBlock.getCapacity());


        popBlock = pool.obtain(35);
        assertEquals(null, popBlock);

        popBlock = pool.obtain(20);
        assertEquals(22, popBlock.getCapacity());
        assertEquals(1230, popBlock.getPosition());

        popBlock = pool.obtain(33);
        assertEquals(33, popBlock.getCapacity());
        assertEquals(100, popBlock.getPosition());

        popBlock = pool.obtain(30);
        assertEquals(33, popBlock.getCapacity());
        assertEquals(1111, popBlock.getPosition());

        popBlock = pool.obtain(30);
        assertEquals(null, popBlock);

        assertTrue(pool.isEmpty());
    }

    @Test
    public void limitRatioTest() {
        EmptyBlockPositionPool pool = new EmptyBlockPositionPool(1);
        EmptyBlockPositionPool.EmptyBlockInfo emptyBlockInfo = new EmptyBlockPositionPool.EmptyBlockInfo(100,10);
        pool.offer(emptyBlockInfo);

        emptyBlockInfo = new EmptyBlockPositionPool.EmptyBlockInfo(100,20);
        pool.offer(emptyBlockInfo);
        emptyBlockInfo = new EmptyBlockPositionPool.EmptyBlockInfo(100,30);
        pool.offer(emptyBlockInfo);


        assertEquals(10, pool.obtain(5).getCapacity());
        assertEquals(null, pool.obtain(5));
        assertEquals(20, pool.obtain(10).getCapacity());
        assertEquals(null, pool.obtain(10));


    }

}