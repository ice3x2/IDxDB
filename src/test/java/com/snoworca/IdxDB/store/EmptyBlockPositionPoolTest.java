package com.snoworca.IdxDB.store;

import com.snoworca.IdxDB.store.DataBlockHeader;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class EmptyBlockPositionPoolTest {
    @Test
    public void test() {
        EmptyBlockPositionPool pool = new EmptyBlockPositionPool();
        DataBlock dataBlock = new DataBlock();
        DataBlockHeader dataBlockHeader = new DataBlockHeader(100,32, (byte) 0);
        dataBlock.setHeader(dataBlockHeader);
        pool.push(dataBlock);

        dataBlock = new DataBlock();
        dataBlockHeader = new DataBlockHeader(100,33, (byte) 0);
        dataBlock.setHeader(dataBlockHeader);
        pool.push(dataBlock);

        dataBlock = new DataBlock();
        dataBlockHeader = new DataBlockHeader(1111,33, (byte) 0);
        dataBlock.setHeader(dataBlockHeader);
        pool.push(dataBlock);

        dataBlock = new DataBlock();
        dataBlockHeader = new DataBlockHeader(1111,22, (byte) 0);
        dataBlock.setHeader(dataBlockHeader);
        pool.push(dataBlock);

        DataBlock popBlock = pool.get(32);
        assertEquals(32, popBlock.getCapacity());
        pool.push(dataBlock);

        popBlock = pool.get(35);
        assertEquals(null, popBlock);

        popBlock = pool.get(20);
        assertEquals(22, popBlock.getCapacity());


    }

}