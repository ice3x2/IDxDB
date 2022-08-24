package com.snoworca.IDxDB.data;


import org.junit.jupiter.api.Test;

import javax.xml.crypto.Data;

public class DataBlockTest {

    @Test
    public void dataBlockParsingTest() {
        long time = System.currentTimeMillis();
        DataBlock block = DataBlock.newDataBlock(time);
        byte[] buffer = block.toBuffer();

        DataBlockHeader header = DataBlockHeader.fromBuffer(buffer);
        DataBlock distBlock = DataBlock.parseData(header, buffer, DataBlockHeader.HEADER_SIZE);



    }

}