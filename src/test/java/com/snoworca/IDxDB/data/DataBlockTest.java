package com.snoworca.IDxDB.data;


import org.junit.jupiter.api.Test;

import javax.xml.crypto.Data;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.*;

public class DataBlockTest {

    @Test
    public void dataBlockParsing8ByteValueTest() {
        long time = System.currentTimeMillis();
        DataBlock block = DataBlock.newDataBlock(time);
        byte[] buffer = block.toBuffer();

        DataBlockHeader header = DataBlockHeader.fromBuffer(buffer);
        DataBlock distBlock = DataBlock.parseData(header, buffer, DataBlockHeader.HEADER_SIZE);
        assertEquals(distBlock.getValue(), block.getValue());
    }

    @Test
    public void dataBlockParsing4ByteValueTest() {
        int value = ThreadLocalRandom.current().nextInt();
        DataBlock block = DataBlock.newDataBlock(value);
        byte[] buffer = block.toBuffer();

        DataBlockHeader header = DataBlockHeader.fromBuffer(buffer);
        DataBlock distBlock = DataBlock.parseData(header, buffer, DataBlockHeader.HEADER_SIZE);
        assertEquals(distBlock.getValue(), block.getValue());
        assertEquals(distBlock.getValue(), value);
        assertTrue( distBlock.getValue() instanceof  Integer);
    }

    @Test
    public void dataBlockParsing1ByteValueTest() {
        byte value = (byte) ThreadLocalRandom.current().nextInt(128);
        DataBlock block = DataBlock.newDataBlock(value);
        byte[] buffer = block.toBuffer();

        DataBlockHeader header = DataBlockHeader.fromBuffer(buffer);
        DataBlock distBlock = DataBlock.parseData(header, buffer, DataBlockHeader.HEADER_SIZE);
        assertEquals(distBlock.getValue(), block.getValue());
        assertEquals(distBlock.getValue(), value);
        assertTrue( distBlock.getValue() instanceof  Byte);
    }

    @Test
    public void dataBlockParsingStringValueTest() {
        String value = UUID.randomUUID().toString();
        DataBlock block = DataBlock.newDataBlock(value);
        byte[] buffer = block.toBuffer();

        DataBlockHeader header = DataBlockHeader.fromBuffer(buffer);
        DataBlock distBlock = DataBlock.parseData(header, buffer, DataBlockHeader.HEADER_SIZE);
        assertEquals(distBlock.getValue(), block.getValue());
        assertEquals(distBlock.getValue(), value);
        assertTrue( distBlock.getValue() instanceof  String);
    }


    @Test
    public void dataBlockParsingByteArrayValueTest() {
        int len = ThreadLocalRandom.current().nextInt(1000) + 1000;
        byte[] value = new byte[len];
        ThreadLocalRandom.current().nextBytes(value);
        DataBlock block = DataBlock.newDataBlock(value);
        byte[] buffer = block.toBuffer();

        DataBlockHeader header = DataBlockHeader.fromBuffer(buffer);
        DataBlock distBlock = DataBlock.parseData(header, buffer, DataBlockHeader.HEADER_SIZE);
        assertArrayEquals( (byte[])distBlock.getValue(), (byte[])block.getValue());
        assertArrayEquals((byte[])distBlock.getValue(), value);
        assertEquals( ((byte[])distBlock.getValue()).length, len);
    }




}