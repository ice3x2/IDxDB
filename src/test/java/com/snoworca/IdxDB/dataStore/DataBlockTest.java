package com.snoworca.IdxDB.dataStore;


import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.*;

public class DataBlockTest {

    @Test
    public void dataBlockParsing8ByteValueTest() {
        long time = System.currentTimeMillis();
        DataBlock block = DataBlock.newDataBlock(time);
        byte[] buffer = block.toBuffer();

        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);

        DataBlockHeader header = DataBlockHeader.fromByteBuffer(byteBuffer);
        DataBlock distBlock = DataBlock.parseData(header, byteBuffer);
        assertEquals(distBlock.getValue(), block.getValue());
    }

    @Test
    public void dataBlockParsing4ByteValueTest() {
        int value = ThreadLocalRandom.current().nextInt();
        DataBlock block = DataBlock.newDataBlock(value);
        byte[] buffer = block.toBuffer();

        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);

        DataBlockHeader header = DataBlockHeader.fromByteBuffer(byteBuffer);
        DataBlock distBlock = DataBlock.parseData(header, byteBuffer);
        assertEquals(distBlock.getValue(), block.getValue());
        assertEquals(distBlock.getValue(), value);
        assertTrue( distBlock.getValue() instanceof  Integer);
    }

    @Test
    public void dataBlockParsing1ByteValueTest() {
        byte value = (byte) ThreadLocalRandom.current().nextInt(128);
        DataBlock block = DataBlock.newDataBlock(value);
        byte[] buffer = block.toBuffer();

        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);

        DataBlockHeader header = DataBlockHeader.fromByteBuffer(byteBuffer);
        DataBlock distBlock = DataBlock.parseData(header,byteBuffer);
        assertEquals(distBlock.getValue(), block.getValue());
        assertEquals(distBlock.getValue(), value);
        assertTrue( distBlock.getValue() instanceof  Byte);
    }

    @Test
    public void dataBlockParsingStringValueTest() {
        String value = UUID.randomUUID().toString();
        DataBlock block = DataBlock.newDataBlock(value);
        byte[] buffer = block.toBuffer();

        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);

        DataBlockHeader header = DataBlockHeader.fromByteBuffer(byteBuffer);
        DataBlock distBlock = DataBlock.parseData(header,byteBuffer);
        assertEquals(distBlock.getValue(), block.getValue());
        assertEquals(distBlock.getValue(), value);
        assertTrue( distBlock.getValue() instanceof  String);
    }


    @Test
    public void dataBlockParsingByteArrayValueTest() {
        int len = ThreadLocalRandom.current().nextInt(1000) + 1000;
        byte[] value = new byte[len];
        ThreadLocalRandom.current().nextBytes(value);
        DataBlock block = DataBlock.newDataBlock(value, 0);
        byte[] buffer = block.toBuffer();

        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
        DataBlockHeader header = DataBlockHeader.fromByteBuffer(byteBuffer);

        DataBlock distBlock = DataBlock.parseData(header, byteBuffer);

        assertArrayEquals( (byte[])distBlock.getValue(), (byte[])block.getValue());
        assertArrayEquals((byte[])distBlock.getValue(), value);
        assertEquals( ((byte[])distBlock.getValue()).length, len);
    }

    @Test
    public void dataBlockParsingByteArrayValueForSplitByteBufferTest() {
        int len = ThreadLocalRandom.current().nextInt(200) + 200;
        byte[] value = new byte[len];
        ThreadLocalRandom.current().nextBytes(value);
        DataBlock block = DataBlock.newDataBlock(value, 0);
        byte[] buffer = block.toBuffer();

        int lenA = ThreadLocalRandom.current().nextInt(100) + DataBlockHeader.HEADER_SIZE;
        byte[] bufferA = Arrays.copyOfRange(buffer, 0, lenA);
        byte[] bufferB = Arrays.copyOfRange(buffer, lenA, lenA + 10);
        byte[] bufferC = Arrays.copyOfRange(buffer, lenA + 10, buffer.length);




        ByteBuffer byteBufferA = ByteBuffer.wrap(bufferA);
        ByteBuffer byteBufferB = ByteBuffer.wrap(bufferB);
        ByteBuffer byteBufferC = ByteBuffer.wrap(bufferC);
        DataBlockHeader header = DataBlockHeader.fromByteBuffer(byteBufferA);

        DataBlock distBlock = DataBlock.parseData(header, byteBufferA,byteBufferB,byteBufferC);

        assertArrayEquals( (byte[])distBlock.getValue(), (byte[])block.getValue());
        assertArrayEquals((byte[])distBlock.getValue(), value);
        assertEquals( ((byte[])distBlock.getValue()).length, len);
    }




}