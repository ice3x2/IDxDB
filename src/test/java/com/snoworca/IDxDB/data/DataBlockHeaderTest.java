package com.snoworca.IDxDB.data;

import com.snoworca.IDxDB.exception.DataBlockParseException;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DataBlockHeaderTest {

    @Test
    public void dataHeaderParsingTest() {
        DataBlockHeader.setTopID(123123132L);
        DataBlockHeader dataHeader = new DataBlockHeader(DataType.TYPE_DOUBLE, 8);
        byte[] buffer = new byte[DataBlockHeader.HEADER_SIZE];
        dataHeader.writeBuffer(buffer);

        ByteBuffer headerByteBuffer = ByteBuffer.wrap(buffer);

        DataBlockHeader distDataHeader = DataBlockHeader.fromByteBuffer(headerByteBuffer);

        assertEquals(distDataHeader.getID(), dataHeader.getID());
        assertEquals(distDataHeader.getLength(), 8);
        assertEquals(distDataHeader.getType(), DataType.TYPE_DOUBLE);
    }

    @Test
    public void dataBlockParseExceptionTest() {
        DataBlockHeader.setTopID(45656792L);
        boolean isFail = false;
        try {
            new DataBlockHeader(DataType.TYPE_DOUBLE, 1230);
        } catch (DataBlockParseException e) {
            e.printStackTrace();
            isFail = true;
        }
        assertTrue(isFail);

        DataBlockHeader dataHeader = new DataBlockHeader(DataType.TYPE_DOUBLE, 8);
        byte[] buffer = new byte[DataBlockHeader.HEADER_SIZE];
        dataHeader.writeBuffer(buffer);
        buffer[DataBlockHeader.HEADER_IDX_TYPE] = DataType.TYPE_BYTE;

        ByteBuffer headerByteBuffer = ByteBuffer.wrap(buffer);

        try {
            DataBlockHeader distDataHeader = DataBlockHeader.fromByteBuffer(headerByteBuffer);
        } catch (DataBlockParseException e) {
            e.printStackTrace();
            return;
        }

        assertTrue(false);

    }



}