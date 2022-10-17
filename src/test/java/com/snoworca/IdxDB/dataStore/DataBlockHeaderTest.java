package com.snoworca.IdxDB.dataStore;

import com.snoworca.IdxDB.exception.DataBlockParseException;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DataBlockHeaderTest {

    @Test
    public void dataHeaderParsingTest() {

        DataBlockHeader dataHeader = new DataBlockHeader(DataType.TYPE_DOUBLE, 8, 0);
        byte[] buffer = new byte[DataBlockHeader.HEADER_SIZE];
        dataHeader.writeBuffer(buffer);

        ByteBuffer headerByteBuffer = ByteBuffer.wrap(buffer);

        DataBlockHeader distDataHeader = DataBlockHeader.fromByteBuffer(headerByteBuffer);

        assertEquals(distDataHeader.getLength(), 8);
        assertEquals(distDataHeader.getType(), DataType.TYPE_DOUBLE);
    }

    @Test
    public void dataBlockParseExceptionTest() {

        boolean isFail = false;
        try {
            new DataBlockHeader(DataType.TYPE_DOUBLE, 1230, 0);
        } catch (DataBlockParseException e) {
            e.printStackTrace();
            isFail = true;
        }
        assertTrue(isFail);

        DataBlockHeader dataHeader = new DataBlockHeader(DataType.TYPE_DOUBLE, 8, 0);
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