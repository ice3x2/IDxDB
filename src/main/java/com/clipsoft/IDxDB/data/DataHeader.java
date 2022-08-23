package com.clipsoft.IDxDB.data;

import java.util.concurrent.atomic.AtomicLong;

public class DataHeader {
    private final static AtomicLong TOP_ID = new AtomicLong(Long.MIN_VALUE);
    private long ID;
    private byte length;
    private byte type = DataType.TYPE_NULL;
}

