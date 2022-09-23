package com.snoworca.IdxDB.collection;

public interface StoreDelegator {

        public long cache(byte[] buffer);
        public byte[] load(long pos);

}
