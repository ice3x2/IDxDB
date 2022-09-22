package com.snoworca.IdxDB.collection;

public interface FileCacheDelegator {

        public long cache(byte[] buffer);
        public byte[] load(long pos);

}
