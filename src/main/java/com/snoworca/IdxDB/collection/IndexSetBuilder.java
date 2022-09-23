package com.snoworca.IdxDB.collection;

import com.snoworca.IdxDB.CollectionCreateCallback;
import com.snoworca.IdxDB.dataStore.DataIO;

import java.util.concurrent.locks.ReentrantLock;

public class IndexSetBuilder {

    private IndexSetOption indexSetOption = null;
    private DataIO dataIO;

    private ReentrantLock createLock;
    private CollectionCreateCallback callback;

    public IndexSetBuilder(CollectionCreateCallback callback, DataIO dataIO, String name, ReentrantLock createLock) {
        this.dataIO = dataIO;
        this.createLock = createLock;
        this.callback = callback;
        this.indexSetOption = new IndexSetOption(name);
    }


    IndexSetBuilder(String name, DataIO dataIO) {
        indexSetOption = new IndexSetOption(name);
    }


    public IndexSetBuilder setFileStore(boolean enable) {
        indexSetOption.setFileStore(enable);
        return this;
    }


    public IndexSetBuilder index(String key, int sort) {
        indexSetOption.setIndex(key, sort);
        return this;
    }


    public IndexSetBuilder memCacheSize(int limit) {
        indexSetOption.setMemCacheSize(limit);
        return this;
    }

    public IndexSet create() {
        IndexSet indexSet = new IndexSet(dataIO, indexSetOption);
        createLock.lock();
        this.callback.onCreate(indexSet);
        createLock.unlock();
        return indexSet;
    }

}
