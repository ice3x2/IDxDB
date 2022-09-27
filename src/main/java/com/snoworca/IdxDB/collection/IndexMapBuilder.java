package com.snoworca.IdxDB.collection;

import com.snoworca.IdxDB.CollectionCreateCallback;
import com.snoworca.IdxDB.dataStore.DataIO;

import java.util.concurrent.locks.ReentrantLock;

public class IndexMapBuilder {

    private IndexMapOption indexMapOption = null;
    private DataIO dataIO;

    private ReentrantLock createLock;
    private CollectionCreateCallback callback;

    public IndexMapBuilder(CollectionCreateCallback callback, DataIO dataIO, String name, ReentrantLock createLock) {
        this.dataIO = dataIO;
        this.createLock = createLock;
        this.callback = callback;
        this.indexMapOption = new IndexMapOption(name);
    }

    IndexMapBuilder(String name, DataIO dataIO) {
        indexMapOption = new IndexMapOption(name);
    }

    public IndexMapBuilder setAccessOrder(boolean enable) {
        indexMapOption.setAccessOrder(enable);
        return this;
    }


    public IndexMapBuilder index(String key, int sort) {
        indexMapOption.setIndex(key, sort);
        return this;
    }


    public IndexMapBuilder memCacheSize(int limit) {
        indexMapOption.setMemCacheSize(limit);
        return this;
    }

    public IndexMap create() {
        IndexMap indexMap = new IndexMap(dataIO, indexMapOption);
        createLock.lock();
        this.callback.onCreate(indexMap);
        createLock.unlock();
        return indexMap;
    }


}