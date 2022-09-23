package com.snoworca.IdxDB.collection;

import com.snoworca.IdxDB.CollectionCreateCallback;
import com.snoworca.IdxDB.dataStore.DataIO;

import java.util.Observable;
import java.util.concurrent.locks.ReentrantLock;

public class IndexTreeBuilder {

    private IndexTreeOption indexTreeOption = null;
    private DataIO dataIO;

    private ReentrantLock createLock;
    private CollectionCreateCallback callback;

    public IndexTreeBuilder(CollectionCreateCallback callback, DataIO dataIO, String name, ReentrantLock createLock) {
        this.dataIO = dataIO;
        this.createLock = createLock;
        this.callback = callback;
        this.indexTreeOption = new IndexTreeOption(name);
    }


    IndexTreeBuilder(String name, DataIO dataIO) {
        indexTreeOption = new IndexTreeOption(name);
    }


    public IndexTreeBuilder setFileStore(boolean enable) {
        indexTreeOption.setFileStore(enable);
        return this;
    }


    public IndexTreeBuilder index(String key, int sort) {
        indexTreeOption.setIndex(key, sort);
        return this;
    }


    public IndexTreeBuilder memCacheSize(int limit) {
        indexTreeOption.setMemCacheSize(limit);
        return this;
    }

    public IndexTree create() {
        IndexTree indexTree = new IndexTree(dataIO, indexTreeOption);
        createLock.lock();
        this.callback.onCreate(indexTree);
        createLock.unlock();
        return indexTree;
    }

}
