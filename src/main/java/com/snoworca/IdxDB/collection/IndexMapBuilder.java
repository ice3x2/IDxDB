package com.snoworca.IdxDB.collection;

import com.snoworca.IdxDB.CollectionCreateCallback;
import com.snoworca.IdxDB.store.DataStore;

import java.util.concurrent.locks.ReentrantLock;

public class IndexMapBuilder {

    private IndexMapOption indexMapOption = null;

    private ReentrantLock createLock;
    private CollectionCreateCallback callback;

    private DataStore dataStore;

    private int collectionID;

    public IndexMapBuilder(CollectionCreateCallback callback,int id, DataStore dataStore, String name, ReentrantLock createLock) {
        this.dataStore = dataStore;
        this.createLock = createLock;
        this.callback = callback;
        this.indexMapOption = new IndexMapOption(name);
        this.collectionID = id;
    }

    IndexMapBuilder(String name, DataStore dataStore) {
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

    public IndexMapBuilder setMemCacheIndex(boolean enable) {
        indexMapOption.setMemCacheIndex(enable);
        return this;
    }

    public IndexMapBuilder setCapacityRatio(float ratio) {
        indexMapOption.setCapacityRatio(ratio);
        return this;
    }

    public IndexMapBuilder memCacheSize(int limit) {
        indexMapOption.setMemCacheSize(limit);
        return this;
    }

    public IndexLinkedMap create() {
        IndexLinkedMap indexLinkedMap = new IndexLinkedMap(collectionID, dataStore, indexMapOption);
        createLock.lock();
        this.callback.onCreate(indexLinkedMap);
        createLock.unlock();
        return indexLinkedMap;
    }


}
