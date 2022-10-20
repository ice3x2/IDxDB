package com.snoworca.IdxDB.collection;

import com.snoworca.IdxDB.CollectionCreateCallback;
import com.snoworca.IdxDB.CompressionType;
import com.snoworca.IdxDB.dataStore.DataIO;
import com.snoworca.IdxDB.store.DataStore;

import java.util.concurrent.locks.ReentrantLock;

public class IndexSetBuilder {

    private IndexSetOption indexSetOption = null;
    private DataStore dataStore;

    private ReentrantLock createLock;
    private CollectionCreateCallback callback;

    private int collectionID;

    public IndexSetBuilder(CollectionCreateCallback callback,int id, DataStore dataStore, String name, ReentrantLock createLock) {
        this.dataStore = dataStore;
        this.createLock = createLock;
        this.callback = callback;
        this.indexSetOption = new IndexSetOption(name);
        this.collectionID =id;
    }




    IndexSetBuilder(String name, DataStore dataStore) {
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


    public IndexSetBuilder setMemCacheIndex(boolean enable) {
        indexSetOption.setMemCacheIndex(enable);
        return this;
    }


    public IndexSetBuilder setCapacityRatio(float ratio) {
        indexSetOption.setCapacityRatio(ratio);
        return this;
    }




    public IndexTreeSet create() {
        IndexTreeSet indexTreeSet = new IndexTreeSet(collectionID, dataStore, indexSetOption);
        createLock.lock();
        this.callback.onCreate(indexTreeSet);
        createLock.unlock();
        return indexTreeSet;
    }

}
