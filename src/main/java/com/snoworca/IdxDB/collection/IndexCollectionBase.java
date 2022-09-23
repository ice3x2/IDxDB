package com.snoworca.IdxDB.collection;

import com.snoworca.IdxDB.dataStore.DataBlock;
import com.snoworca.IdxDB.dataStore.DataIO;

import java.io.IOException;

public abstract class IndexCollectionBase implements IndexCollection {

    private DataIO dataIO;
    private String name;
    private boolean isFileStore;
    private int memCacheSize;
    private long lastDataStorePos = -1;
    private StoreDelegator storeDelegator;

    IndexCollectionBase(DataIO dataIO, CollectionOption collectionOption) {
        this.dataIO = dataIO;
        this.name = collectionOption.getName();
        this.isFileStore = collectionOption.isFileStore();
        this.memCacheSize = 0;
    }

    protected int getMemCacheSize() {
        return memCacheSize;
    }


    @Override
    public String getName() {
        return name;
    }

    protected StoreDelegator getStoreDelegator() {
        return storeDelegator;
    }

    protected void makeStoreDelegatorImpl() {
        storeDelegator = new StoreDelegator() {
            @Override
            public long cache(byte[] buffer) {
                try {
                    DataBlock dataBlock = dataIO.write(buffer);
                    long dataPos = dataBlock.getPos();
                    dataIO.setNextPos(lastDataStorePos, dataPos);
                    dataIO.setPrevPos(dataPos, lastDataStorePos);
                    lastDataStorePos = dataPos;
                    return dataPos;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public byte[] load(long pos) {
                try {
                    DataBlock dataBlock = dataIO.get(pos);
                    return dataBlock.getData();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }
}
