package com.snoworca.IdxDB.collection;

import com.snoworca.IdxDB.dataStore.DataIO;
import com.snoworca.cson.CSONArray;
import com.snoworca.cson.CSONObject;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class IndexMap extends IndexCollectionBase {


    private LinkedHashMap<Object, CSONItem> itemHashMap = new LinkedHashMap<>();
    private ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private String indexKey = "";

    private volatile boolean isChanged = false;


    IndexMap(DataIO dataIO, CollectionOption collectionOption) {
        super(dataIO, collectionOption);
        //TODO LinkedHashMap 의 마지막 인자인 accessOrder 를 받게한다.

        new LinkedHashMap<>()

    }


    @Override
    public Set<String> indexKeys() {
        return null;
    }

    @Override
    public boolean add(CSONObject csonObject) {
        Object indexValue = csonObject.get(indexKey);
        if(indexValue == null) {
            return false;
        }
        ReentrantReadWriteLock.WriteLock lock = readWriteLock.writeLock();
        lock.lock();
        itemHashMap.put(indexValue, new CSONItem(getStoreDelegator(), csonObject, indexKey, indexValue));
        isChanged = true;
        lock.unlock();
        return true;
    }

    @Override
    public boolean addAll(CSONArray csonArray) {
        LinkedHashMap<Object, CSONItem> cacheMap = new LinkedHashMap<>();
        for(int i = 0, n = csonArray.size();i < n; ++i) {
            CSONObject csonObject = csonArray.optObject(i);
            if(csonObject == null) return false;
            Object indexValue = csonObject.opt(indexKey);
            if(indexValue == null) return false;
            cacheMap.put(indexValue, new CSONItem(getStoreDelegator(), csonObject, indexKey, indexValue));
        }
        ReentrantReadWriteLock.WriteLock lock = readWriteLock.writeLock();
        lock.lock();
        itemHashMap.putAll(cacheMap);
        isChanged = true;
        lock.unlock();
        return true;
    }

    @Override
    public boolean addOrReplace(CSONObject csonObject) {
        Object indexValue = csonObject.opt(indexKey);
        if(indexValue == null) {
            return false;
        }
        ReentrantReadWriteLock.ReadLock readLock = readWriteLock.readLock();
        readLock.lock();
        CSONItem item = itemHashMap.get(indexValue);
        readLock.unlock();
        if(item != null) {
            item.setCsonObject(csonObject);
        } else {
            ReentrantReadWriteLock.WriteLock writeLock = readWriteLock.writeLock();
            writeLock.lock();
            itemHashMap.put(indexValue, new CSONItem(getStoreDelegator(),csonObject, indexKey, indexValue));
            writeLock.unlock();
        }
        return true;
    }

    @Override
    public boolean addOrReplaceAll(CSONArray csonArray) {
        ArrayList<CSONItem> cacheList = new ArrayList<>();
        for(int i = 0, n = csonArray.size();i < n; ++i) {
            CSONObject csonObject = csonArray.optObject(i);
            if(csonObject == null) return false;
            Object indexValue = csonObject.opt(indexKey);
            if(indexValue == null) return false;
            cacheList.add(new CSONItem(getStoreDelegator(), csonObject, indexKey, indexValue));
        }
        for(int i = 0, n = cacheList.size(); i < n; ++i) {
            CSONItem csonItem = cacheList.get(i);
            ReentrantReadWriteLock.ReadLock readLock = readWriteLock.readLock();
            readLock.lock();
            CSONItem item = itemHashMap.get(csonItem.getIndexValue());
            readLock.unlock();
            if(item != null) {
                item.setCsonObject(csonItem.getCsonObject());
            } else {
                ReentrantReadWriteLock.WriteLock writeLock = readWriteLock.writeLock();
                writeLock.lock();
                itemHashMap.put(csonItem.getIndexValue(), csonItem);
                writeLock.unlock();
            }
        }
        return true;
    }

    @Override
    public List<CSONObject> findByIndex(Object start, FindOption options, int limit) {
        return null;
    }

    @Override
    public List<Object> removeByIndex(Object start, FindOption options) {
        return null;
    }

    @Override
    public List<CSONObject> list(int limit, boolean reverse) {
        return null;
    }

    @Override
    public int size() {
        ReentrantReadWriteLock.ReadLock readLock = readWriteLock.readLock();
        readLock.lock();
        try {
            return itemHashMap.size();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void commit() {

    }

    @Override
    public boolean isEmpty() {
        ReentrantReadWriteLock.ReadLock readLock = readWriteLock.readLock();
        readLock.lock();
        try {
            return itemHashMap.isEmpty();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public boolean remove(CSONObject o) {
        return false;
    }

    @Override
    public long getHeadPos() {
        return 0;
    }

    @Override
    public void clear() {

    }

    @Override
    public Iterator<CSONObject> iterator() {
        return null;
    }
}
