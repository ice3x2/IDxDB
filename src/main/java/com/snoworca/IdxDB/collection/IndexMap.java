package com.snoworca.IdxDB.collection;

import com.snoworca.IdxDB.dataStore.DataIO;
import com.snoworca.cson.CSONArray;
import com.snoworca.cson.CSONObject;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class IndexMap extends IndexCollectionBase {


    private LinkedHashMap<Object, CSONItem> itemHashMap_ = new LinkedHashMap<>();
    private ArrayList<TransactionOrder> transactionTempList = new ArrayList<>();

    private ReentrantReadWriteLock transactionTempReadWriteLock = new ReentrantReadWriteLock();
    private ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private String indexKey = "";

    private volatile boolean isChanged = false;

    private static final float DEFAULT_LOAD_FACTOR = 0.75f;
    private static final int DEFAULT_INITIAL_CAPACITY = 1 << 4; // aka 16

    IndexMap(DataIO dataIO, IndexMapOption collectionOption) {
        super(dataIO, collectionOption);
        //TODO LinkedHashMap 의 마지막 인자인 accessOrder 를 받게한다.

        itemHashMap_ = new LinkedHashMap<>(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR, collectionOption.isAccessOrder());

    }


    @Override
    public boolean add(CSONObject csonObject) {
        Object indexValue = csonObject.get(indexKey);
        if(indexValue == null) {
            return false;
        }
        ReentrantReadWriteLock.WriteLock lock = transactionTempReadWriteLock.writeLock();
        lock.lock();
        transactionTempList.add(new TransactionOrder(TransactionOrder.ORDER_ADD,  new CSONItem(getStoreDelegator(), csonObject, indexKey, indexValue)));
        lock.unlock();
        return true;
    }

    @Override
    public boolean addAll(CSONArray csonArray) {
        ArrayList<TransactionOrder> cacheList = new ArrayList<>();
        for(int i = 0, n = csonArray.size();i < n; ++i) {
            CSONObject csonObject = csonArray.optObject(i);
            if(csonObject == null) return false;
            Object indexValue = csonObject.opt(indexKey);
            if(indexValue == null) return false;
            cacheList.add(new TransactionOrder(TransactionOrder.ORDER_ADD, new CSONItem(getStoreDelegator(), csonObject, indexKey, indexValue)));
        }
        ReentrantReadWriteLock.WriteLock lock = transactionTempReadWriteLock.writeLock();
        lock.lock();
        transactionTempList.addAll(cacheList);
        lock.unlock();
        return true;
    }

    @Override
    public boolean addOrReplace(CSONObject csonObject) {
        Object indexValue = csonObject.opt(indexKey);
        if(indexValue == null) {
            return false;
        }
        ReentrantReadWriteLock.WriteLock writeLock = transactionTempReadWriteLock.writeLock();
        writeLock.lock();
        transactionTempList.add(new TransactionOrder(TransactionOrder.ORDER_ADD_OR_REPLACE, new CSONItem(getStoreDelegator(),csonObject, indexKey, indexValue)));
        writeLock.unlock();
        return true;
    }

    @Override
    public boolean addOrReplaceAll(CSONArray csonArray) {
        ArrayList<TransactionOrder> cacheList = new ArrayList<>();
        for(int i = 0, n = csonArray.size();i < n; ++i) {
            CSONObject csonObject = csonArray.optObject(i);
            if(csonObject == null) return false;
            Object indexValue = csonObject.opt(indexKey);
            if(indexValue == null) return false;
            cacheList.add(new TransactionOrder(TransactionOrder.ORDER_ADD_OR_REPLACE,new CSONItem(getStoreDelegator(), csonObject, indexKey, indexValue)));
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
        /*ReentrantReadWriteLock.ReadLock readLock = readWriteLock.readLock();
        readLock.lock();
        try {
            return itemHashMap.size();
        } finally {
            readLock.unlock();
        }*/
        return 0;
    }

    @Override
    public void commit() {

    }

    @Override
    public boolean isEmpty() {
        /*ReentrantReadWriteLock.ReadLock readLock = readWriteLock.readLock();
        readLock.lock();
        try {
            return itemHashMap.isEmpty();
        } finally {
            readLock.unlock();
        }*/
        return false;
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
