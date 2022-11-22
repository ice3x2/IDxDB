package com.snoworca.IdxDB.collection;

import com.snoworca.IdxDB.CompareUtil;
import com.snoworca.IdxDB.OP;
import com.snoworca.IdxDB.store.DataStore;
import com.snoworca.cson.CSONObject;

import java.io.IOException;
import java.util.*;

public class IndexTreeSet extends IndexCollectionBase {

    private TreeSet<CSONItem> itemSet;
    private TreeSet<CSONItem> cacheSet;

    private StoreDelegator storeDelegator;
    private String indexKey;
    private int indexSort;

    private boolean isMemCacheIndex;


    public IndexTreeSet(int id, DataStore dataStore, IndexSetOption option) {
        super(id, dataStore, option);
        this.indexKey = super.getIndexKey();
        this.storeDelegator = super.getStoreDelegator();
        this.indexSort = super.getSort();
        this.isMemCacheIndex = option.isMemCacheIndex();


    }


    @Override
    protected void onInit(CollectionOption collectionOption) {
        itemSet = new TreeSet<>();
        cacheSet = new TreeSet<>();
    }

    @Override
    protected void onRestoreCSONItem(CSONItem csonItem) {
        csonItem.clearCache();
        itemSet.add(csonItem);
    }


    @Override
    protected void onMemStore() {
        writeLock();
        try {
            int count = 0;
            int memCacheLimit = getMemCacheSize();
            memCacheLimit = 320;
            for (CSONItem csonItem : itemSet) {
                boolean isMemCache = count < memCacheLimit;
                if (isMemCache) {
                    csonItem.cache();
                    cacheSet.add(csonItem);
                } else {
                    csonItem.clearCache();
                }
                ++count;
            }
        } finally {
            writeUnlock();
        }
    }

    @Override
    public int size() {
        readLock();
        int size = itemSet.size();
        readUnlock();
        return size;
    }



    private void removeCache(int memCacheSize , CSONItem item) {
        if(memCacheSize <= 0) return;
        cacheSet.remove(CSONItem.createIndexItem(item.getIndexValue(), indexSort));
        item.clearCache();

    }

    private void onCache(int memCacheSize) {
        if(memCacheSize <= 0) return;
        int count = cacheSet.size();
        int csonItemIndex = 0;
        if(count == memCacheSize) {
            return;
        }
        Iterator<CSONItem> itemSetIterator = itemSet.iterator();
        Iterator<CSONItem> cacheIterator = cacheSet.iterator();

        while (itemSetIterator.hasNext()) {
            CSONItem item = itemSetIterator.next();
            CSONItem cacheItem = null;
            if(count >= memCacheSize) {
                break;
            }
            if(csonItemIndex < count) {
                if(cacheIterator.hasNext()) {
                    cacheItem = cacheIterator.next();
                    cacheItem.cache();
                }
                ++csonItemIndex;
                continue;
            }
            item.cache();
            cacheSet.add(item);
            ++count;
        }
        cacheIterator = cacheSet.iterator();
        while (cacheIterator.hasNext()) {
            CSONItem cacheItem = cacheIterator.next();
            cacheItem.cache();
        }
    }


    private CSONItem addCache(int memCacheSize , CSONItem item) {
        if(memCacheSize <= 0) return item;
        cacheSet.add(item);
        if (cacheSet.size() > memCacheSize) {
            CSONItem csonItem = cacheSet.pollLast();
            if(csonItem != null) {
                return csonItem;
            }
        }
        return null;
    }

    protected CSONItem getCSONItem(Object index) {

        CSONItem csonItem = itemSet.floor(CSONItem.createIndexItem(index, indexSort));
        if(csonItem != null && csonItem.compareIndex(index) == 0) {
            return csonItem;
        }
        return null;
    }


    @Override
    public synchronized CommitResult commit() {
        ArrayList<TransactionOrder> transactionOrders = getTransactionOrders();
        ArrayList<CSONItem> clearCacheList = new ArrayList<>();
        ArrayList<CSONItem> writeItemList = new ArrayList<>();
        ArrayList<CSONItem> writeOrReplaceItemList = new ArrayList<>();
        writeLock();
        CommitResult commitResult = new CommitResult();
        if(transactionOrders.isEmpty()) {
            return commitResult;
        }
        int memCacheSize = getMemCacheSize();
        try {
            CSONItem foundItem = null;
            for(int i = 0, n = transactionOrders.size(); i < n; ++i) {
                TransactionOrder transactionOrder = transactionOrders.get(i);
                CSONItem item = transactionOrder.getItem();
                switch (transactionOrder.getOrder()) {
                    case TransactionOrder.ORDER_ADD:
                        if(itemSet.add(item)) {
                            CSONItem clearCacheItem = addCache(memCacheSize, item);
                            if(clearCacheItem != null) {
                                clearCacheList.add(clearCacheItem);
                            }
                            commitResult.incrementCountOfAdd();
                        }
                        writeItemList.add(item);
                        break;
                    case TransactionOrder.ORDER_REMOVE:
                        foundItem = null;
                        foundItem = getCSONItem(item.getIndexValue());
                        if(foundItem != null && itemSet.remove(foundItem)) {
                            removeCache(memCacheSize, item);
                            commitResult.incrementCountOfRemove();
                            unlink(foundItem);
                            foundItem.release();
                        }
                        break;
                    case TransactionOrder.ORDER_CLEAR:
                        for(CSONItem clearItem : itemSet) {
                            unlink(clearItem);
                            clearItem.release();
                            commitResult.incrementCountOfRemove();
                        }
                        cacheSet.clear();
                        itemSet.clear();
                        break;
                    case TransactionOrder.ORDER_ADD_OR_REPLACE:
                        foundItem = null;
                        foundItem = getCSONItem(item.getIndexValue());
                        if(foundItem == null) {
                            itemSet.add(item);
                            CSONItem clearCacheItem = addCache(memCacheSize, item);
                            if(clearCacheItem != null) {
                                clearCacheList.add(clearCacheItem);
                            }
                            commitResult.incrementCountOfAdd();
                        }
                        else {
                            foundItem.setCsonObject(item.getCsonObject());
                            item = foundItem;
                            commitResult.incrementCountOfReplace();
                        }
                        writeOrReplaceItemList.add(item);
                        break;
                    case TransactionOrder.ORDER_REPLACE:
                        foundItem = null;
                        foundItem = getCSONItem(item.getIndexValue());
                        if(foundItem != null) {
                            foundItem.setCsonObject(item.getCsonObject());
                            commitResult.incrementCountOfReplace();
                            writeOrReplaceItemList.add(foundItem);
                        }
                        break;
                }
            }
            store(writeItemList);
            replaceOrStore(writeOrReplaceItemList);
            clearCache(clearCacheList);
            clearCache(writeItemList);
            clearCache(writeOrReplaceItemList);
            onCache(memCacheSize);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            writeUnlock();
        }

        return commitResult;

    }



    @Override
    public boolean isEmpty() {
        readLock();
        try {
            return itemSet.isEmpty();
        } finally {
            readUnlock();
        }
    }

    private CSONItem get_(CSONObject csonObject) {
        String indexKey = getIndexKey();
        CSONItem item = new CSONItem(getStoreDelegator(),csonObject,indexKey, getSort(), isMemCacheIndex );
        CSONItem foundItem = itemSet.floor(item);
        if(foundItem == null || !CompareUtil.compare(foundItem.getIndexValue(), csonObject.opt(indexKey), OP.eq)) {
            return null;
        }
        return foundItem;

    }



    @Override
    public List<CSONObject> findByIndex(Object indexValue, FindOption option, int limit) {
        OP op = option.getOp();
        if(op == null) op = OP.eq;
        ArrayList<CSONObject> result = new ArrayList<>();
        if(limit < 1) {

            return result;
        }
        switch (op) {
            case eq:
                CSONObject csonObject = findByIndex(indexValue);
                if(csonObject != null) result.add(csonObject);
                break;
            case gte:
            case gt:
                if(indexSort > 0) {
                    readLock();
                    result = (ArrayList<CSONObject>)jsonItemCollectionsToJsonObjectCollection(0,itemSet.tailSet(makeIndexItem(indexValue),op == OP.gte), limit );
                    readUnlock();;
                } else {
                    readLock();
                    result = (ArrayList<CSONObject>)jsonItemCollectionsToJsonObjectCollection(0,itemSet.descendingSet().tailSet(makeIndexItem(indexValue), op == OP.gte), limit);
                    readUnlock();
                }
                break;
            case lte:
            case lt:
                if(indexSort > 0) {
                    readLock();
                    result = (ArrayList<CSONObject>)jsonItemCollectionsToJsonObjectCollection(0,itemSet.descendingSet().tailSet(makeIndexItem(indexValue), op == OP.lte), limit);
                    readUnlock();
                } else {
                    readLock();
                    result = (ArrayList<CSONObject>)jsonItemCollectionsToJsonObjectCollection(0,itemSet.tailSet(makeIndexItem(indexValue), op == OP.lte), limit);
                    readUnlock();
                }
                break;
        }
        return result;
    }

    @Override
    public void removeByIndex(Object indexValue, FindOption option) {
        OP op = option.getOp();
        if(op == null) op = OP.eq;
        ArrayList<Object> results = new ArrayList<>();
        switch (op) {
            case eq:
                removeTransactionOrder(new CSONObject().put(indexKey, indexValue));
                break;
            case gte:
            case gt:
                if(indexSort > 0) {
                    writeLock();
                    removeByJSONItems(itemSet.tailSet(makeIndexItem(indexValue), op == OP.gte));
                    writeUnlock();
                } else {
                    writeLock();
                    removeByJSONItems(itemSet.descendingSet().tailSet(makeIndexItem(indexValue), op == OP.gte));
                    writeUnlock();
                }
                break;
            case lte:
            case lt:
                if(indexSort > 0) {
                    writeLock();
                    removeByJSONItems(itemSet.descendingSet().tailSet(makeIndexItem(indexValue), op == OP.lte));
                    writeUnlock();
                } else {
                    writeLock();
                    removeByJSONItems(itemSet.tailSet(makeIndexItem(indexValue), op == OP.lte));
                    writeUnlock();
                }
                break;
        }
    }

    @Override
    public List<CSONObject> list(int limit, boolean reverse) {
        return list(0, limit, reverse);
    }

    @Override
    public List<CSONObject> list(int start, int limit, boolean reverse) {
        try {
            readLock();
            if (reverse) {
                List<CSONObject> result = (List<CSONObject>) jsonItemCollectionsToJsonObjectCollection(start,itemSet.descendingSet(), limit);
                return result;
            }
            List<CSONObject> result = (List<CSONObject>) jsonItemCollectionsToJsonObjectCollection(start,itemSet, limit);
            return result;
        } finally {
            readUnlock();
        }
    }

    private Collection<CSONObject> jsonItemCollectionsToJsonObjectCollection(int start, Collection<CSONItem> CSONItems, int limit) {
        int count = 0;
        int continueIdx = 0;
        ArrayList<CSONObject> csonObjects = new ArrayList<>();
        for(CSONItem item : CSONItems) {
            if(continueIdx < start) {
                continueIdx++;
                continue;
            }
            if(count >= limit) {
                break;
            }
            csonObjects.add(item.getCsonObject());
            ++count;
        }
        return csonObjects;
    }


    private void removeByJSONItems(Collection<CSONItem> CSONItems) {
        ArrayList<Object> removedIndexList = new ArrayList<>();
        for(CSONItem item : CSONItems) {
            removedIndexList.add(item.getIndexValue());
        }
        removeTransactionOrder(CSONItems);
    }


    private CSONItem makeIndexItem(Object index) {
        CSONObject indexJson = new CSONObject().put(indexKey, index);
        CSONItem indexItem = new CSONItem(storeDelegator,indexJson, indexKey, indexSort, isMemCacheIndex);
        return indexItem;
    }

    private CSONObject findByIndex(Object indexValue) {
        readLock();
        CSONItem foundItem = get_(new CSONObject().put(indexKey, indexValue));
        readUnlock();
        return foundItem == null ? null : foundItem.getCsonObject();
    }


    public CSONObject last() {
        try {
            readLock();
            if (itemSet.isEmpty()) return null;
            CSONItem item = itemSet.last();
            CSONObject csonObject = item.getCsonObject();
            return csonObject;
        } finally {
            readUnlock();
        }
    }




    private Collection<?> objectCollectionToJSONItemCollection(Collection<?> c) {
        ArrayList<CSONItem> list = new ArrayList<>();
        for(Object obj : c) {
            if(obj instanceof CSONObject) {
                list.add(new CSONItem(storeDelegator,(CSONObject) obj, indexKey, indexSort, isMemCacheIndex));
            } else {
                list.add(new CSONItem(storeDelegator,new CSONObject().put(indexKey, obj), indexKey, indexSort, isMemCacheIndex));
            }
        }
        return list;
    }

    @Override
    public long findIndexPos(Object indexValue) {
        CSONItem foundItem = get_(new CSONObject().put(indexKey, indexValue));
        return foundItem != null ? foundItem.getStoragePos() : -1;
    }


    @Override
    public Iterator<CSONObject> iterator() {
        readLock();
        Iterator<CSONItem> iterator = null;
        try {
            iterator = itemSet.iterator();
        } finally {
            readUnlock();
        }
        Iterator<CSONItem> finalIterator = iterator;

        return new Iterator<CSONObject>() {
            CSONItem current;
            @Override
            public boolean hasNext() {
                readLock();
                try {
                    boolean hasNext = finalIterator.hasNext();
                    return hasNext;
                } finally {
                    readUnlock();
                }
            }

            @Override
            public CSONObject next() {
                readLock();
                try {
                    current = finalIterator.next();
                    if(current == null) return null;
                    return current.getCsonObject();
                }
                catch (NoSuchElementException e) {
                    throw new NoSuchElementException();
                }
                finally {
                    readUnlock();
                }

            }

            @Override
            public void remove() {
                writeLock();
                try {
                    finalIterator.remove();
                    if (current != null) {
                        cacheSet.remove(current);
                        unlink(current);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } catch (IllegalStateException ea) {
                    throw new IllegalStateException();
                } finally {
                    writeLock();
                }

            }
        };
    }

    @Override
    public void restore(StoredInfo info) {
        writeLock();
        try {
            CSONObject csonObject = info.getCsonObject();
            String indexKey = getIndexKey();
            CSONItem csonItem = new CSONItem(getStoreDelegator(), csonObject.optString(indexKey),indexKey, getSort(), isMemCacheIndex);
            csonItem.setStoreCapacity(info.getCapacity());
            csonItem.setStoragePos_(info.getPosition());
            itemSet.add(csonItem);
        } finally {
            writeUnlock();
        }

    }

    @Override
    public void end() {
        onMemStore();
    }
}
