package com.snoworca.IdxDB.collection;

import com.snoworca.IdxDB.OP;
import com.snoworca.IdxDB.store.DataStore;
import com.snoworca.IdxDB.util.CusLinkedHashMap;
import com.snoworca.cson.CSONObject;

import java.io.IOException;
import java.util.*;

public class IndexLinkedMap extends IndexCollectionBase {



    private CusLinkedHashMap<IndexValue, CSONItem> itemHashMap;
    private CusLinkedHashMap<IndexValue,CSONItem> itemHashCacheMap;

    private static final float DEFAULT_LOAD_FACTOR = 0.75f;
    private static final int DEFAULT_INITIAL_CAPACITY = 1 << 4; // aka 16

    private boolean isAccessOrder;
    private boolean isChangeOrder;
    private int memCacheSize;
    private boolean isReverse;

    private boolean isMemCacheIndex;

    public IndexLinkedMap(int id, DataStore dataStore,IndexStoreWriter indexStoreWriter,  IndexMapOption collectionOption) {
        super(id, dataStore,indexStoreWriter, collectionOption);
    }

    @Override
    protected void onInit(CollectionOption collectionOption) {
        isAccessOrder = ((IndexMapOption)collectionOption).isAccessOrder();
        itemHashMap = new CusLinkedHashMap<>(isAccessOrder);
        itemHashCacheMap = new CusLinkedHashMap<>(isAccessOrder);
        memCacheSize = collectionOption.getMemCacheSize();
        isReverse = collectionOption.getIndexSort() < 0;
        isMemCacheIndex =  collectionOption.isMemCacheIndex();
    }

    @Override
    protected void onRestoreCSONItem(CSONItem csonItem) {
        itemHashMap.put(IndexValue.newIndexValueItem(csonItem), csonItem);
    }

    private CSONItem get(Object indexValue) {
        CSONItem csonItem = itemHashMap.get(IndexValue.newIndexValueCache(indexValue));
        if(memCacheSize > 0 && isAccessOrder && csonItem != null) {
           addCache(memCacheSize, csonItem);
        }
        return csonItem;
    }

    private void removeCache(CSONItem item) {
        itemHashCacheMap.remove(IndexValue.newIndexValueItem(item));
        item.clearCache();
    }

    private void removeCache(int memCacheSize , CSONItem item) {
        if(memCacheSize <= 0) return;
        itemHashCacheMap.remove(IndexValue.newIndexValueItem(item));
        int cacheMapSize = itemHashCacheMap.size();
        Collection<CSONItem> items = itemHashMap.values(isReverse);
        Iterator<CSONItem> iterator = items.iterator();
        if(itemHashCacheMap.size() >= memCacheSize) {
            return;
        }

        int count = 0;
        while(iterator.hasNext() && count < cacheMapSize) {
            iterator.next();
            count++;
        }
        while(iterator.hasNext()) {
            CSONItem csonItem = iterator.next();
            itemHashCacheMap.put(IndexValue.newIndexValueItem(csonItem), csonItem);
            csonItem.cache();
        }
    }


    private CSONItem addCache(int memCacheSize , CSONItem item) {
        if(memCacheSize <= 0) {
            return item;
        }
        itemHashCacheMap.put(IndexValue.newIndexValueItem(item), item);
        if(itemHashCacheMap.size() > memCacheSize) {
            Iterator<Map.Entry<IndexValue,CSONItem>> iterator = itemHashCacheMap.entrySet(!isReverse).iterator();
            Map.Entry<IndexValue, CSONItem> entry = iterator.next();
            try {
                iterator.remove();
                return entry.getValue();
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
        return null;
    }


    @Override
    protected void onMemStore() {
        writeLock();
        try {
            int memCacheLimit = getMemCacheSize();
            int count = 0, descCacheBegin = itemHashMap.size() - memCacheLimit;
            boolean asc = getSort() > -1;
            Collection<CSONItem> values = itemHashMap.values();
            for (CSONItem csonItem : values) {
                if (asc) {
                    if (count > memCacheLimit) csonItem.clearCache();
                    else csonItem.cache();
                    //csonItem.setStore(count > memCacheLimit);
                } else {
                    if (count < descCacheBegin) csonItem.clearCache();
                    else csonItem.cache();
                    //csonItem.setStore(count < descCacheBegin);
                }
                ++count;
            }
        } finally {
            writeUnlock();
        }
    }

    @Override
    public List<CSONObject> findByIndex(Object indexValue, FindOption option, int limit) {
        OP op = option.getOp();
        if(op == null) op = OP.eq;
        ArrayList<CSONObject> result = new ArrayList<>();
        if(limit < 1) {
            return result;
        }
        readLock();
        ArrayList<CSONItem> allValues = null;
        if(op != OP.eq) {
            allValues = new ArrayList<>(itemHashMap.values());
            if(getSort() < 0) {
                Collections.reverse(allValues);
            }
        }


        try {
            switch (op) {
                case eq:
                    CSONItem csonItem = get(indexValue);
                    if (csonItem != null) result.add(csonItem.getCsonObject());
                    isChangeOrder = isAccessOrder;
                    break;
                default:
                    result = search(allValues, indexValue, op, limit);
            }
        } finally {
            readUnlock();
        }
        return result;
    }

    private ArrayList<CSONObject> search(ArrayList<CSONItem> allValues, Object indexValue, OP op, int limit) {
        ArrayList<CSONObject> result = new ArrayList<>();
        int count = 0;
        int sort = getSort();
        for(int i = 0, n = allValues.size(); i < n; ++i) {
            CSONItem item = allValues.get(i);
            int compare = item.compareIndex(indexValue) * sort;
            if(compare >= 0 && OP.gte == op) {
                result.add(item.getCsonObject());
                ++count;
            }
            else if(compare > 0 && OP.gt == op) {
                result.add(item.getCsonObject());
                ++count;
            }
            else if(compare <= 0 && OP.lte == op) {
                result.add(item.getCsonObject());
                ++count;
            }
            else if(compare < 0 && OP.lt == op) {
                result.add(item.getCsonObject());
                ++count;
            }
            else if(compare != 0 && OP.ne == op) {
                result.add(item.getCsonObject());
                ++count;
            }
            if(count == limit) {
                break;
            }
        }
        return result;

    }


    @Override
    public void removeByIndex(Object indexValue, FindOption option) {
        OP op = option.getOp();
        if(op == null) op = OP.eq;
        ArrayList<CSONObject> result = new ArrayList<>();
        readLock();
        try {
            switch (op) {
                case eq:
                    CSONItem csonItem = itemHashMap.getNoneOrder(IndexValue.newIndexValueCache(indexValue));
                    if (csonItem != null) removeTransactionOrder(csonItem.getCsonObject());
                    break;
            }
        } finally {
            readUnlock();
        }
    }

    @Override
    public List<CSONObject> list(int limit, boolean reverse) {
        return list(0, limit, reverse);

    }

    @Override
    public List<CSONObject> list(int start, int limit, boolean reverse) {
        boolean asc = getSort() > -1;
        try {
            readLock();
            if ((reverse && asc) || (!reverse && !asc) ) {


                ArrayList<CSONItem> arrayList = new ArrayList<>(itemHashMap.values());
                Collections.reverse(arrayList);
                List<CSONObject> result = (List<CSONObject>) jsonItemCollectionsToJsonObjectCollection(start,arrayList, limit);
                return result;
            }
            List<CSONObject> result = (List<CSONObject>) jsonItemCollectionsToJsonObjectCollection(start, itemHashMap.values(), limit);
            return result;
        } finally {
            readUnlock();
        }
    }

    private Collection<CSONObject> jsonItemCollectionsToJsonObjectCollection(int start,Collection<CSONItem> CSONItems, int limit) {
        int count = 0;
        ArrayList<CSONObject> csonObjects = new ArrayList<>();
        if(CSONItems instanceof ArrayList) {
            for(int i = 0, n = CSONItems.size(); i < n; ++i) {
                if(i < start) {
                    continue;
                }
                CSONItem item = ((ArrayList<CSONItem>) CSONItems).get(i);
                if(count >= limit) {
                    break;
                }
                csonObjects.add(item.getCsonObject());
                ++count;
            }
        } else {
            int continueIdx = 0;
            for(CSONItem item : CSONItems) {
                if(continueIdx < start) {
                    ++continueIdx;
                    continue;
                }
                if(count >= limit) {
                    break;
                }
                csonObjects.add(item.getCsonObject());
                ++count;
            }
        }

        return csonObjects;
    }

    private void onCache(int memCacheSize) {
        if(memCacheSize <= 0) return;
        int count = itemHashCacheMap.size();
        int csonItemIndex = 0;
        Iterator<Map.Entry<IndexValue,CSONItem>> itemSetIterator = itemHashMap.entrySet(isReverse).iterator();
        Iterator<Map.Entry<IndexValue,CSONItem>> cacheIterator = itemHashMap.entrySet(isReverse).iterator();

        while (itemSetIterator.hasNext()) {
            Map.Entry<IndexValue,CSONItem> itemEntry = itemSetIterator.next();
            Map.Entry<IndexValue,CSONItem> cacheItemEntry = null;
            if(count >= memCacheSize) {
                break;
            }
            if(csonItemIndex < count) {
                if(cacheIterator.hasNext()) {
                    cacheItemEntry = cacheIterator.next();
                    cacheItemEntry.getValue().cache();
                }
                ++csonItemIndex;
                continue;
            }
            CSONItem csonItem = itemEntry.getValue();
            csonItem.cache();

            itemHashCacheMap.put(IndexValue.newIndexValueItem(csonItem),csonItem);
            ++count;
        }
        cacheIterator = itemHashMap.entrySet(isReverse).iterator();
        while (cacheIterator.hasNext()) {
            Map.Entry<IndexValue,CSONItem> cacheItemEntry = cacheIterator.next();
            cacheItemEntry.getValue().cache();
        }

    }


    @Override
    public int size() {
        readLock();
        try {
            return itemHashMap.size();
        } finally {
            readUnlock();
        }
    }

    @Override
    public CommitResult commit() {

        ArrayList<TransactionOrder> transactionOrders = getTransactionOrders();
        ArrayList<CSONItem> clearCacheList = new ArrayList<>();
        ArrayList<CSONItem> writeItemList = new ArrayList<>();
        ArrayList<CSONItem> writeOrReplaceItemList = new ArrayList<>();
        writeLock();
        CommitResult commitResult = new CommitResult();
        try {
            if(!transactionOrders.isEmpty()) {
                CSONItem foundItem = null;
                for (int i = 0, n = transactionOrders.size(); i < n; ++i) {
                    TransactionOrder transactionOrder = transactionOrders.get(i);
                    CSONItem item = transactionOrder.getItem();
                    switch (transactionOrder.getOrder()) {
                        case TransactionOrder.ORDER_REMOVE:
                            if ((item = itemHashMap.remove(IndexValue.newIndexValueItem(item))) != null) {
                                removeCache(item);
                                unlink(item);
                                item.release();
                                commitResult.incrementCountOfRemove();
                            }
                            break;
                        case TransactionOrder.ORDER_CLEAR:
                            for (CSONItem clearItem : itemHashMap.values()) {
                                removeCache(clearItem);
                                unlink(clearItem);
                                clearItem.release();
                                commitResult.incrementCountOfRemove();
                            }
                            itemHashCacheMap.clear();
                            itemHashMap.clear();
                            break;
                        case TransactionOrder.ORDER_ADD:
                            itemHashMap.put(IndexValue.newIndexValueItem(item), item);
                            CSONItem clearCacheItem = addCache(memCacheSize, item);
                            if(clearCacheItem != null) {
                                clearCacheList.add(clearCacheItem);
                            }
                            commitResult.incrementCountOfAdd();
                            writeItemList.add(item);
                            break;
                        case TransactionOrder.ORDER_ADD_OR_REPLACE:
                            CSONObject replaceCsonObject = item.getCsonObject();
                            Object indexValueForReplace = item.getIndexValue();
                            foundItem = null;
                            foundItem = itemHashMap.getAndAccessOrder(IndexValue.newIndexValueCache(indexValueForReplace));
                            if (foundItem == null) {
                                itemHashMap.put(IndexValue.newIndexValueItem(item), item);
                                clearCacheItem = addCache(memCacheSize, item);
                                if(clearCacheItem != null) {
                                    clearCacheList.add(clearCacheItem);
                                }
                                commitResult.incrementCountOfAdd();
                            } else {
                                foundItem.setCsonObject(replaceCsonObject);
                                item = foundItem;
                                commitResult.incrementCountOfReplace();
                            }
                            writeOrReplaceItemList.add(item);
                            break;
                        case TransactionOrder.ORDER_REPLACE:
                            replaceCsonObject = item.getCsonObject();
                            indexValueForReplace = item.getIndexValue();
                            foundItem = null;
                            foundItem = itemHashMap.getAndAccessOrder(IndexValue.newIndexValueCache(indexValueForReplace));
                            if (foundItem != null) {
                                foundItem.replace(replaceCsonObject);
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
            } else if(!isChangeOrder) {
                return commitResult;
            }
            isChangeOrder = false;
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
            return itemHashMap.isEmpty();
        } finally {
            readUnlock();
        }
    }

    @Override
    public long findIndexPos(Object indexValue) {
        CSONItem item = itemHashMap.get(IndexValue.newIndexValueCache(indexValue));
        return item != null ? item.getStoragePos() : -1;
    }


    @Override
    public Iterator<CSONObject> iterator() {
        readLock();
        Iterator<CSONItem> iterator = null;
        boolean isAsc = this.getSort() > -1;
        try {
            if(!isAsc) {
                ArrayList<CSONItem> list = new ArrayList<>(itemHashMap.values());
                Collections.reverse(list);
                iterator = list.iterator();
            } else {
                iterator = itemHashMap.values().iterator();
            }
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
                    if(!isAsc) {
                        itemHashMap.remove(IndexValue.newIndexValueItem(current));
                    }
                    finalIterator.remove();
                    if (current != null) {
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
            CSONItem csonItem = new CSONItem(getStoreDelegator(), csonObject, getIndexKey(), getSort(), isMemCacheIndex);
            csonItem.setStoreCapacity(info.getCapacity());
            csonItem.setStoragePos_(info.getPosition());
            itemHashMap.put(IndexValue.newIndexValueItem(csonItem), csonItem);
        } finally {
            writeUnlock();
        }
    }

    @Override
    public void end() {
        onMemStore();
    }
}
