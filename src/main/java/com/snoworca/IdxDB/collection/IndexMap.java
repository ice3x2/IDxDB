package com.snoworca.IdxDB.collection;

import com.snoworca.IdxDB.OP;
import com.snoworca.IdxDB.dataStore.DataIO;
import com.snoworca.cson.CSONObject;

import java.io.IOException;
import java.util.*;

public class IndexMap extends IndexCollectionBase {


    private LinkedHashMap<Object, CSONItem> itemHashMap;

    private static final float DEFAULT_LOAD_FACTOR = 0.75f;
    private static final int DEFAULT_INITIAL_CAPACITY = 1 << 4; // aka 16


    public IndexMap(DataIO dataIO, IndexMapOption collectionOption) {
        super(dataIO, collectionOption);
        //TODO LinkedHashMap 의 마지막 인자인 accessOrder 를 받게한다.

        itemHashMap = new LinkedHashMap<>(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR, collectionOption.isAccessOrder());

    }

    @Override
    protected void onInit() {
        itemHashMap = new LinkedHashMap<>();
    }

    @Override
    protected void onRestoreCSONItem(CSONItem csonItem) {
        itemHashMap.put(csonItem.getIndexValue(), csonItem);
    }

    @Override
    protected Iterator<CSONItem> getCSONItemIterator() {
        return itemHashMap.values().iterator();
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
        try {
            switch (op) {
                case eq:
                    CSONItem csonItem = itemHashMap.get(indexValue);
                    if (csonItem != null) result.add(csonItem.getCsonObject());
                    break;
            }
        } finally {
            readUnlock();
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
                    CSONItem csonItem = itemHashMap.get(indexValue);
                    if (csonItem != null) removeTransactionOrder(csonItem.getCsonObject());
                    break;
            }
        } finally {
            readUnlock();
        }
    }

    @Override
    public List<CSONObject> list(int limit, boolean reverse) {
        boolean asc = getSort() > -1;
        try {
            readLock();
            if ((reverse && asc) || (!reverse && !asc) ) {
                ArrayList<CSONItem> arrayList = new ArrayList<>(itemHashMap.values());
                Collections.reverse(arrayList);
                List<CSONObject> result = (List<CSONObject>) jsonItemCollectionsToJsonObjectCollection(arrayList, limit);
                return result;
            }
            List<CSONObject> result = (List<CSONObject>) jsonItemCollectionsToJsonObjectCollection(itemHashMap.values(), limit);
            return result;
        } finally {
            readUnlock();
        }

    }

    private Collection<CSONObject> jsonItemCollectionsToJsonObjectCollection(Collection<CSONItem> CSONItems, int limit) {
        int count = 0;
        ArrayList<CSONObject> csonObjects = new ArrayList<>();
        if(CSONItems instanceof ArrayList) {
            for(int i = 0, n = CSONItems.size(); i < n; ++i) {
                CSONItem item = ((ArrayList<CSONItem>) CSONItems).get(i);
                if(count >= limit) {
                    break;
                }
                csonObjects.add(item.getCsonObject());
                ++count;
            }
        } else {
            for(CSONItem item : CSONItems) {
                if(count >= limit) {
                    break;
                }
                csonObjects.add(item.getCsonObject());
                ++count;
            }
        }

        return csonObjects;
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
        writeLock();
        CommitResult commitResult = new CommitResult();
        try {
            if(transactionOrders.isEmpty()) {
                return commitResult;
            }
            for(int i = 0, n = transactionOrders.size(); i < n; ++i) {
                TransactionOrder transactionOrder = transactionOrders.get(i);
                CSONItem item = transactionOrder.getItem();
                switch (transactionOrder.getOrder()) {
                    case TransactionOrder.ORDER_REMOVE:
                        Object indexValueOfRemove = item.getIndexValue();
                        if((item = itemHashMap.remove(indexValueOfRemove)) != null) {
                            commitResult.incrementCountOfRemove();
                            unlink(item);
                            item.release();
                        }
                        break;
                    case TransactionOrder.ORDER_CLEAR:
                        for(CSONItem clearItem : itemHashMap.values()) {
                            unlink(clearItem);
                            clearItem.release();
                            commitResult.incrementCountOfRemove();
                        }
                        itemHashMap.clear();
                        break;
                    case TransactionOrder.ORDER_ADD:
                    case TransactionOrder.ORDER_ADD_OR_REPLACE:
                        CSONObject addOrReplaceCsonObject = item.getCsonObject();
                        Object indexValueForAddOrReplace = item.getIndexValue();
                        CSONItem foundItemOfAddOrReplace = itemHashMap.get(indexValueForAddOrReplace);
                        if(foundItemOfAddOrReplace == null) {
                            itemHashMap.put(indexValueForAddOrReplace, item);
                            item.storeIfNeed();
                            commitResult.incrementCountOfAdd();
                        } else {
                            unlink(foundItemOfAddOrReplace);
                            foundItemOfAddOrReplace.setStoragePos(-1);
                            foundItemOfAddOrReplace.setCsonObject(addOrReplaceCsonObject);
                            foundItemOfAddOrReplace.storeIfNeed();
                            commitResult.incrementCountOfReplace();
                        }
                        break;
                    case TransactionOrder.ORDER_REPLACE:
                        CSONObject replaceCsonObject = item.getCsonObject();
                        Object indexValueForReplace = item.getIndexValue();
                        CSONItem foundItemOfReplace = itemHashMap.get(indexValueForReplace);
                        if(foundItemOfReplace != null) {
                            unlink(foundItemOfReplace);
                            foundItemOfReplace.setStoragePos(-1);
                            foundItemOfReplace.setCsonObject(replaceCsonObject);
                            foundItemOfReplace.storeIfNeed();
                            commitResult.incrementCountOfReplace();
                        }
                        break;
                }
            }
            int count = 0;
            int memCacheLimit = getMemCacheSize();
            for (CSONItem csonItem : itemHashMap.values()) {
                if (csonItem.getStoragePos() > 0 && csonItem.isChanged()) {
                    try {
                        unlink(csonItem);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    csonItem.setStoragePos(-1);
                }
                csonItem.storeIfNeed();
                csonItem.setStore(count > memCacheLimit);
                ++count;
            }

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
    public Iterator<CSONObject> iterator() {
        readLock();
        Iterator<CSONItem> iterator = null;
        try {
            iterator = itemHashMap.values().iterator();
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
}
