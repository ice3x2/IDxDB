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

    private boolean isAccessOrder;
    private boolean isChangeOrder = false;

    public IndexMap(DataIO dataIO, IndexMapOption collectionOption) {
        super(dataIO, collectionOption);

    }

    @Override
    protected void onInit(CollectionOption collectionOption) {
        isAccessOrder = ((IndexMapOption)collectionOption).isAccessOrder();
        itemHashMap = new LinkedHashMap<>(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR, isAccessOrder);

    }

    @Override
    protected void onRestoreCSONItem(CSONItem csonItem) {

        itemHashMap.put(csonItem.getIndexValue(), csonItem);
    }

    @Override
    protected void onMemStore() {
        int memCacheLimit = getMemCacheSize();
        int count = 0, descCacheBegin = itemHashMap.size() - memCacheLimit;
        boolean asc = getSort() > -1;
        Collection<CSONItem> values = itemHashMap.values();
        for (CSONItem csonItem : values) {
            if (csonItem.getStoragePos() > 0 && csonItem.isChanged()) {
                try {
                    unlink(csonItem);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                csonItem.setStoragePos(-1);
            }
            csonItem.storeIfNeed();
            if(asc) {
                csonItem.setStore(count > memCacheLimit);
            } else {
                csonItem.setStore(count < descCacheBegin);
            }
            ++count;
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
        try {
            switch (op) {
                case eq:
                    CSONItem csonItem = itemHashMap.get(indexValue);
                    if (csonItem != null) result.add(csonItem.getCsonObject());
                    isChangeOrder = isAccessOrder;
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
            if(!transactionOrders.isEmpty()) {
                for (int i = 0, n = transactionOrders.size(); i < n; ++i) {
                    TransactionOrder transactionOrder = transactionOrders.get(i);
                    CSONItem item = transactionOrder.getItem();
                    switch (transactionOrder.getOrder()) {
                        case TransactionOrder.ORDER_REMOVE:
                            Object indexValueOfRemove = item.getIndexValue();
                            if ((item = itemHashMap.remove(indexValueOfRemove)) != null) {
                                commitResult.incrementCountOfRemove();
                                unlink(item);
                                item.release();
                            }
                            break;
                        case TransactionOrder.ORDER_CLEAR:
                            for (CSONItem clearItem : itemHashMap.values()) {
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
                            if (foundItemOfAddOrReplace == null) {
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
                            if (foundItemOfReplace != null) {
                                unlink(foundItemOfReplace);
                                foundItemOfReplace.setStoragePos(-1);
                                foundItemOfReplace.setCsonObject(replaceCsonObject);
                                foundItemOfReplace.storeIfNeed();
                                commitResult.incrementCountOfReplace();
                            }
                            break;
                    }
                }
            } else if(!isChangeOrder) {
                return commitResult;
            }

            isChangeOrder = false;
            onMemStore();

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
                        itemHashMap.remove(current.getIndexValue());
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
}
