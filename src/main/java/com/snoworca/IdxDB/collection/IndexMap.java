package com.snoworca.IdxDB.collection;

import com.snoworca.IdxDB.OP;
import com.snoworca.IdxDB.dataStore.DataIO;
import com.snoworca.IdxDB.util.CusLinkedHashMap;
import com.snoworca.cson.CSONObject;

import java.io.IOException;
import java.util.*;

public class IndexMap extends IndexCollectionBase {



    private CusLinkedHashMap<IndexValue, CSONItem> itemHashMap_;
    private CusLinkedHashMap<IndexValue,CSONItem> itemHashCacheMap;

    private static final float DEFAULT_LOAD_FACTOR = 0.75f;
    private static final int DEFAULT_INITIAL_CAPACITY = 1 << 4; // aka 16

    private boolean isAccessOrder;
    private boolean isChangeOrder;
    private int memCacheSize;
    private boolean isReverse;

    public IndexMap(DataIO dataIO, IndexMapOption collectionOption) {
        super(dataIO, collectionOption);

    }

    @Override
    protected void onInit(CollectionOption collectionOption) {
        isAccessOrder = ((IndexMapOption)collectionOption).isAccessOrder();
        itemHashMap_ = new CusLinkedHashMap<>(isAccessOrder);
        itemHashCacheMap = new CusLinkedHashMap<>(isAccessOrder);
        memCacheSize = collectionOption.getMemCacheSize();
        isReverse = collectionOption.getIndexSort() < 0;
    }

    @Override
    protected void onRestoreCSONItem(CSONItem csonItem) {
        itemHashMap_.put(IndexValue.newIndexValueItem(csonItem), csonItem);
    }

    private CSONItem get(Object indexValue) {
        CSONItem csonItem = itemHashMap_.get(IndexValue.newIndexValueCache(indexValue));
        if(memCacheSize > 0 && isAccessOrder && csonItem != null) {
           addCache(memCacheSize, csonItem);
        }
        return csonItem;
    }


    private void removeCache(int memCacheSize , CSONItem item) {
        if(memCacheSize <= 0) return;
        itemHashCacheMap.remove(IndexValue.newIndexValueItem(item));
        int cacheMapSize = itemHashCacheMap.size();
        Collection<CSONItem> items = itemHashMap_.values(isReverse);
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
            csonItem.setStore(false);
        }
    }


    private void addCache(int memCacheSize , CSONItem item) {
        if(memCacheSize <= 0) {
            item.setStore(false);
            return;
        }
        itemHashCacheMap.put(IndexValue.newIndexValueItem(item), item);
        if(itemHashCacheMap.size() > memCacheSize) {
            Iterator<Map.Entry<IndexValue,CSONItem>> iterator = itemHashCacheMap.entrySet(!isReverse).iterator();
            Map.Entry<IndexValue, CSONItem> entry = iterator.next();
            entry.getValue().setStore(true);
            iterator.remove();
        }

    }


    @Override
    protected void onMemStore() {
        int memCacheLimit = getMemCacheSize();
        int count = 0, descCacheBegin = itemHashMap_.size() - memCacheLimit;
        boolean asc = getSort() > -1;
        Collection<CSONItem> values = itemHashMap_.values();
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
        ArrayList<CSONItem> allValues = null;
        if(op != OP.eq) {
            allValues = new ArrayList<>(itemHashMap_.values());
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
                    CSONItem csonItem = itemHashMap_.getNoneOrder(IndexValue.newIndexValueCache(indexValue));
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


                ArrayList<CSONItem> arrayList = new ArrayList<>(itemHashMap_.values());
                Collections.reverse(arrayList);
                List<CSONObject> result = (List<CSONObject>) jsonItemCollectionsToJsonObjectCollection(arrayList, limit);
                return result;
            }
            List<CSONObject> result = (List<CSONObject>) jsonItemCollectionsToJsonObjectCollection(itemHashMap_.values(), limit);
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
            return itemHashMap_.size();
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
                            if ((item = itemHashMap_.remove(IndexValue.newIndexValueItem(item))) != null) {
                                commitResult.incrementCountOfRemove();
                                removeCache(memCacheSize, item);
                                unlink(item);
                                item.release();
                            }
                            break;
                        case TransactionOrder.ORDER_CLEAR:
                            for (CSONItem clearItem : itemHashMap_.values()) {
                                unlink(clearItem);
                                clearItem.release();
                                commitResult.incrementCountOfRemove();
                            }
                            itemHashCacheMap.clear();
                            itemHashMap_.clear();
                            break;
                        case TransactionOrder.ORDER_ADD:
                        case TransactionOrder.ORDER_ADD_OR_REPLACE:
                            Object indexValueForAddOrReplace = item.getIndexValue();
                            CSONObject addOrReplaceCsonObject = item.getCsonObject();
                            CSONItem foundItemOfAddOrReplace = itemHashMap_.getAndAccessOrder(IndexValue.newIndexValueCache(indexValueForAddOrReplace));
                            if (foundItemOfAddOrReplace == null) {
                                itemHashMap_.put(IndexValue.newIndexValueItem(item), item);
                                item.storeIfNeed();
                                commitResult.incrementCountOfAdd();
                                foundItemOfAddOrReplace = item;
                            } else {
                                unlink(foundItemOfAddOrReplace);
                                foundItemOfAddOrReplace.setStoragePos(-1);
                                foundItemOfAddOrReplace.setCsonObject(addOrReplaceCsonObject);
                                foundItemOfAddOrReplace.storeIfNeed();
                                commitResult.incrementCountOfReplace();
                            }
                            addCache(memCacheSize, foundItemOfAddOrReplace);
                            break;
                        case TransactionOrder.ORDER_REPLACE:
                            CSONObject replaceCsonObject = item.getCsonObject();
                            Object indexValueForReplace = item.getIndexValue();
                            CSONItem foundItemOfReplace = itemHashMap_.getAndAccessOrder(IndexValue.newIndexValueCache(indexValueForReplace));
                            if (foundItemOfReplace != null) {
                                unlink(foundItemOfReplace);
                                foundItemOfReplace.setStoragePos(-1);
                                foundItemOfReplace.setCsonObject(replaceCsonObject);
                                foundItemOfReplace.storeIfNeed();
                                commitResult.incrementCountOfReplace();
                                addCache(memCacheSize, foundItemOfReplace);
                            }
                            break;
                    }
                }
            } else if(!isChangeOrder) {
                return commitResult;
            }

            isChangeOrder = false;
            //onMemStore();

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
            return itemHashMap_.isEmpty();
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
                ArrayList<CSONItem> list = new ArrayList<>(itemHashMap_.values());
                Collections.reverse(list);
                iterator = list.iterator();
            } else {
                iterator = itemHashMap_.values().iterator();
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
                        itemHashMap_.remove(IndexValue.newIndexValueItem(current));
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
