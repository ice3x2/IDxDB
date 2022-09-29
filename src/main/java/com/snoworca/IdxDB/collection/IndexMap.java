package com.snoworca.IdxDB.collection;

import com.snoworca.IdxDB.OP;
import com.snoworca.IdxDB.dataStore.DataIO;
import com.snoworca.cson.CSONObject;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;

public class IndexMap extends IndexCollectionBase {



    private AccessOrderCtrlLinkedHashMap<Object, CSONItem> itemHashMap;
    private AccessOrderCtrlLinkedHashMap<Object, CSONItem> itemHashCacheMap;

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
        itemHashMap = new AccessOrderCtrlLinkedHashMap<>(isAccessOrder);
        itemHashCacheMap = new AccessOrderCtrlLinkedHashMap<>(isAccessOrder);
    }

    @Override
    protected void onRestoreCSONItem(CSONItem csonItem) {

        itemHashMap.put(csonItem.getIndexValue(), csonItem);
    }

    private CSONItem get(Object indexValue) {
        CSONItem csonItem = itemHashMap_.get(indexValue);

        return csonItem;
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
                    CSONItem csonItem = itemHashMap_.get(indexValue);
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


    private class AccessOrderCtrlLinkedHashMap<K,E> extends LinkedHashMap<K,E> {
        AccessOrderCtrlLinkedHashMap(boolean accessOrder) {
            super(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR, accessOrder);
        }

        public E getNoneOrder(Object key) {
            boolean defaultAccessOrder = isAccessOrder;
            isAccessOrder = false;
            E e = super.get(key);
            isAccessOrder = defaultAccessOrder;
            return e;
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
