package com.snoworca.IdxDB.collection;

import com.snoworca.IdxDB.CompareUtil;
import com.snoworca.IdxDB.OP;
import com.snoworca.IdxDB.dataStore.DataIO;
import com.snoworca.cson.CSONArray;
import com.snoworca.cson.CSONObject;

import java.io.IOException;
import java.util.*;

public class IndexSet extends IndexCollectionBase {

    private TreeSet<CSONItem> itemSet;

    private StoreDelegator storeDelegator;
    private String indexKey;
    private int indexSort;




    public IndexSet(DataIO dataIO, IndexSetOption option) {
        super(dataIO, option);
        this.indexKey = super.getIndexKey();
        this.storeDelegator = super.getStoreDelegator();
        this.indexSort = super.getSort();


    }


    @Override
    protected void onInit(CollectionOption collectionOption) {
        itemSet = new TreeSet<>();
    }

    @Override
    protected void onRestoreCSONItem(CSONItem csonItem) {
        itemSet.add(csonItem);
    }


    @Override
    protected void onMemStore() {
        int count = 0;
        int memCacheLimit = getMemCacheSize();
        for (CSONItem csonItem : itemSet) {
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
    }

    @Override
    public int size() {
        readLock();
        int size = itemSet.size();
        readUnlock();
        return size;
    }



    @Override
    public synchronized CommitResult commit() {

        ArrayList<TransactionOrder> transactionOrders = getTransactionOrders();
        writeLock();
        CommitResult commitResult = new CommitResult();
        try {
            if(transactionOrders.isEmpty()) {
                return commitResult;
            }
            boolean indexChanged = false;
            for(int i = 0, n = transactionOrders.size(); i < n; ++i) {
                TransactionOrder transactionOrder = transactionOrders.get(i);
                CSONItem item = transactionOrder.getItem();
                switch (transactionOrder.getOrder()) {
                    case TransactionOrder.ORDER_ADD:
                        if(itemSet.add(item)) commitResult.incrementCountOfAdd();
                        item.storeIfNeed();
                        indexChanged = true;
                    break;
                    case TransactionOrder.ORDER_REMOVE:
                        if(item.getStoragePos() < 0) {
                            CSONItem realItem = get_(item.getCsonObject());
                            if(realItem != null) item = realItem;
                        }
                        if(itemSet.remove(item)) {
                            commitResult.incrementCountOfRemove();
                        }
                        unlink(item);
                        item.release();
                        indexChanged = true;
                    break;
                    case TransactionOrder.ORDER_CLEAR:
                        for(CSONItem clearItem : itemSet) {
                            unlink(clearItem);
                            clearItem.release();
                            commitResult.incrementCountOfRemove();
                        }
                        itemSet.clear();
                    break;
                    case TransactionOrder.ORDER_ADD_OR_REPLACE:
                        CSONObject addOrReplaceCsonObject = item.getCsonObject();
                        CSONItem foundItemOfAddOrReplace = get_(addOrReplaceCsonObject);
                        if(foundItemOfAddOrReplace == null) {
                            itemSet.add(item);
                            item.storeIfNeed();
                            commitResult.incrementCountOfAdd();
                        } else {
                            if(foundItemOfAddOrReplace.getStoragePos() < 0) {
                                CSONItem foundItemOfAddOrReplaceReal = get_(foundItemOfAddOrReplace.getCsonObject());
                                if(foundItemOfAddOrReplaceReal != null) {
                                    foundItemOfAddOrReplace = foundItemOfAddOrReplaceReal;
                                }
                            }
                            unlink(foundItemOfAddOrReplace);
                            foundItemOfAddOrReplace.setStoragePos(-1);
                            foundItemOfAddOrReplace.setCsonObject(addOrReplaceCsonObject);
                            foundItemOfAddOrReplace.storeIfNeed();
                            commitResult.incrementCountOfReplace();
                        }

                    break;
                    case TransactionOrder.ORDER_REPLACE:
                        CSONObject replaceCsonObject = item.getCsonObject();
                        CSONItem foundItemOfReplace = get_(replaceCsonObject);
                        if(foundItemOfReplace != null) {
                            if(foundItemOfReplace.getStoragePos() < 0) {
                                CSONItem foundItemOfReplaceReal = get_(foundItemOfReplace.getCsonObject());
                                if(foundItemOfReplaceReal != null) {
                                    foundItemOfReplace = foundItemOfReplaceReal;
                                }
                            }
                            unlink(foundItemOfReplace);
                            foundItemOfReplace.setStoragePos(-1);
                            foundItemOfReplace.setCsonObject(replaceCsonObject);
                            foundItemOfReplace.storeIfNeed();
                            commitResult.incrementCountOfReplace();
                        }
                    break;
                }
            }

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
            return itemSet.isEmpty();
        } finally {
            readUnlock();
        }
    }

    private CSONItem get_(CSONObject csonObject) {
        String indexKey = getIndexKey();
        CSONItem item = new CSONItem(getStoreDelegator(),csonObject,indexKey, getSort());
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
                    result = (ArrayList<CSONObject>)jsonItemCollectionsToJsonObjectCollection(itemSet.tailSet(makeIndexItem(indexValue),op == OP.gte), limit );
                    readUnlock();;
                } else {
                    readLock();
                    result = (ArrayList<CSONObject>)jsonItemCollectionsToJsonObjectCollection(itemSet.descendingSet().tailSet(makeIndexItem(indexValue), op == OP.gte), limit);
                    readUnlock();
                }
                break;
            case lte:
            case lt:
                if(indexSort > 0) {
                    readLock();
                    result = (ArrayList<CSONObject>)jsonItemCollectionsToJsonObjectCollection(itemSet.descendingSet().tailSet(makeIndexItem(indexValue), op == OP.lte), limit);
                    readUnlock();
                } else {
                    readLock();
                    result = (ArrayList<CSONObject>)jsonItemCollectionsToJsonObjectCollection(itemSet.tailSet(makeIndexItem(indexValue), op == OP.lte), limit);
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
        try {
            readLock();
            if (reverse) {
                List<CSONObject> result = (List<CSONObject>) jsonItemCollectionsToJsonObjectCollection(itemSet.descendingSet(), limit);
                return result;
            }
            List<CSONObject> result = (List<CSONObject>) jsonItemCollectionsToJsonObjectCollection(itemSet, limit);
            return result;
        } finally {
            readUnlock();
        }

    }

    private Collection<CSONObject> jsonItemCollectionsToJsonObjectCollection(Collection<CSONItem> CSONItems, int limit) {
        int count = 0;
        ArrayList<CSONObject> csonObjects = new ArrayList<>();
        for(CSONItem item : CSONItems) {
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
        CSONItem indexItem = new CSONItem(storeDelegator,indexJson, indexKey, indexSort);
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
                list.add(new CSONItem(storeDelegator,(CSONObject) obj, indexKey, indexSort));
            } else {
                list.add(new CSONItem(storeDelegator,new CSONObject().put(indexKey, obj), indexKey, indexSort));
            }
        }
        return list;
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
