package com.snoworca.IdxDB.collection;

import com.snoworca.IdxDB.CompareUtil;
import com.snoworca.IdxDB.OP;
import com.snoworca.IdxDB.dataStore.DataBlock;
import com.snoworca.IdxDB.dataStore.DataIO;
import com.snoworca.cson.CSONArray;
import com.snoworca.cson.CSONObject;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class IndexSet implements IndexCollection{

    private ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    private String name;

    private final int memCacheLimit;
    private String indexKey = "";
    private TreeSet<String> indexKeySet;
    private int indexSort = 0;
    private TreeSet<CSONItem> itemSet = new TreeSet<>();
    private ConcurrentLinkedQueue<Long> removeQueue = new ConcurrentLinkedQueue<>();
    private DataIO dataIO;
    private StoreDelegator storeDelegator;

    private long lastDataStorePos = -1;

    private long headPos = -1;
    private CSONObject optionInfo;
    private volatile boolean isChanged = false;



    public IndexSet(DataIO dataIO, IndexSetOption option) {
        ReentrantReadWriteLock.WriteLock lock =  readWriteLock.writeLock();
        lock.lock();
        try {
            this.indexKey = option.getIndexKey();
            this.indexSort = option.getIndexSort();
            this.memCacheLimit = option.getMemCacheSize();
            this.dataIO = option.isFileStore() ? dataIO : null;
            this.name = option.getName();
            if (this.dataIO != null) {
                makeStoreDelegatorImpl();
            }
            this.headPos = option.getHeadPos();
            initData();
            optionInfo = option.toCsonObject();
        } finally {
            lock.unlock();
        }
    }

    public CSONObject getOptionInfo() {
        return new CSONObject(optionInfo.toByteArray());
    }

    private void initData() {
        try {
            if(headPos < 1) {
                DataBlock headBlock = dataIO.write(new byte[]{0});
                lastDataStorePos = headPos = headBlock.getPos();
            } else {
                Iterator<DataBlock> dataBlockIterator = dataIO.iterator(headPos);
                boolean header = true;
                while(dataBlockIterator.hasNext()) {
                    DataBlock dataBlock = dataBlockIterator.next();
                    if(header) {
                        header = false;
                        continue;
                    }
                    byte[] buffer = dataBlock.getData();
                    lastDataStorePos = dataBlock.getPos();
                    CSONObject csonObject = new CSONObject(buffer);
                    Object indexValue = csonObject.opt(indexKey);
                    CSONItem csonItem = new CSONItem(storeDelegator, indexKey,indexValue == null ? 0 : indexValue, indexSort);
                    csonItem.setStoragePos(lastDataStorePos);
                    csonItem.setStore(true);
                    itemSet.add(csonItem);
                }
                long count = 0;
                for (CSONItem CSONItem : itemSet) {
                    CSONItem.setStore(count > memCacheLimit);
                    ++count;
                }


            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getHeadPos() {
        return headPos;
    }


    private void makeStoreDelegatorImpl() {
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



    @Override
    public int size() {
        //ReentrantReadWriteLock.WriteLock lock =  readWriteLock.writeLock();
        ReentrantReadWriteLock.ReadLock lock =  readWriteLock.readLock();
        lock.lock();
        int size = itemSet.size();
        lock.unlock();
        return size;
    }

    @Override
    public synchronized void commit() {
        ReentrantReadWriteLock.WriteLock lock =  readWriteLock.writeLock();
        if(!isChanged && removeQueue.isEmpty()) {
            return;
        }
        lock.lock();
        try {
            int count = 0;
            if(isChanged) {
                for (CSONItem csonItem : itemSet) {
                    long storePos = csonItem.getStoragePos();
                    if (csonItem.getStoragePos() > 0 && csonItem.isChanged()) {
                        try {
                            dataIO.unlink(storePos);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        csonItem.setStoragePos(-1);
                    }
                    csonItem.storeFileIfNeed();
                    csonItem.setStore(count > memCacheLimit);
                    ++count;
                }
                isChanged = false;
            }
            ArrayList<Long> removeList = new ArrayList<>(removeQueue);
            removeQueue.clear();
            for(int i = 0, n = removeList.size(); i < n; ++i) {
                long filePos = removeList.get(i);
                try {
                    dataIO.unlink(filePos);
                } catch (IOException ignored) {}
            }
        } finally {
            lock.unlock();
        }


    }

    @Override
    public boolean isEmpty() {
        //ReentrantReadWriteLock.WriteLock lock =  readWriteLock.writeLock();
        ReentrantReadWriteLock.ReadLock lock =  readWriteLock.readLock();
        lock.lock();
        boolean isEmpty = itemSet.isEmpty();
        lock.unlock();
        return isEmpty;
    }


    @Override
    public String getName() {
        return name;
    }

    @Override
    public Set<String> indexKeys() {
        if(indexKeySet != null) return indexKeySet;
        TreeSet<String> set = new TreeSet<>();
        set.add(indexKey);
        indexKeySet = set;
        return set;
    }


    private CSONItem getByIndexValue(Object value) {
        return get(new CSONObject().put(indexKey, value));
    }

    private CSONItem get(CSONObject jsonObject) {
        CSONItem item = new CSONItem(storeDelegator,jsonObject,indexKey, indexSort);
        ReentrantReadWriteLock.ReadLock lock =  readWriteLock.readLock();
        lock.lock();
        CSONItem foundItem = itemSet.floor(item);
        lock.unlock();
        if(foundItem == null || !CompareUtil.compare(foundItem.getIndexValue(), jsonObject.opt(indexKey), OP.eq)) {
            return null;
        }
        return foundItem;

    }

    @Override
    public boolean addOrReplace(CSONObject jsonObject) {
        if(jsonObject == null || jsonObject.opt(indexKey) == null) {
            return false;
        }
        CSONItem foundItem = get(jsonObject);
        isChanged = true;
        if(foundItem == null) {
            boolean isSuccess = add(jsonObject);
            return isSuccess;
        }
        foundItem.setCsonObject(jsonObject);
        return true;
    }

    @Override
    public boolean addOrReplaceAll(CSONArray jsonArray) {
        boolean isSuccess = true;
        for(int i = 0, n = jsonArray.size(); i < n; ++i) {
            CSONObject jsonObject = jsonArray.optObject(i);
            if(jsonObject == null || jsonObject.opt(indexKey) == null) {
                return false;
            }
        }
        isChanged = true;
        for(int i = 0, n = jsonArray.size(); i < n; ++i) {
            CSONObject jsonObject = jsonArray.getObject(i);
            isSuccess = isSuccess & addOrReplace(jsonObject);
        }
        return isSuccess;
    }

    @Override
    public boolean add(CSONObject jsonObject) {
        CSONItem item = new CSONItem(storeDelegator,jsonObject, indexKey, indexSort);
        ReentrantReadWriteLock.WriteLock lock =  readWriteLock.writeLock();
        lock.lock();
        boolean success = itemSet.add(item);
        if(this.dataIO == null && itemSet.size() > memCacheLimit) {
            itemSet.pollLast();
        }
        lock.unlock();
        isChanged = true;
        return success;
    }

    @Override
    public boolean addAll(CSONArray csonArray) {
        ArrayList<CSONObject> jsonObjects = new ArrayList<>();
        for(int i = 0, n = csonArray.size(); i < n; ++i) {
            CSONObject jsonObject = csonArray.optObject(i);
            if(jsonObject == null) {
                return false;
            }
            jsonObjects.add(jsonObject);
        }
        boolean isSuccess = addAll(jsonObjects);
        isChanged = true;
        return isSuccess;
    }

    @Override
    public List<CSONObject> findByIndex(Object indexValue, FindOption option, int limit) {
        ReentrantReadWriteLock.ReadLock lock = readWriteLock.readLock();
        OP op = option.getOp();
        if(op == null) op = OP.eq;
        ArrayList<CSONObject> result = new ArrayList<>();
        if(limit < 1) {

            return result;
        }
        switch (op) {
            case eq:
                CSONObject jsonObject = findByIndex(indexValue);
                if(jsonObject != null) result.add(jsonObject);
                break;
            case gte:
            case gt:
                if(indexSort > 0) {
                    lock.lock();
                    result = (ArrayList<CSONObject>)jsonItemCollectionsToJsonObjectCollection(itemSet.tailSet(makeIndexItem(indexValue),op == OP.gte), limit );
                    lock.unlock();
                } else {
                    lock.lock();
                    result = (ArrayList<CSONObject>)jsonItemCollectionsToJsonObjectCollection(itemSet.descendingSet().tailSet(makeIndexItem(indexValue), op == OP.gte), limit);
                    lock.unlock();
                }
                break;
            case lte:
            case lt:
                if(indexSort > 0) {
                    lock.lock();
                    result = (ArrayList<CSONObject>)jsonItemCollectionsToJsonObjectCollection(itemSet.descendingSet().tailSet(makeIndexItem(indexValue), op == OP.lte), limit);
                    lock.unlock();
                } else {
                    lock.lock();
                    result = (ArrayList<CSONObject>)jsonItemCollectionsToJsonObjectCollection(itemSet.tailSet(makeIndexItem(indexValue), op == OP.lte), limit);
                    lock.unlock();
                }
                break;
        }
        return result;
    }

    @Override
    public List<Object> removeByIndex(Object indexValue, FindOption option) {
        ReentrantReadWriteLock.WriteLock writeLock = readWriteLock.writeLock();
        OP op = option.getOp();
        if(op == null) op = OP.eq;
        ArrayList<Object> results = new ArrayList<>();
        switch (op) {
            case eq:
                if(removeIndex(indexValue)) {
                    results.add(indexValue);
                }
                break;
            case gte:
            case gt:
                if(indexSort > 0) {
                    writeLock.lock();
                    results = removeByJSONItems(itemSet.tailSet(makeIndexItem(indexValue), op == OP.gte));
                    writeLock.unlock();
                } else {
                    writeLock.lock();
                    results = removeByJSONItems(itemSet.descendingSet().tailSet(makeIndexItem(indexValue), op == OP.gte));
                    writeLock.unlock();
                }
                break;
            case lte:
            case lt:
                if(indexSort > 0) {
                    writeLock.lock();
                    results = removeByJSONItems(itemSet.descendingSet().tailSet(makeIndexItem(indexValue), op == OP.lte));
                    writeLock.unlock();
                } else {
                    writeLock.lock();
                    results = removeByJSONItems(itemSet.tailSet(makeIndexItem(indexValue), op == OP.lte));
                    writeLock.unlock();
                }
                break;
        }
        return results;
    }

    @Override
    public List<CSONObject> list(int limit, boolean reverse) {
        ReentrantReadWriteLock.ReadLock readLock = readWriteLock.readLock();
        try {
            readLock.lock();
            if (reverse) {
                List<CSONObject> result = (List<CSONObject>) jsonItemCollectionsToJsonObjectCollection(itemSet.descendingSet(), limit);
                return result;
            }
            List<CSONObject> result = (List<CSONObject>) jsonItemCollectionsToJsonObjectCollection(itemSet, limit);
            return result;
        } finally {
            readLock.unlock();
        }

    }

    private Collection<CSONObject> jsonItemCollectionsToJsonObjectCollection(Collection<CSONItem> CSONItems, int limit) {
        int count = 0;
        ArrayList<CSONObject> jsonObjects = new ArrayList<>();
        for(CSONItem item : CSONItems) {
            if(count >= limit) {
                break;
            }
            jsonObjects.add(item.getCsonObject());
            ++count;
        }
        return jsonObjects;
    }


    private ArrayList<Object> removeByJSONItems(Collection<CSONItem> CSONItems) {
        ArrayList<Object> removedIndexList = new ArrayList<>();
        for(CSONItem item : CSONItems) {
            removedIndexList.add(item.getIndexValue());
            addPosInRemoveList(item);
        }
        CSONItems.clear();
        return removedIndexList;
    }



    private CSONItem makeIndexItem(Object index) {
        CSONObject indexJson = new CSONObject().put(indexKey, index);
        CSONItem indexItem = new CSONItem(storeDelegator,indexJson, indexKey, indexSort);
        return indexItem;
    }

    private CSONObject findByIndex(Object indexValue) {
        CSONItem foundItem = get(new CSONObject().put(indexKey, indexValue));
        return foundItem == null ? null : foundItem.getCsonObject();
    }

    private boolean removeIndex(Object indexValue) {
        CSONItem indexItem = getByIndexValue(indexValue); //makeIndexItem(indexValue);
        if(indexItem == null) return false;
        ReentrantReadWriteLock.WriteLock lock =  readWriteLock.writeLock();
        lock.lock();
        boolean isSuccess = itemSet.remove(indexItem);
        addPosInRemoveList(indexItem);
        lock.unlock();;
        return isSuccess;
    }

    public CSONObject last() {
        ReentrantReadWriteLock.ReadLock readLock = readWriteLock.readLock();
        try {
            readLock.lock();
            if (itemSet.isEmpty()) return null;
            return itemSet.last().getCsonObject();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public boolean remove(CSONObject o) {
        CSONItem item = get(o); //new CSONItem(storeDelegator,(CSONObject) o, indexKey, indexSort);
        if(item == null) return false;
        ReentrantReadWriteLock.WriteLock lock =  readWriteLock.writeLock();
        lock.lock();
        boolean isSuccess = itemSet.remove(item);
        addPosInRemoveList(item);
        lock.unlock();
        return isSuccess;
    }

    private void addPosInRemoveList(CSONItem item) {
        long pos = item.getStoragePos();
        if(pos > 0) {
            removeQueue.add(pos);
        }
    }


    private boolean addAll(Collection<? extends CSONObject> c) {
        Collection<?> list = objectCollectionToJSONItemCollection(c);
        ReentrantReadWriteLock.WriteLock lock = readWriteLock.writeLock();
        lock.lock();
        boolean success = itemSet.addAll((Collection<? extends CSONItem>) list);
        if(success && this.dataIO == null) {
            while(itemSet.size() > memCacheLimit) {
                itemSet.pollLast();
            }
        }
        lock.unlock();
        return success;

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


    public void clear() {
        ReentrantReadWriteLock.WriteLock lock =  readWriteLock.writeLock();
        lock.lock();
        for(CSONItem item : itemSet) {
            addPosInRemoveList(item);
        }
        itemSet.clear();
        lock.unlock();
    }

    @Override
    public Iterator<CSONObject> iterator() {
        ReentrantReadWriteLock.ReadLock lock = readWriteLock.readLock();
        lock.lock();
        final Iterator<CSONItem> iterator = itemSet.iterator();
        lock.unlock();

        return new Iterator<CSONObject>() {
            CSONItem current;
            @Override
            public boolean hasNext() {
                ReentrantReadWriteLock.ReadLock lock =  readWriteLock.readLock();
                lock.lock();
                try {
                    boolean hasNext = iterator.hasNext();
                    return hasNext;
                } finally {
                    lock.unlock();
                }
            }

            @Override
            public CSONObject next() {
                ReentrantReadWriteLock.ReadLock lock =  readWriteLock.readLock();
                lock.lock();
                try {
                    current = iterator.next();
                    if(current == null) return null;
                    return current.getCsonObject();
                } finally {
                    lock.unlock();
                }

            }

            @Override
            public void remove() {
                ReentrantReadWriteLock.WriteLock lock =  readWriteLock.writeLock();
                lock.lock();
                try {
                    iterator.remove();
                    if(current != null) {
                        addPosInRemoveList(current);
                    }
                } finally {
                    lock.unlock();
                }

            }
        };
    }
}
