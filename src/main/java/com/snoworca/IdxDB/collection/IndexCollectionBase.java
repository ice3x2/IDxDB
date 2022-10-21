package com.snoworca.IdxDB.collection;

import com.snoworca.IdxDB.CompressionType;
import com.snoworca.IdxDB.OP;
import com.snoworca.IdxDB.store.DataBlock;
import com.snoworca.IdxDB.store.DataStore;
import com.snoworca.cson.CSONArray;
import com.snoworca.cson.CSONObject;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class IndexCollectionBase implements IndexCollection, Restorable {

    private ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private ReentrantReadWriteLock readWriteTransactionTempLock = new ReentrantReadWriteLock();
    private ArrayList<TransactionOrder> transactionOrders = new ArrayList<>();

    private int collectionID;
    private final int memCacheLimit;
    private DataStore dataStore;
    private String name;
    private LinkedHashSet<String> indexKeySet;
    private int memCacheSize;
    private long lastStorePos = -1;
    private StoreDelegator storeDelegator;

    private String indexKey;
    private int sort;
    private long headPos = -1;

    private boolean isMemCacheIndex = true;

    private float capacityRatio = 0.3f;
    private boolean isCapacityFixed = false;

    private CollectionOption collectionOption;

    private CompressionType compressionType;

    IndexCollectionBase(int id, DataStore dataStore, CollectionOption collectionOption) {
        this.collectionID = id;
        this.dataStore = dataStore;
        this.name = collectionOption.getName();
        this.memCacheSize = collectionOption.getMemCacheSize();
        this.indexKey = collectionOption.getIndexKey();
        this.sort = collectionOption.getIndexSort();
        this.memCacheLimit = collectionOption.getMemCacheSize();
        this.headPos = collectionOption.getHeadPos();
        this.isMemCacheIndex = collectionOption.isMemCacheIndex();
        this.capacityRatio = collectionOption.getCapacityRatio();
        this.isCapacityFixed = capacityRatio < 0.001f;
        onInit(collectionOption);
        if (this.dataStore != null) {
            makeStoreDelegatorImpl();
        }
        initData();

        this.collectionOption = collectionOption;
    }


    private Collection<CSONObject> toCSONObjectListFrom(CSONArray csonArray) {
        ArrayList<CSONObject> allList = new ArrayList<>();
        for(int i = 0, n = csonArray.size(); i < n; ++i) {
            CSONObject csonObject = csonArray.optObject(i);
            if(csonObject == null || csonObject.opt(indexKey) == null) {
                return null;
            }
            allList.add(csonObject);
        }
        return allList;
    }

    @Override
    public CSONObject findOneByIndex(Object index) {
        List<CSONObject> result = findByIndex(index, FindOption.fromOP(OP.eq), 1);
        if(result.isEmpty()) return null;
        return result.get(0);
    }


    @Override
    public int getID() {
        return collectionID;
    }

    @Override
    public boolean addOrReplaceAll(CSONArray csonArray) {
        Collection<CSONObject > allList = toCSONObjectListFrom(csonArray);
        if(allList == null) return false;
        addOrReplaceAllTransactionOrder(allList);
        return true;
    }

    @Override
    public boolean addOrReplace(CSONObject csonObject) {
        if(!checkIndexKey(csonObject)) {
            return false;
        }
        addOrReplaceTransactionOrder(csonObject);
        return true;
    }


    @Override
    public void removeByIndex(Object indexValue) {
        removeByIndex(indexValue, FindOption.fromOP(OP.eq));
    }

    @Override
    public boolean remove(CSONObject o) {
        if(!checkIndexKey(o)) {
            return false;
        }
        removeTransactionOrder(o);
        return true;
    }


    @Override
    public void clear() {
        clearTransactionOrder();
    }


    @Override
    public boolean add(CSONObject csonObject) {
        if(!checkIndexKey(csonObject)) {
            return false;
        }
        addTransactionOrder(csonObject);
        return true;
    }


    @Override
    public boolean addAll(CSONArray csonArray) {
        Collection<CSONObject > allList = toCSONObjectListFrom(csonArray);
        if(allList == null) return false;
        addAllTransactionOrder(allList);
        return true;
    }

    protected abstract void onInit(CollectionOption collectionOption);

    protected abstract void onRestoreCSONItem(CSONItem csonItem);
    protected abstract void onMemStore();


    public CSONObject getOptionInfo() {
        return new CSONObject(collectionOption.toCsonObject().toBytes());
    }

    private void initData() {
        /*try {
            if(headPos < 1) {
                DataBlock headBlock = dataStore.write(new CSONObject().toByteArray());
                lastStorePos = headPos = headBlock.getPos();
            } else {
                Iterator<DataBlock> dataBlockIterator = dataStore.iterator(headPos);
                boolean header = true;
                while(dataBlockIterator.hasNext()) {
                    DataBlock dataBlock = dataBlockIterator.next();
                    if(header) {
                        header = false;
                        continue;
                    }
                    byte[] buffer = dataBlock.getPayload();
                    lastStorePos = dataBlock.getPos();
                    Object indexValue = indexFromBuffer(buffer).get(0);
                    CSONItem csonItem = new CSONItem(storeDelegator, indexKey,indexValue, sort, isMemCacheIndex);
                    csonItem.setStoragePos_(lastStorePos);
                    csonItem.setStore(true);
                    onRestoreCSONItem(csonItem);
                }
                onMemStore();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }*/
    }


    protected boolean unlink(CSONItem item) throws IOException {
        if(item == null || !item.isStored()) return false;
        if(dataStore != null) {
            dataStore.unlink(item.getStoragePos(), item.getStoreCapacity());
        }
        return true;
    }

    protected String getIndexKey() {
        return indexKey;
    }

    protected int getSort() {
        return sort;
    }

    protected boolean checkIndexKey(CSONObject csonObject) {
        return csonObject != null && csonObject.opt(indexKey) != null;
    }

    @Override
    public long getHeadPos() {
        return headPos;
    }

    @Override
    public void rollback() {
        ReentrantReadWriteLock.WriteLock writeLock = readWriteTransactionTempLock.writeLock();
        writeLock.lock();
        transactionOrders.clear();
        writeLock.unlock();
    }

    protected int getMemCacheSize() {
        return memCacheSize;
    }

    protected void addTransactionOrder(CSONObject csonObject) {
        ReentrantReadWriteLock.WriteLock writeLock = readWriteTransactionTempLock.writeLock();
        writeLock.lock();
        CSONItem item = new CSONItem(storeDelegator, csonObject, indexKey, sort,isMemCacheIndex);
        transactionOrders.add(new TransactionOrder(TransactionOrder.ORDER_ADD, item));
        writeLock.unlock();
    }

    protected void addOrReplaceTransactionOrder(CSONObject csonObject) {
        ReentrantReadWriteLock.WriteLock writeLock = readWriteTransactionTempLock.writeLock();
        writeLock.lock();
        CSONItem item = new CSONItem(storeDelegator, csonObject, indexKey, sort,isMemCacheIndex);
        transactionOrders.add(new TransactionOrder(TransactionOrder.ORDER_ADD_OR_REPLACE, item));
        writeLock.unlock();
    }



    protected void addOrReplaceAllTransactionOrder(Collection<CSONObject> csonObjects) {
        ReentrantReadWriteLock.WriteLock writeLock = readWriteTransactionTempLock.writeLock();
        ArrayList<TransactionOrder> list = new ArrayList<>(csonObjects.size());
        if(csonObjects instanceof  ArrayList) {
            for (int i = 0, n = csonObjects.size(); i < n; ++i) {
                CSONItem item = new CSONItem(storeDelegator, ((ArrayList<CSONObject>) csonObjects).get(i), indexKey, sort,isMemCacheIndex);
                list.add(new TransactionOrder(TransactionOrder.ORDER_ADD_OR_REPLACE, item));
            }

        } else {
            for(CSONObject csonObject : csonObjects) {
                CSONItem item = new CSONItem(storeDelegator, csonObject, indexKey, sort,isMemCacheIndex);
                list.add(new TransactionOrder(TransactionOrder.ORDER_ADD_OR_REPLACE, item));
            }
        }
        writeLock.lock();
        transactionOrders.addAll(list);
        writeLock.unlock();
    }

    protected ArrayList<TransactionOrder> getTransactionOrders() {
        ReentrantReadWriteLock.WriteLock lock = readWriteTransactionTempLock.writeLock();
        lock.lock();
        try {
            ArrayList<TransactionOrder> result = new ArrayList<>(transactionOrders);
            transactionOrders.clear();
            return result;
        } finally {
            lock.unlock();;
        }

    }

    protected void addAllTransactionOrder(Collection<CSONObject> csonObjects) {
        ReentrantReadWriteLock.WriteLock writeLock = readWriteTransactionTempLock.writeLock();
        ArrayList<TransactionOrder> list = new ArrayList<>(csonObjects.size());
        if(csonObjects instanceof  ArrayList) {
            for (int i = 0, n = csonObjects.size(); i < n; ++i) {
                CSONItem item = new CSONItem(storeDelegator, ((ArrayList<CSONObject>) csonObjects).get(i), indexKey, sort,isMemCacheIndex);
                list.add(new TransactionOrder(TransactionOrder.ORDER_ADD, item));
            }

        } else {
            for(CSONObject csonObject : csonObjects) {
                CSONItem item = new CSONItem(storeDelegator, csonObject, indexKey, sort,isMemCacheIndex);
                list.add(new TransactionOrder(TransactionOrder.ORDER_ADD, item));
            }
        }
        writeLock.lock();
        transactionOrders.addAll(list);
        writeLock.unlock();
    }

    protected void replaceTransactionOrder(CSONObject csonObject) {
        ReentrantReadWriteLock.WriteLock writeLock = readWriteTransactionTempLock.writeLock();
        writeLock.lock();
        CSONItem item = new CSONItem(storeDelegator, csonObject, indexKey, sort,isMemCacheIndex);
        transactionOrders.add(new TransactionOrder(TransactionOrder.ORDER_REPLACE, item));
        writeLock.unlock();
    }

    protected void removeTransactionOrder(CSONObject csonObject) {
        ReentrantReadWriteLock.WriteLock writeLock = readWriteTransactionTempLock.writeLock();
        writeLock.lock();
        CSONItem item = new CSONItem(storeDelegator, csonObject, indexKey, sort,isMemCacheIndex);
        transactionOrders.add(new TransactionOrder(TransactionOrder.ORDER_REMOVE, item));
        writeLock.unlock();
    }

    protected void removeTransactionOrder(Collection<CSONItem> csonItem) {
        ReentrantReadWriteLock.WriteLock writeLock = readWriteTransactionTempLock.writeLock();
        writeLock.lock();
        if(csonItem instanceof ArrayList) {
            for(int i = 0, n = csonItem.size(); i < n; ++i) {
                transactionOrders.add(new TransactionOrder(TransactionOrder.ORDER_REMOVE, ((ArrayList<CSONItem>) csonItem).get(i)));
            }
        } else {
            for(CSONItem item : csonItem) {
                transactionOrders.add(new TransactionOrder(TransactionOrder.ORDER_REMOVE, item));
            }
        }

        writeLock.unlock();
    }

    protected void clearTransactionOrder() {
        ReentrantReadWriteLock.WriteLock writeLock = readWriteTransactionTempLock.writeLock();
        writeLock.lock();
        transactionOrders.add(new TransactionOrder(TransactionOrder.ORDER_CLEAR, null));
        writeLock.unlock();
    }


    protected void writeLock() {
        ReentrantReadWriteLock.WriteLock lock = readWriteLock.writeLock();
        lock.lock();
    }

    protected void writeUnlock() {
        ReentrantReadWriteLock.WriteLock lock = readWriteLock.writeLock();
        lock.unlock();
    }

    protected void readLock() {
        ReentrantReadWriteLock.ReadLock lock = readWriteLock.readLock();
        lock.lock();
    }

    protected void readUnlock() {
        ReentrantReadWriteLock.ReadLock lock = readWriteLock.readLock();
        lock.unlock();
    }

    @Override
    public String getName() {
        return name;
    }



    protected StoreDelegator getStoreDelegator() {
        return storeDelegator;
    }



    protected void makeStoreDelegatorImpl() {
        storeDelegator = new StoreDelegator() {

            @Override
            public StoredInfo storeData(long pos, CSONObject csonObject) {
                byte[] buffer = csonObject.toBytes();
                DataBlock dataBlock;
                try {
                    if (pos == -1) {
                        dataBlock = dataStore.write(collectionID, buffer);
                    } else {
                        dataBlock = dataStore.replaceOrWrite(collectionID, buffer, pos);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return new StoredInfo(dataBlock.getPosition(), dataBlock.getCapacity());
            }


            @Override
            public StoredInfo loadData(long pos) {
                try {
                    DataBlock dataBlock = dataStore.get(pos);
                    return new StoredInfo(dataBlock.getPosition(), dataBlock.getCapacity(), new CSONObject(dataBlock.getData()));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    @Override
    public Set<String> indexKeys() {
        if(indexKeySet != null) return indexKeySet;
        LinkedHashSet<String> set = new LinkedHashSet<>();
        set.add(indexKey);
        indexKeySet = set;
        return set;
    }
}
